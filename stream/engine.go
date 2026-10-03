package stream

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
)

// ingestionItem 是摄取协程交给串行状态协程的一条已分类记录。
type ingestionItem struct {
	source string
	offset int64
	raw    string
	event  *Event // 解析成功且基础字段合法
	perr   *ClassifiedError
}

// Engine 编排：并发摄取 -> 校验分类 -> 水位 -> 窗口 -> 检查点 -> 发布。
//
// 并发模型：每个源一个 goroutine 读取/解析，结果通过“有界队列”交给唯一的
// 状态处理 goroutine。所有窗口/水位/去重状态只在该 goroutine 内修改，
// 因此并发摄入下结果天然确定，无需细粒度锁。
//
// 背压：队列满时源 goroutine 在 send 上阻塞，于是该源停止从底层介质读取
// （读得慢 -> 有界内存，绝不静默丢事件）；其他源有独立队列，慢源不会
// 长期阻塞整体吞吐。
type Engine struct {
	cfg     Config
	onError ErrorHandler
	stats   Stats
	emitSeq int64
	// live 暴露运行中状态（Status 读取）；cur 仅供状态协程无锁更新。
	// 两者指向同一对象；live 用 atomic.Pointer 保证并发发布安全。
	live atomic.Pointer[engineLive]
	cur  *engineLive
}

// NewEngine 创建引擎。onError 用于观测所有分类异常；为 nil 时异常仅计数。
func NewEngine(cfg Config, onError ErrorHandler) *Engine {
	if cfg.WindowSize <= 0 {
		panic("stream: WindowSize must be positive")
	}
	if cfg.AllowedLateness < 0 {
		panic("stream: AllowedLateness must not be negative")
	}
	if cfg.SourceQueueSize <= 0 {
		cfg.SourceQueueSize = 1024
	}
	if cfg.CheckpointEvery <= 0 {
		cfg.CheckpointEvery = 1000
	}
	return &Engine{cfg: cfg, onError: onError}
}

// offsetSeeker 是支持按偏移恢复的源（SliceSource/FileSource 均实现）。
type offsetSeeker interface {
	SeekOffset(n int64) error
}

// sentinelInfiniteWM 是最终排空使用的逻辑 +inf 水位。
// 取一个仍可被 time.Time/JSON 序列化的远未来时间，足以关闭任何现实窗口。
var sentinelInfiniteWM = endOfInputWatermark

// Run 并发运行所有源直到全部结束（或 ctx 取消），随后最终排空：
// 推进水位到 +inf 关闭全部窗口，提交最后一个检查点并发布其结果。
//
// 配置了 CheckpointPath 时：从最近有效检查点严格校验后恢复，恢复期间先
// 重放已提交结果再处理新事件；任何未进入检查点的结果都不会对外发布。
func (e *Engine) Run(ctx context.Context, sources []Source, sink Sink) (retErr error) {
	if sink == nil {
		return errors.New("stream: nil sink")
	}
	if len(sources) == 0 {
		return errors.New("stream: no sources")
	}
	e.stats = Stats{}

	store := NewWindowStore(e.cfg.WindowSize, e.cfg.AllowedLateness)
	store.SetEmittedRetention(e.cfg.EmittedRetention)
	tracker := NewWatermarkTracker()
	for _, src := range sources {
		tracker.Register(src.Name())
	}

	var lastCkpt *Checkpoint
	haveCkpt := false
	if e.cfg.CheckpointPath != "" {
		ck, err := LoadCheckpoint(e.cfg.CheckpointPath, e.cfg)
		if err != nil {
			var ce *CheckpointError
			if errors.As(err, &ce) && ce.Kind == CKPTNotFound {
				// 全新启动
			} else {
				return err
			}
		} else {
			if err := checkSourceSet(ck, sources); err != nil {
				return err
			}
			store.Restore(windowSnapshot{
				Open: ck.Windows, Emitted: ck.Emitted, NextSeq: ck.NextEmitSeq,
			})
			for name, p := range ck.Sources {
				tracker.Restore(name, p.Watermark)
			}
			lastCkpt = ck
			haveCkpt = true
		}
	}

	e.emitSeq = store.NextSeq()

	// 恢复：按确定顺序重放已提交结果到（全新打开的）汇；完成前不处理新事件。
	if haveCkpt {
		if err := emitReplay(sink, store.EmittedResults()); err != nil {
			return fmt.Errorf("replay committed results: %w", err)
		}
		e.stats.WindowsEmitted = int64(len(store.EmittedResults()))
	}

	// 恢复各源消费偏移。
	if haveCkpt {
		for _, src := range sources {
			p := lastCkpt.Sources[src.Name()]
			if p.Offset > 0 {
				sk, ok := src.(offsetSeeker)
				if !ok {
					return fmt.Errorf("source %s does not support checkpoint resume", src.Name())
				}
				if err := sk.SeekOffset(p.Offset); err != nil {
					return fmt.Errorf("source %s seek: %w", src.Name(), err)
				}
			}
		}
	}

	queues := make(map[string]chan ingestionItem, len(sources))
	dones := make(map[string]chan struct{}, len(sources))
	order := make([]string, 0, len(sources))
	for _, src := range sources {
		name := src.Name()
		if _, dup := queues[name]; dup {
			return fmt.Errorf("duplicate source name %q", name)
		}
		queues[name] = make(chan ingestionItem, e.cfg.SourceQueueSize)
		dones[name] = make(chan struct{})
		order = append(order, name)
	}

	ingestCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 每源一个摄取 goroutine；慢源只会填满自己的有界队列并自我阻塞。
	for _, src := range sources {
		src := src
		ch := queues[src.Name()]
		done := dones[src.Name()]
		go func() {
			for {
				raw, off, ok, err := src.Next(ingestCtx)
				if err != nil {
					item := ingestionItem{source: src.Name(), offset: off, raw: raw,
						perr: classify(KindMalformed, src.Name(), off, raw, time.Time{},
							"source read error: "+err.Error())}
					select {
					case ch <- item:
					case <-ingestCtx.Done():
					}
					close(done)
					return
				}
				if !ok {
					close(done)
					return
				}
				ev, perr := ParseRecord(raw, src.Name(), off)
				item := ingestionItem{source: src.Name(), offset: off, raw: raw,
					event: ev, perr: perr}
				select {
				case ch <- item:
				case <-ingestCtx.Done():
					close(done)
					return
				}
			}
		}()
	}

	// 偏移在状态协程中按物理记录单调推进（含坏记录/空行）。
	offsets := make(map[string]int64, len(sources))
	if haveCkpt {
		for name, p := range lastCkpt.Sources {
			offsets[name] = p.Offset
		}
	}

	// 运行中可观测状态：全部原子量，Status 可无锁并发读取。
	lv := &engineLive{srcIdx: make(map[string]int, len(order))}
	lv.sources = make([]sourceLive, len(order))
	for i, name := range order {
		lv.sources[i].name = name
		lv.sources[i].ch = queues[name]
		lv.sources[i].offset.Store(offsets[name])
		lv.srcIdx[name] = i
	}
	for name, wm := range tracker.PerSource() {
		if idx, ok := lv.srcIdx[name]; ok {
			lv.sources[idx].wmNano.Store(timeToNano(wm))
		}
	}
	lv.running.Store(true)
	defer lv.running.Store(false)
	e.cur = lv
	e.live.Store(lv)
	refreshCounts := func() {
		lv.activeWindows.Store(int64(store.OpenCount()))
		lv.retainedEmitted.Store(int64(store.EmittedCount()))
	}
	refreshCounts()

	var pending []*WindowResult // 已关闭、等待提交后发布的窗口
	sinceCkpt := int64(0)
	// pruneWM 是最近一次真实观测到的全局水位（不含 EOI 哨兵）。
	// 已发布尾部的遗忘只跟随真实事件时间进度：EOI 哨兵是逻辑 +inf，
	// 若用它剪枝会把重启重放所需的已提交结果也一并遗忘。
	var pruneWM time.Time

	checkpointAndPublish := func() error {
		if e.cfg.CheckpointPath == "" {
			// 无持久化：状态处理串行且确定性，关闭即发布即可。
			for _, r := range pending {
				if err := sink.Emit(r); err != nil {
					return err
				}
				e.stats.WindowsEmitted++
			}
			pending = pending[:0]
			lv.windowsEmitted.Store(e.stats.WindowsEmitted)
			return nil
		}
		wms := tracker.PerSource()
		sp := make(map[string]SourceProgress, len(sources))
		for _, name := range order {
			sp[name] = SourceProgress{Offset: offsets[name], Watermark: wms[name]}
		}
		seq := int64(0)
		if lastCkpt != nil {
			seq = lastCkpt.Seq
		}
		// 按真实水位剪枝已发布尾部：检查点体积只与活跃窗口、
		// 迟到视界和保留范围相关，不随累计窗口数膨胀。
		snap := store.Snapshot(pruneWM)
		ck := buildCheckpoint(seq+1, e.cfg, sp, snap)
		if err := SaveCheckpoint(e.cfg.CheckpointPath, ck); err != nil {
			return err
		}
		lastCkpt = ck
		e.stats.Checkpoints++
		// 提交成功后才对外发布（恢复期间不发布未提交结果）。
		for _, r := range pending {
			if err := sink.Emit(r); err != nil {
				return err
			}
			e.stats.WindowsEmitted++
		}
		pending = pending[:0]
		sinceCkpt = 0
		lv.checkpoints.Store(e.stats.Checkpoints)
		lv.windowsEmitted.Store(e.stats.WindowsEmitted)
		refreshCounts() // 快照可能剪枝了已发布尾部
		return nil
	}

	closeReady := func(wm time.Time) error {
		for _, id := range store.Closeable(wm) {
			closedAt := time.Now().UTC()
			if e.cfg.DeterministicClosedAt {
				// 确定性模式：ClosedAt 取窗口关闭视界（windowEnd+lateness）
				// 而非墙钟，同一批逻辑输入重复运行的输出可逐字节对比。
				closedAt = id.Start.Add(e.cfg.WindowSize + e.cfg.AllowedLateness).UTC()
			}
			r, ok := store.Close(id, wm, closedAt, e.emitSeq)
			if !ok {
				continue
			}
			e.emitSeq++
			pending = append(pending, r)
		}
		return nil
	}

	// active 跟踪尚未 EOF 的源；reflect.Select 在“数据/EOF/ctx”之间公平选择。
	active := make(map[string]bool, len(order))
	for _, name := range order {
		active[name] = true
	}
	cases := make([]reflect.SelectCase, 0, 2*len(order)+1)
	caseSource := make([]string, 0, 2*len(order))
	addCases := func() {
		cases = cases[:0]
		caseSource = caseSource[:0]
		ctxCase := reflect.SelectCase{Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done())}
		cases = append(cases, ctxCase)
		caseSource = append(caseSource, "")
		for _, name := range order {
			if !active[name] {
				continue
			}
			cases = append(cases, reflect.SelectCase{
				Dir: reflect.SelectRecv, Chan: reflect.ValueOf(queues[name])})
			caseSource = append(caseSource, name)
			cases = append(cases, reflect.SelectCase{
				Dir: reflect.SelectRecv, Chan: reflect.ValueOf(dones[name])})
			caseSource = append(caseSource, name)
		}
	}

	for len(active) > 0 {
		if err := ctx.Err(); err != nil {
			cancel()
			return err
		}
		addCases()
		chosen, recv, _ := reflect.Select(cases)
		if chosen == 0 {
			cancel()
			return ctx.Err()
		}
		name := caseSource[chosen]
		// 布局 ctx(0), data0(1), done0(2), data1(3), done1(4) ...：
		// done 通道位于偶数下标。源 goroutine 保证“入队全部记录 happens-before close(done)”，
		// 因此观察到 done 后，非阻塞排空即可拿到该源全部残余记录。
		if chosen%2 == 0 {
			drainRemaining(queues[name], func(it ingestionItem) {
				e.processItem(it, store, tracker, offsets)
			})
			tracker.Finish(name)
			lv.sources[lv.srcIdx[name]].finished.Store(true)
			delete(active, name)
			gwm := tracker.Global()
			pruneWM = gwm
			lv.globalWMNano.Store(timeToNano(gwm))
			if err := closeReady(gwm); err != nil {
				return err
			}
			refreshCounts()
			if len(pending) > 0 {
				if err := checkpointAndPublish(); err != nil {
					return err
				}
			}
			continue
		}
		if !recv.IsValid() {
			continue
		}
		it := recv.Interface().(ingestionItem)
		e.processItem(it, store, tracker, offsets)
		if it.event != nil {
			sinceCkpt++
		}

		gwm := tracker.Global()
		pruneWM = gwm
		lv.globalWMNano.Store(timeToNano(gwm))
		if err := closeReady(gwm); err != nil {
			return err
		}
		if e.cfg.CheckpointPath == "" {
			// 无检查点时快照不会运行，这里直接收紧内存中的已发布尾部，
			// 保证长跑下内存同样有界。
			store.PruneEmitted(pruneWM)
		}
		refreshCounts()
		// 一旦有窗口被水位关闭：立即提交并发布（提交成功后才对外可见）；
		// 否则按固定事件数周期做检查点。
		if len(pending) > 0 || sinceCkpt >= e.cfg.CheckpointEvery {
			if err := checkpointAndPublish(); err != nil {
				return err
			}
		}
	}

	// 最终排空：所有源已结束，有界源在 EOF 后不会再有数据，按事件时间语义
	// 将最终水位推进到 +inf（sentinelInfiniteWM），关闭所有残留窗口并提交发布。
	if err := closeReady(sentinelInfiniteWM); err != nil {
		return err
	}
	if err := checkpointAndPublish(); err != nil {
		return err
	}
	refreshCounts()
	return nil
}

func drainRemaining(ch chan ingestionItem, fn func(ingestionItem)) {
	for {
		select {
		case it := <-ch:
			fn(it)
		default:
			return
		}
	}
}

// processItem 处理一条摄取记录（仅在状态 goroutine 中调用）。
func (e *Engine) processItem(it ingestionItem, store *WindowStore,
	tracker *WatermarkTracker, offsets map[string]int64) {
	lv := e.cur
	src := &lv.sources[lv.srcIdx[it.source]]
	// 偏移按物理记录推进，空行/注释/坏记录也不例外，保证恢复不重不漏。
	if it.offset >= offsets[it.source] {
		offsets[it.source] = it.offset + 1
		src.offset.Store(offsets[it.source])
	}
	e.stats.Ingested++
	lv.ingested.Add(1)

	if it.event == nil && it.perr != nil {
		it.perr.Watermark = tracker.Global()
		e.reportError(it.perr)
		switch it.perr.Kind {
		case KindMalformed:
			e.stats.Malformed++
			lv.malformed.Add(1)
		case KindInvalidTimestamp:
			e.stats.InvalidTS++
			lv.invalidTS.Add(1)
		}
		return
	}
	ev := it.event
	if ev == nil {
		return // 空行/注释
	}

	// 异常事件：相对当前全局水位超前过多（可配置的未来范围上界）。
	gwm := tracker.Global()
	if e.cfg.MaxOutofOrder > 0 && !gwm.IsZero() &&
		ev.Timestamp.After(gwm.Add(e.cfg.MaxOutofOrder)) {
		ce := classify(KindOutOfRange, ev.Source, ev.Offset, it.raw, ev.Timestamp,
			fmt.Sprintf("event time %s is more than %s ahead of global watermark %s",
				ev.Timestamp.UTC().Format(time.RFC3339Nano), e.cfg.MaxOutofOrder,
				gwm.UTC().Format(time.RFC3339Nano)))
		ce.Watermark = gwm
		e.reportError(ce)
		e.stats.OutOfRange++
		lv.outOfRange.Add(1)
		return
	}

	// 先观测水位再入窗：以最新水位判定窗口是否已关闭，语义最严格。
	newWM, advanced := tracker.Observe(ev.Source, ev.Timestamp)
	if advanced {
		src.wmNano.Store(timeToNano(ev.Timestamp))
	}
	accepted, dropped, dup := store.Add(ev, newWM)
	switch {
	case dup:
		ce := classify(KindDuplicate, ev.Source, ev.Offset, it.raw, ev.Timestamp,
			fmt.Sprintf("duplicate event id %q for key %q; ignored idempotently",
				ev.ID, ev.Key))
		ce.Watermark = newWM
		e.reportError(ce)
		e.stats.Duplicates++
		lv.duplicates.Add(1)
	case dropped:
		start := WindowStart(ev.Timestamp, e.cfg.WindowSize)
		ce := classify(KindLateDropped, ev.Source, ev.Offset, it.raw, ev.Timestamp,
			fmt.Sprintf("event id %q for key %q window [%s,%s) arrived after close horizon (global watermark=%s); closed result immutable",
				ev.ID, ev.Key,
				start.UTC().Format(time.RFC3339Nano),
				start.Add(e.cfg.WindowSize).UTC().Format(time.RFC3339Nano),
				newWM.UTC().Format(time.RFC3339Nano)))
		ce.Watermark = newWM
		e.reportError(ce)
		e.stats.LateDropped++
		lv.lateDropped.Add(1)
	case accepted:
		e.stats.Accepted++
		lv.accepted.Add(1)
		lv.activeWindows.Store(int64(store.OpenCount()))
	}
}

func (e *Engine) reportError(ce *ClassifiedError) {
	if e.onError != nil {
		e.onError(ce)
	}
}

// checkSourceSet 校验恢复源集合与检查点一致。
func checkSourceSet(ck *Checkpoint, sources []Source) error {
	want := make(map[string]struct{}, len(sources))
	for _, s := range sources {
		want[s.Name()] = struct{}{}
	}
	var missing, extra []string
	for name := range ck.Sources {
		if _, ok := want[name]; !ok {
			extra = append(extra, name)
		}
	}
	for name := range want {
		if _, ok := ck.Sources[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	return checkpointErrorf(CKPTBadConfig, "",
		"source set differs from checkpoint: missing in checkpoint=%v, checkpoint-only=%v",
		missing, extra)
}

// emitReplay 按发射序号确定序重放已提交结果。
func emitReplay(sink Sink, committed []WindowResult) error {
	out := make([]WindowResult, len(committed))
	copy(out, committed)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].EmitSeq > out[j].EmitSeq; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	for i := range out {
		r := out[i]
		if err := sink.Emit(&r); err != nil {
			return err
		}
	}
	return nil
}

// Report 返回最近一次 Run 的可观测计数。
func (e *Engine) Report() Stats { return e.stats }
