# event-time-stream-runtime-go

一个可在**本地**运行的实时事件流处理引擎：从多个本地文件并发摄取事件，
按**事件时间**做键控窗口聚合与水位推进，以**有界状态**处理乱序与迟到，
并通过**一致性检查点**保证故障重启后结果不重、不漏、可解释。

- 无外部服务、无网络依赖、无真实凭据：只使用 Go 标准库与本地文件。
- 并发摄入：每源独立 goroutine + 有界队列，背压传导到底层读取，慢源不长期阻塞其他源。
- 事件时间语义：每源水位 = 已观测最大事件时间；全局水位 = 活跃源水位最小值。
- 窗口在 `window_end + allowed_lateness` 的关闭视界被水位越过后**最终关闭**；
  之后到达的迟到事件被明确分类为 `LATE_DROPPED`，绝不修改已关闭结果。
- 同一窗口结果唯一确定：单线程串行状态处理 + 顺序无关的聚合归约
  （count/sum/min/max，首事件取 `(事件时间, 事件ID)` 最小者），
  关闭顺序按 `(窗口起点, 键)` 固定，发射序号 `emit_seq` 全局单调。
- 一致性检查点：原子写（tmp + fsync + rename）、SHA-256 校验和、
  版本号魔数与窗口配置校验；损坏/缺失/版本不兼容/配置不一致均以**可区分原因**拒绝。
- 提交后发布：窗口结果只在其所属状态成功落盘检查点后才**追加**写入输出；
  输出文件是只增不改的完整历史，恢复时按 `emit_seq` 对账（保留完整行、
  剥掉崩溃半行、前缀去重、缺失补写），有限保留下重启也不丢不重。
- 长跑有界状态：`EmittedRetention` 限定**内存与检查点**中已发布结果的
  保留范围，有界的只是状态而非输出；内存与检查点体积只取决于活跃窗口
  与保留尾部，与累计窗口总量无关，输出文件始终保留全部已产出窗口。
- 结果确定：同一逻辑输入与到达顺序、源交错、重复投递无关；
  小数值按确定顺序补偿求和，跨运行逐比特一致。
- 运行状态摘要：`Engine.Status()` 在摄入进行中并发读取活跃窗口数、
  已终结窗口数、各源积压与各类异常计数，用于判断状态是否有界。

## 目录结构

```
stream/                 库包
  types.go              事件、窗口结果、配置、统计
  errors.go             异常事件分类（ClassifiedError）
  source.go             Source 接口、CSV/JSONL 解析、内存源/文件源
  watermark.go          每源水位与全局最小水位
  window.go             键控窗口、确定性聚合、迟到判定、去重、快照
  checkpoint.go         版本化检查点、校验和、原子写、分类拒绝
  sink.go               JSONSink / MemorySink / CommittedFileSink（追加式对账恢复）
  engine.go             并发摄取编排、背压、提交-发布协议
  *_test.go             全部语义与恢复测试
cmd/etsr/main.go        本地命令行入口
```

## 事件时间与迟到语义

- **事件时间**取自记录字段（不是处理时间）。支持 RFC3339（含纳秒/数字时区）；
  无时区的朴素时间按 UTC 解释；内部一律归一化到 UTC。
- **每源水位**：`wm(source) = max(已观测事件时间)`，单调不减。
- **全局水位**：`min(所有未结束源的水位)`。源 EOF 后退出最小值计算
  （有界源结束意味着不会再来更旧的数据）。全部源结束后最终水位推进到
  逻辑 +inf（输出中显示为 `"EOI"`），关闭所有残留窗口。
- **窗口**：滚动窗口 `[start, start+windowSize)`，`start = eventTime.Truncate(size)`。
- **关闭视界**：`closeHorizon = windowEnd + allowedLateness`。
  当 `globalWatermark >= closeHorizon` 时窗口最终关闭并发布。
- **乱序事件**：事件时间早于当前水位、但其窗口关闭视界尚未越过时，
  正常进入正确窗口并参与聚合。
- **迟到事件**：窗口关闭后到达（`globalWatermark >= windowEnd+lateness`）
  或窗口已发布：分类为 `LATE_DROPPED`，**不修改**任何已关闭结果。
- **重复投递**：同一窗口内同一事件 ID 视为重复（`DUPLICATE`），幂等忽略；
  聚合（count/sum/min/max）和首事件选择与到达顺序无关。
- **超前异常**：可选 `MaxOutofOrder`（`-max-ahead`），事件时间相对全局水位
  超前超过该上界的事件分类为 `OUT_OF_RANGE` 并可观测拒绝；默认关闭（0=不限）。

> 说明：在**跨源并发**摄取下，一条边界附近的事件是“乱序进入”还是“迟到丢弃”
> 取决于它相对水位推进的**真实到达顺序**（这是事件时间系统的标准行为）。
> 本引擎保证的是：对任一确定的到达序列，窗口结果唯一且确定；所有判定都在
> 错误对象中打印事件时间、当时水位与判定依据，可事后解释。

## 背压策略

- 每个源一个摄取 goroutine，把解析结果写入容量为 `SourceQueueSize`（`-queue`）
  的**有界通道**。
- 状态处理只有一个 goroutine（通过 `reflect.Select` 公平轮询各源）。
  队列满时该源 goroutine 在发送处阻塞 → 不再从文件读取 → 内存占用有界；
  有效事件**不会被静默丢弃**。
- 各源队列相互独立：快源被反压不会占用慢源的处理机会，慢源也不会长期阻塞
  整体吞吐（见 `TestEngineBackpressureDoesNotBlockOtherSources`）。
- 异常记录（格式错/时间戳非法）不进窗口，但同样被消费推进偏移并计入统计。

## 异常事件分类

| Kind               | 含义                                                       |
|--------------------|------------------------------------------------------------|
| `MALFORMED`        | 格式错误：字段数不对、JSON 损坏、缺 id/key、value 非数字等 |
| `INVALID_TIMESTAMP`| 时间戳缺失或无法解析                                       |
| `OUT_OF_RANGE`     | 事件时间相对全局水位超前超过允许范围（需开启 `-max-ahead`）|
| `LATE_DROPPED`     | 窗口关闭视界已越过，迟到事件被丢弃，不修改已关闭结果       |
| `DUPLICATE`        | 同窗口内事件 ID 重复，幂等忽略                             |

每条异常都带 `source / offset / 原始内容 / 事件时间 / 当前全局水位 / 判定原因`，
通过 `-errors` 指定文件（默认 stderr）输出。

## 长跑场景：状态有界范围与保留策略

长时间运行、键与窗口持续增长的场景下，状态体积只与以下三者相关，
**与累计处理过的窗口总量无关**：

1. **活跃（打开中的）窗口**：由水位推进自然约束——全局水位越过
   `window_end + lateness` 窗口即关闭，数量上界约为
   `(window + lateness) / window × 键基数`；
2. **仍在等待迟到的已发布结果（保留尾部）**：由 `EmittedRetention`
   （CLI `-retention`，单位秒）约束。全局水位越过
   `window_end + lateness + retention` 后，该窗口的已发布记录被安全遗忘：
   此后任何该窗口事件必然被水位判为 `LATE_DROPPED`（迟到语义不变），
   结果也早已恰好一次投递给汇（不丢不重）。
3. **每源摄取队列**：容量固定（`SourceQueueSize`），背压有界。

修剪在状态协程中随水位推进持续进行，因此内存与每次检查点的落盘体积
都稳定在与保留范围相称的水平（见 `TestLongRunBoundedState`：300 与
3000 个窗口两档总量，检查点均为 ~8KB）。

`EmittedRetention = 0`（默认）表示永久保留已发布结果，行为与旧版一致
（内存状态随累计窗口数增长，但检查点仍可用于恢复）；长跑部署建议设为非 0。

**保留策略与输出重建（重启后历史结果一条不少）**：有限的是**内存与检查点**，
不是输出。`-out` 指定的输出文件是一份**只增不改的完整结果历史**：

- 结果只在其所属状态成功进入检查点**之后**才追加写入输出文件（一次
  `write` 写完整一行并 `fsync`）；修剪只会把结果从内存/检查点遗忘，
  而能被修剪的结果在此前提交时必然已经完整落进输出文件，因此历史不丢。
- 重启时输出文件**不再被截断重写**。引擎先加载检查点，随后让
  `CommittedFileSink` 对输出文件做一次流式恢复对账（只读缓冲为常数大小，
  不把历史读入内存）：
  1. 逐行校验，确认完整行是合法结果、`emit_seq` 从 1 起**严格连续无重号**；
  2. 记录已完整持久化的最大 `emit_seq`（已持久化前缀）；
  3. 把文件末尾最后一个换行符之后的内容视为**崩溃半行**并剥除；
  4. 检查点保留尾部按 `emit_seq` 重放时，凡已在前缀内的结果一律跳过
     （不重），缺失的（含被剥掉半行的那个窗口、以及提交后尚未写完的
     窗口）按原序号补写（不漏），之后新结果继续追加。
- 因此无论保留范围多小、重启多少次（输入不变或只追加），输出文件始终
  包含**所有**已产出窗口，每个窗口恰好一行；内存占用与每次落盘的检查点
  体积仍只取决于活跃窗口、等待迟到的保留数据与保留范围。
- 若输出文件的**完整行**损坏或 `emit_seq` 出现断档（文件被外部截断或与
  别的运行混用），引擎以明确错误拒绝启动，而不会基于错误的前缀静默丢结果；
  无检查点的全新启动会截断旧输出从头开始。

`emit_seq` 跨修剪与恢复保持单调：检查点中的 `next_emit_seq` 在已发布
记录被修剪后仍记住下一个序号。

> 实现细节：最终排空（所有源 EOF）使用 +inf 哨兵水位，**不**触发修剪，
> 否则末次检查点会丢掉全部保留尾部，崩溃于末次提交之后时无法补写尾部；
> 更早的结果则始终由输出文件自身承载，不依赖检查点。

## 结果确定性保证

同一批逻辑输入，无论事件来自哪个源、多源并发如何交错、是否有重复
投递，每个窗口的聚合结果完全一致，重复运行可逐字节对比：

- **串行状态机**：所有窗口/水位/去重状态只在单一状态协程内修改，
  并发摄入不改变状态更新顺序的语义。
- **确定的关闭顺序**：窗口按 `(窗口起点, 键)` 排序关闭，`emit_seq`
  全局单调，与交错无关。
- **小数值的确定求和**：浮点加法不满足结合律，朴素累加的结果会随
  到达顺序在末位抖动。引擎为每个打开窗口保存原始值，关闭时先按
  全序（数值，再按比特位）排序，再做 Neumaier 补偿求和——累加顺序
  只取决于值的多重集合本身，因此 `Sum` 跨运行**逐比特一致**，且
  补偿求和压低了舍入误差（见 `deterministicSum` 与
  `TestDeterministicSumOrderIndependent`）。原始值随检查点落盘，
  崩溃恢复后继续累加仍然确定。
- **幂等去重**：重复投递（同窗口同事件 ID）被幂等忽略，不影响结果。
- **逐字节对比的口径**：`ClosedAt` 是墙钟时间、`Watermark` 取决于
  到达交错，二者不属于聚合值；对比时请用
  `key / window_start / count / sum / min / max / first_event_id /
  emit_seq`（测试中的 `detCanonical` 即此口径，Sum/Min/Max 按比特）。

## 运行状态摘要（体检）

`Engine.Status()` 返回 `StatusSnapshot`，可在摄入进行中从任意
goroutine 并发读取：不改配置、不中断摄入、只读原子量与队列长度，
对吞吐的影响可忽略。

| 字段              | 含义                                                     |
|-------------------|----------------------------------------------------------|
| `ActiveWindows`   | 当前打开中的窗口数（有界性的直接证据）                   |
| `WindowsClosed`   | 累计已最终关闭（终结）的窗口数                           |
| `EmittedRetained` | 当前仍保留的已发布结果数（受 `EmittedRetention` 约束）   |
| `Ingested` 等     | 摄入/接受/各类异常（MALFORMED、INVALID_TS、OUT_OF_RANGE、LATE_DROPPED、DUPLICATE）与检查点计数 |
| `GlobalWatermark` | 当前全局水位                                             |
| `Sources`         | 各源 `Offset`（已消费条数）、`Backlog/QueueCap`（积压）、`Finished`、`Watermark`，按源名排序 |

判断状态是否有界：长跑中 `ActiveWindows` 与 `EmittedRetained` 应稳定在
由窗口/迟到/保留配置决定的水平附近，不随 `WindowsClosed` 增长。
定位问题：`Backlog` 持续贴近 `QueueCap` 说明该源被反压；各类异常计数
对应分类异常表。注意摘要是无锁读取，单条在途记录可造成计数间不超过
“每源 1 条”的瞬时偏斜，趋势与量级不受影响。

CLI 用 `-status-interval N` 每 N 秒向 stderr 打印一行上述摘要。

## 检查点：兼容范围与拒绝原因

检查点是一个带魔数、版本与校验和的 JSON 文件：

```
magic="ETSR-CKPT", version=1, seq, created_at,
window_ns, lateness_ns,
sources: { name: { offset(已消费条数), watermark } },
window_state: [打开中的窗口：聚合值、原始事件值（确定求和用）、事件ID去重集合],
emitted:     [保留尾部内的已发布窗口结果（用于恢复重放：补写输出缺失尾部）],
next_emit_seq: 下一个发射序号（已发布记录被修剪后仍保持单调）,
checksum:    以上字段规范 JSON 的 SHA-256 hex
```

写入是原子的：同目录临时文件 → `fsync` → `rename` → 目录 `fsync`。

恢复时严格校验，失败返回 `CheckpointError`，其 `Kind` 为可区分原因：

| Kind                    | 含义                                             |
|-------------------------|--------------------------------------------------|
| `NOT_FOUND`             | 文件不存在（视为全新启动，非错误）               |
| `MALFORMED`             | 空文件/JSON 截断损坏/**校验和不符（被篡改）**    |
| `BAD_MAGIC`             | 不是本系统写出的检查点                           |
| `INCOMPATIBLE_VERSION`  | 版本号不是当前二进制支持的版本（当前仅 `1`）     |
| `BAD_CONFIG`            | 窗口大小/迟到配置不一致，或恢复源集合不一致      |

恢复流程：校验通过 → 还原窗口/去重状态与各源水位下界 → 按偏移跳过已消费记录 →
输出汇对账已有输出文件（保留完整行、剥除崩溃半行、记录已持久化 emit_seq
前缀）→ 按 `emit_seq` 重放检查点中的已提交尾部（前缀内的跳过）→ 再继续
消费。恢复期间不会发布任何未提交结果。

**兼容范围（当前版本 v1）**：仅接受 `version=1`、且窗口大小与允许迟到
与当前配置一致、源集合（按源名）完全一致的检查点。不同窗口/迟到语义会改变
结果含义，因此被明确拒绝而不是静默给出错误结果。

## 可配置项（`Config` / CLI flags）

| 配置项             | CLI flag     | 默认  | 说明                                              |
|--------------------|--------------|-------|---------------------------------------------------|
| `WindowSize`       | `-window`    | 60s   | 滚动窗口长度（秒）                                |
| `AllowedLateness`  | `-lateness`  | 10s   | 允许迟到；水位越过 `end+lateness` 后窗口最终关闭  |
| `SourceQueueSize`  | `-queue`     | 1024  | 每源有界摄取队列容量（背压粒度）                  |
| `CheckpointEvery`  | `-every`     | 1000  | 每处理多少条有效事件额外做一次周期检查点          |
| `MaxOutofOrder`    | `-max-ahead` | 0     | 事件时间相对全局水位的最大超前；0=不限            |
| `CheckpointPath`   | `-checkpoint`| 无    | 检查点文件路径；提供即启用崩溃恢复                |
| `EmittedRetention` | `-retention` | 0     | 已发布结果在**内存/检查点**中的保留秒数（事件时间）；0=永久保留。输出文件始终保留全部结果 |
| —                  | `-status-interval` | 0 | 运行中每 N 秒向 stderr 打印状态摘要；0=不打印     |

其他 CLI：`-inputs a.csv,b.csv`（多源，逗号分隔）、`-out`（JSON Lines 输出）、
`-errors`（分类异常输出）。

## 输入格式

每行一条记录，空行与 `#` 开头注释被跳过；两种格式可混用：

```
id,key,2026-01-01T00:00:01Z,1.5
{"id":"e2","key":"a","ts":"2026-01-01T00:00:02Z","v":2.5}
```

输出为 JSON Lines，字段包含键、窗口起止、count/sum/min/max、首事件 ID、
关闭时水位（最终排空为 `"EOI"`）与单调 `emit_seq`。

## 本地验证方法

```bash
# 编译与静态检查
go build ./...
go vet ./...

# 全部测试（含并发、乱序迟到、重复、检查点损坏与恢复重放、
#           长跑有界、顺序无关确定性、运行状态摘要）
go test ./...
go test -race -count=5 ./...          # 竞态检测 + 多轮调度压力

# 针对性验证新增能力（日志会打印各场景结论）
go test ./stream/ -run 'TestLongRun' -v        # 两档窗口总量下状态体积有界
go test ./stream/ -run 'TestOutputSurvivesRestartWithFiniteRetention' -v \
  # 300/3000 两档 + 有限保留 + 多次重启（含追加），输出与一次性运行逐窗口一致（含小数比特）
go test ./stream/ -run 'TestOutputTornLastLineRecovery' -v \
  # 写结果半行崩溃后的恢复：完整行保留、半行剥除、窗口不重不漏
go test ./stream/ -run 'TestOutputRejectsCorruptCompleteLines' -v \
  # 完整行损坏 / emit_seq 断档时拒绝启动
go test ./stream/ -run 'TestOutputRestartStateStaysBounded' -v \
  # 增量重启到 3000 窗：检查点始终 ~7KB，输出含全部 12000 条
go test ./stream/ -run 'Deterministic' -v      # 求和顺序无关 + 交错/重复投递下结果逐比特一致
go test ./stream/ -run 'TestStatusSnapshot' -v # 运行中读取状态摘要

# CLI：两个本地文件并发摄取
cat > /tmp/a.csv <<'CSV'
a1,a,2026-01-01T00:00:05Z,1
a3,a,2026-01-01T00:00:30Z,3
a2,a,2026-01-01T00:00:10Z,2
a4,a,2026-01-01T00:01:05Z,3
a5,a,2026-01-01T00:00:59Z,99
CSV
cat > /tmp/b.csv <<'CSV'
b1,b,2026-01-01T00:00:10Z,5
b2,b,2026-01-01T00:00:40Z,7
CSV

# 无检查点（内存确定性运行）
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv -window 60 -lateness 0

# 启用检查点（崩溃恢复；输出文件追加保留全部历史，启动时对账去重，不重不漏）
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv \
  -checkpoint /tmp/ck.json -out /tmp/out.jsonl -errors /tmp/err.log \
  -window 60 -lateness 0 -every 1
# 再次运行（模拟重启，输入不变）：ingested=0，windows=0，输出行数不变（不重发）
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv \
  -checkpoint /tmp/ck.json -out /tmp/out.jsonl -window 60 -lateness 0 -every 1
# 模拟输出最后一行写了一半后崩溃：剥掉半行、重放补写，行数恢复且该窗口不重复
size=$(stat -c%s /tmp/out.jsonl); truncate -s $((size-30)) /tmp/out.jsonl
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv \
  -checkpoint /tmp/ck.json -out /tmp/out.jsonl -window 60 -lateness 0 -every 1
# 损坏检查点：以可区分原因拒绝（退出码 3）
echo '{"magic":"ETSR-CKPT"' > /tmp/bad.json
go run ./cmd/etsr -inputs /tmp/a.csv -checkpoint /tmp/bad.json -out /dev/null
```

上例中 `a5`（00:00:59）在全局水位被 `a4`（00:01:05）推过关闭视界后到达，
被分类为 `LATE_DROPPED`，`[00:00,00:01)` 的 `a` 窗口结果保持 `count=3,sum=6`
不变。

长跑有界性与状态摘要也可以从 CLI 直接观察：

```bash
# 生成 600 个分钟窗、3 个键的长跑输入
cat > /tmp/gen.go <<'EOF'
package main

import ("fmt"; "os"; "time")

func main() {
	f, _ := os.Create("/tmp/long.csv")
	defer f.Close()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for w := 0; w < 600; w++ {
		for k := 0; k < 3; k++ {
			ts := base.Add(time.Duration(w)*time.Minute + time.Duration(k)*time.Second)
			fmt.Fprintf(f, "w%dk%d,key%d,%s,1.5\n", w, k, k, ts.Format(time.RFC3339))
		}
	}
}
EOF
go run /tmp/gen.go

# 保留 120s：检查点体积只与保留尾部相称，不随 600 个窗口的总量增长；
# 输出文件追加写、重启不截断，无论重启多少次都保留全部窗口结果。
go run ./cmd/etsr -inputs /tmp/long.csv -window 60 -lateness 5 -retention 120 \
  -checkpoint /tmp/long-ck.json -out /tmp/long-out.jsonl -every 50 -status-interval 1
ls -l /tmp/long-ck.json     # 只有几 KB；把 600 改成 6000 再跑，体积基本不变
wc -l /tmp/long-out.jsonl   # 结果不丢不重：600 窗 x 3 键 = 1800 行
# 同输入重启：输出仍为 1800 行（windows=0，前缀结果去重跳过）；
# 往 long.csv 追加若干窗再重启：行数恰为新的窗口总数，历史一行不少。
```

## 测试覆盖一览

- `parse_test.go`：CSV/JSONL 解析、空行注释、各类格式与时间戳错误分类。
- `window_test.go`：多源最小水位与单调性、乱序进入正确窗口、
  水位关闭后迟到不可变、重复 ID 幂等、关闭顺序确定。
- `engine_test.go`：全部分类（含 OUT_OF_RANGE/LATE_DROPPED）的确定性判据、
  8 轮并发结果一致性、慢源背压不死锁不丢事件。
- `checkpoint_test.go`：往返序列化、空文件/截断/篡改校验和/bad magic/
  版本不兼容/窗口与迟到配置不一致。
- `recovery_test.go`：分阶段文件增长+重启与一次性运行结果逐窗口一致、
  `emit_seq` 无重号、损坏检查点拒绝、崩溃发生在提交前时无未提交结果外泄。
- `longrun_test.go`：两档差 10 倍的累计窗口总量（300/3000 窗）下，
  运行中轮询 `Status` 验证活跃窗口数与保留尾部有界、最终检查点体积
  不随总量增长；保留策略下重启只重放有界尾部、`emit_seq` 跨修剪单调、
  多阶段结果并集与一次性运行逐窗口一致。
- `output_recovery_test.go`（追加式输出汇的重启恢复）：
  - `TestOutputSurvivesRestartWithFiniteRetention`：300/3000 两档差十倍
    总量、有限保留（2 分钟，对两档都大量修剪）、纯重启 + 追加后再重启，
    输出文件与一次性运行**逐窗口**对比 `count/sum/min/max(按浮点比特)/
    first_event_id` 完全一致，行数=窗口总数、`emit_seq` 连续无重号；
  - `TestOutputTornLastLineRecovery`：故障注入“检查点已提交、结果行
    写到一半崩溃”，重启后此前完整行原样保留、半行被剥除，截断窗口由
    重放恰好补写一次；再追加数据两次重启仍与一次性运行一致；无换行
    收尾（哪怕恰好可解析）的末行同样不信任；
  - `TestOutputRejectsCorruptCompleteLines`：完整行损坏 / `emit_seq`
    断档时引擎明确报错拒绝启动，不静默丢历史；
  - `TestOutputRestartStateStaysBounded`：从 300 窗逐段追加、9 次重启
    累积到 3000 窗，检查点峰值 ~7KB（与一次跑到 3000 相当），输出
    文件含全部 12000 条结果、每个窗口恰好一行。
- `determinism_test.go`：求和原语对任意打乱逐比特稳定（含朴素累加
  随顺序变化的反例对照）；同一逻辑输入在单源顺序/多源交错/重复投递/
  启用检查点四种形态下，逐窗口聚合结果（含小数 Sum 的比特）完全一致。
- `status_test.go`：运行中并发读取状态摘要——活跃窗口、各源积压与
  偏移、分类计数满足一致性不变量，结束后与 `Report` 对齐。

失败用例的日志会打印事件时间、全局水位、检查点版本与具体判定依据；
长跑与确定性用例的日志会打印两档规模的状态体积、保留尾部大小与
对比结论（`go test -v` 可见）。
