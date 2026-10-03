# event-time-stream-runtime-go

一个可在**本地**运行的实时事件流处理引擎：从多个本地文件并发摄取事件，
按**事件时间**做键控窗口聚合与水位推进，以**有界状态**处理乱序与迟到，
并通过**一致性检查点**保证故障重启后结果不重、不漏、可解释。

- 无外部服务、无网络依赖、无真实凭据：只使用 Go 标准库与本地文件。
- 并发摄入：每源独立 goroutine + 有界队列，背压传导到底层读取，慢源不长期阻塞其他源。
- 事件时间语义：每源水位 = 已观测最大事件时间；全局水位 = 活跃源水位最小值。
- 窗口在 `window_end + allowed_lateness` 的关闭视界被水位越过后**最终关闭**；
  之后到达的迟到事件被明确分类为 `LATE_DROPPED`，绝不修改已关闭结果。
- **长跑有界状态**：已发布结果只在 `关闭视界 + emitted_retention` 的保留范围内
  留存于内存与检查点；水位越过后安全遗忘。内存与落盘进度体积只跟随活跃窗口、
  迟到视界与保留范围，不随累计窗口数膨胀。
- **结果与到达顺序无关**：单线程串行状态处理 + 顺序无关的聚合归约
  （count/min/max；sum 用**精确有理数累加器**，小数累加不受顺序影响；
  首事件取 `(事件时间, 事件ID)` 最小者）。开启 `-deterministic` 后
  `closed_at` 也取确定性值，同一批逻辑输入重复运行输出**逐字节一致**。
- **运行状态体检**：`Engine.Status()`（或 CLI `-status-interval`）运行中随时可读
  活跃窗口数、已终结窗口数、各源积压与各类异常/迟到计数；只读原子量，
  不需要停止摄入，基本不影响吞吐。
- 一致性检查点：原子写（tmp + fsync + rename）、SHA-256 校验和、
  版本号魔数与窗口配置校验；损坏/缺失/版本不兼容/配置不一致均以**可区分原因**拒绝。
- 提交后发布：窗口结果只在其所属状态成功落盘检查点后才写入输出；
  恢复时先按 `emit_seq` 确定序重放保留的已提交结果，再处理新事件；
  文件汇追加+幂等去重，保留范围之外的历史结果也不会因重启丢失。

## 目录结构

```
stream/                 库包
  types.go              事件、窗口结果、配置、统计
  errors.go             异常事件分类（ClassifiedError）
  source.go             Source 接口、CSV/JSONL 解析、内存源/文件源
  watermark.go          每源水位与全局最小水位
  window.go             键控窗口、确定性聚合、迟到判定、去重、快照、保留范围剪枝
  exactsum.go           顺序无关的精确小数累加器（big.Rat）
  checkpoint.go         版本化检查点、校验和、原子写、分类拒绝
  sink.go               JSONSink / MemorySink / CommittedFileSink（追加+幂等）
  status.go             运行中状态摘要（Status / Engine.Status）
  engine.go             并发摄取编排、背压、提交-发布协议
  *_test.go             全部语义、恢复、长跑有界、确定性与状态摘要测试
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

## 长跑场景：状态有界范围与保留策略

同一进程长时间运行、键与窗口持续增长时，状态体积只与以下三者相关，
**不随累计处理过的窗口数膨胀**：

| 状态                     | 有界范围                                                     |
|--------------------------|--------------------------------------------------------------|
| 打开中的窗口             | 水位推进速度 × 键基数（另加每源摄取队列容量的瞬时值）        |
| 窗口内事件 ID 去重集合   | 各打开窗口自身的大小，窗口关闭即释放                         |
| 已发布结果（内存+检查点）| 保留范围：`windowEnd + allowedLateness + emittedRetention`   |

- **遗忘规则**：全局水位越过 `windowEnd + allowedLateness + emittedRetention`
  （CLI `-retention`，默认 3600 秒；0 表示关闭视界一过即遗忘）后，该窗口的
  已发布记录从内存与下一次检查点中移除。此后该窗口的任何事件必然被水位视界
  判为 `LATE_DROPPED`——迟到判定不依赖保留记录，因此**迟到语义完全不变**。
- **发射序号单调**：`emit_seq` 的下一个值持久化在检查点 `next_emit_seq` 中，
  旧记录被遗忘后序号也不会回退重用。
- **结果不丢不重**：`CommittedFileSink` 以追加模式打开输出文件，启动时扫描已有
  内容建立去重集合（崩溃半截行会被截掉）；恢复重放时已在文件中的窗口被幂等
  跳过。因此即使检查点只保留了尾部结果，输出文件始终是已提交结果的全集。
- 剪枝只跟随**真实观测到的水位**；最终排空的 EOI（+inf）哨兵只用于关闭窗口，
  不会触发遗忘，重启重放所需的尾部结果始终保留。

## 结果确定性保证

对同一批**逻辑输入**（同一事件集合），无论事件来自几个源、多源如何交错、
是否重复投递、重复运行多少次，每个窗口的聚合值完全一致：

- **串行状态处理**：所有窗口/水位/去重状态只在单一状态协程中修改。
- **顺序无关归约**：count/min/max 天然满足交换结合律；首事件取
  `(事件时间, 事件ID)` 最小者；重复 ID 幂等忽略。
- **小数精确累加**：`sum` 不用 float64 逐步累加（加法顺序会影响末位），
  而是用 `big.Rat` 保存精确和——每个 float64 都是二进制有理数，可精确表示，
  加法精确且满足交换/结合律；仅在输出时一次性确定性地舍入回 float64。
  因此 `0.1+0.2+...`、`1e16+1-1e16` 这类顺序敏感累加，任何到达顺序结果
  逐位一致（后者恒为 `1`）。精确和随检查点 `sum_exact` 字段持久化，
  恢复后继续累加仍然顺序无关。
- **确定性时间戳**：开启 `DeterministicClosedAt`（CLI `-deterministic`）后，
  `closed_at` 取窗口关闭视界（`windowEnd+lateness`）而非墙钟；配合固定的
  关闭顺序（按 `(窗口起点, 键)`）与单调 `emit_seq`，重复运行的输出文件
  可**逐字节对比**（见 `TestDeterministicResultsRegardlessOfOrder`）。

> 注意：确定性保证针对“逻辑输入集合”。若不同运行中事件相对水位的真实到达
> 顺序不同，边界事件可能被分别判为乱序进入或迟到丢弃（标准事件时间语义）；
> 测试中用足够大的 `allowedLateness` 固定逻辑接受集合来验证逐字节一致。

## 运行状态摘要（体检）

`Engine.Status()` 返回一次只读快照，可在运行中随时、并发地调用：
全部数据来自原子量与队列长度，**不需要停止摄入、不改配置、基本不影响吞吐**。
CLI 用 `-status-interval N` 每 N 秒向 stderr 打印一行 JSON。

```json
{
  "running": true,
  "global_watermark": "2026-01-01T00:03:41Z",
  "active_windows": 3,        // 打开中的窗口数（应只跟随活跃范围）
  "retained_emitted": 12,     // 保留范围内的已发布窗口数（应有界）
  "windows_emitted": 531,     // 累计已终结窗口数（单调递增）
  "ingested": 10000, "accepted": 9998,
  "malformed": 1, "invalid_ts": 0, "out_of_range": 0,
  "late_dropped": 1, "duplicates": 0, "checkpoints": 12,
  "sources": [
    {"name": "a.csv", "offset": 5000, "queued": 2, "capacity": 1024,
     "watermark": "2026-01-01T00:03:41Z", "finished": false}
  ]
}
```

判读要点：

- **状态是否有界**：长跑中 `active_windows` 与 `retained_emitted` 应稳定在
  由键基数、迟到视界与保留范围决定的水平，不随 `windows_emitted` 增长。
- **积压定位**：某源 `queued` 持续贴近 `capacity` 说明该源被反压；
  各源 `offset`/`watermark` 对比可找出拖慢全局水位的慢源。
- **异常画像**：`malformed / invalid_ts / out_of_range / late_dropped /
  duplicates` 分类计数与 `-errors` 文件中的明细一一对应。


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

## 检查点：兼容范围与拒绝原因

检查点是一个带魔数、版本与校验和的 JSON 文件：

```
magic="ETSR-CKPT", version=2, seq, created_at,
window_ns, lateness_ns,
next_emit_seq: 下一个发射序号（已发布记录被遗忘后仍单调）,
sources: { name: { offset(已消费条数), watermark } },
window_state: [打开中的窗口、事件ID去重集合、sum_exact(精确和)],
emitted:     [保留范围内的已发布窗口结果（用于恢复重放）],
checksum:    以上字段规范 JSON 的 SHA-256 hex
```

写入是原子的：同目录临时文件 → `fsync` → `rename` → 目录 `fsync`。
`emitted` 只保留 `关闭视界 + emittedRetention` 以内的尾部（见上一节），
因此长跑下检查点体积有界。

恢复时严格校验，失败返回 `CheckpointError`，其 `Kind` 为可区分原因：

| Kind                    | 含义                                             |
|-------------------------|--------------------------------------------------|
| `NOT_FOUND`             | 文件不存在（视为全新启动，非错误）               |
| `MALFORMED`             | 空文件/JSON 截断损坏/**校验和不符（被篡改）**    |
| `BAD_MAGIC`             | 不是本系统写出的检查点                           |
| `INCOMPATIBLE_VERSION`  | 版本号不是当前二进制支持的版本（当前仅 `2`）     |
| `BAD_CONFIG`            | 窗口大小/迟到配置不一致，或恢复源集合不一致      |

恢复流程：校验通过 → 还原窗口/去重状态与各源水位下界 → 按偏移跳过已消费记录 →
先重放保留的已提交结果（按 `emit_seq`）→ 再继续消费。恢复期间不会发布任何
未提交结果；`CommittedFileSink` 追加+幂等去重，保留范围之外的历史结果
在输出文件中不受影响。

**兼容范围（当前版本 v2）**：仅接受 `version=2`、且窗口大小与允许迟到
与当前配置一致、源集合（按源名）完全一致的检查点。不同窗口/迟到语义会改变
结果含义，因此被明确拒绝而不是静默给出错误结果。v1 检查点（无
`next_emit_seq`/`sum_exact`）以 `INCOMPATIBLE_VERSION` 拒绝。

## 可配置项（`Config` / CLI flags）

| 配置项                 | CLI flag         | 默认  | 说明                                              |
|------------------------|------------------|-------|---------------------------------------------------|
| `WindowSize`           | `-window`        | 60s   | 滚动窗口长度（秒）                                |
| `AllowedLateness`      | `-lateness`      | 10s   | 允许迟到；水位越过 `end+lateness` 后窗口最终关闭  |
| `SourceQueueSize`      | `-queue`         | 1024  | 每源有界摄取队列容量（背压粒度）                  |
| `CheckpointEvery`      | `-every`         | 1000  | 每处理多少条有效事件额外做一次周期检查点          |
| `MaxOutofOrder`        | `-max-ahead`     | 0     | 事件时间相对全局水位的最大超前；0=不限            |
| `CheckpointPath`       | `-checkpoint`    | 无    | 检查点文件路径；提供即启用崩溃恢复                |
| `EmittedRetention`     | `-retention`     | 3600s | 已发布结果在关闭视界后的保留范围；0=关闭即遗忘    |
| `DeterministicClosedAt`| `-deterministic` | false | ClosedAt 取关闭视界而非墙钟，重复运行输出逐字节一致 |

其他 CLI：`-inputs a.csv,b.csv`（多源，逗号分隔）、`-out`（JSON Lines 输出）、
`-errors`（分类异常输出）、`-status-interval N`（每 N 秒向 stderr 打印状态摘要）。

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
# 长跑有界、确定性、运行状态摘要）
go test ./...
go test -race -count=5 ./...          # 竞态检测 + 多轮调度压力

# 针对性验证新能力（-v 日志会列出每个场景与结论）
go test -v -run TestLongRunStateBounded ./stream/               # 两档窗口总量对比状态体积
go test -v -run TestRestartWithPrunedEmittedKeepsOutput ./stream/ # 保留范围=0 重启不丢不重
go test -v -run TestDeterministicResultsRegardlessOfOrder ./stream/ # 打乱/重复投递/重跑逐字节一致
go test -v -run TestStatus ./stream/                            # 运行中读取状态摘要

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

# 启用检查点（崩溃恢复；输出文件追加+幂等去重，重启后不重不漏）
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv \
  -checkpoint /tmp/ck.json -out /tmp/out.jsonl -errors /tmp/err.log \
  -window 60 -lateness 0 -every 1
# 再次运行（模拟重启，输入不变）：ingested=0，保留的已提交结果按 emit_seq
# 重放并被文件汇幂等跳过，输出文件内容不变
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv \
  -checkpoint /tmp/ck.json -out /tmp/out.jsonl -window 60 -lateness 0 -every 1
# 损坏检查点：以可区分原因拒绝（退出码 3）
echo '{"magic":"ETSR-CKPT"' > /tmp/bad.json
go run ./cmd/etsr -inputs /tmp/a.csv -checkpoint /tmp/bad.json -out /dev/null

# 长跑有界 + 运行状态体检 + 确定性输出：
# 保留范围 60s，每 2 秒打印一次状态摘要（active_windows/retained_emitted 应有界）
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv \
  -checkpoint /tmp/ck2.json -out /tmp/out2.jsonl \
  -window 60 -lateness 0 -retention 60 -status-interval 2 -deterministic
# 确定性验证：同一输入重复运行，输出逐字节一致
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv -window 60 -lateness 0 -deterministic > /tmp/o1.jsonl
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv -window 60 -lateness 0 -deterministic > /tmp/o2.jsonl
diff /tmp/o1.jsonl /tmp/o2.jsonl && echo BYTE-IDENTICAL
```

上例中 `a5`（00:00:59）在全局水位被 `a4`（00:01:05）推过关闭视界后到达，
被分类为 `LATE_DROPPED`，`[00:00,00:01)` 的 `a` 窗口结果保持 `count=3,sum=6`
不变。

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
- `longrun_test.go`：两档差别很大的窗口总量（500 vs 5000 个窗口）对比——
  活跃窗口数、保留已发布数与检查点体积不随总量增长；保留范围=0 下重启，
  输出文件 400/400 行不丢不重、检查点不膨胀。
- `determinism_test.go`：同一批逻辑输入在打乱顺序、多源交错、重复投递、
  重复运行下输出逐字节一致；`1e16+1-1e16` 顺序敏感累加恒为精确值 `1`。
- `status_test.go`：运行前/运行中/运行后读取状态摘要——运行标志、活跃窗口、
  各源积压（快源+慢汇制造背压）、分类计数单调且与 `Report()` 一致。

失败用例的日志会打印事件时间、全局水位、检查点版本与具体判定依据；
长跑/确定性/状态摘要用例的 `-v` 日志逐条列出场景与结论。
