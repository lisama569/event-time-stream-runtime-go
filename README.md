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
- 提交后发布：窗口结果只在其所属状态成功落盘检查点后才追加进输出文件
  （逐条 fsync）；恢复时先按 `emit_seq` 确定序重放保留尾部的已提交结果，
  文件汇按已落盘高水位幂等跳过，再处理新事件。
- 长跑有界状态：`EmittedRetention` 限定已发布结果的保留范围，
  内存与检查点体积只取决于活跃窗口与保留尾部，与累计窗口总量无关。
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
  sink.go               JSONSink / MemorySink / CommittedFileSink
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
（状态随累计窗口数增长），便于兼容既有部署。

**保留策略的恢复语义**：检查点只保留尾部已发布结果，但输出文件
（`CommittedFileSink`）在重启时**不截断**——历史结果早已逐条追加并
fsync 落盘，保留范围之外的旧结果只存在于输出文件中，重启后原样保留，
一条不少。恢复重建按如下协议保证不重不漏：

- 引擎重放检查点保留尾部的已提交结果（按 `emit_seq` 确定序）；
- 文件汇启动时扫描既有内容，记住“已完整落盘的最大 `emit_seq`”
  （高水位）。重放或新发射的结果只要 `emit_seq` 不超过高水位，就必然
  已在文件中（每条结果提交后才追加并 fsync，文件始终是全局发射序列的
  一段前缀），幂等跳过；只有缺失的尾部才真正补写；
- 去重只记一个高水位序号，内存 O(1)，不随累计窗口总量增长；
- `emit_seq` 跨修剪与恢复保持单调：检查点中的 `next_emit_seq` 在已发布
  记录被修剪后仍记住下一个序号。

因此无论 `-retention` 设成多少，重启（含多次重启、追加数据）后的输出
文件都与一次性运行逐窗口一致。全新启动（无有效检查点）时引擎会调用
文件汇的 `Reset` 截断清零，避免上一轮无关运行的残留输出混入新结果。

> 实现细节：最终排空（所有源 EOF）使用 +inf 哨兵水位，**不**触发修剪，
> 否则末次检查点会丢掉全部保留尾部，重启时无法补齐输出尾部。

## 输出文件：崩溃恢复与残缺行处理

`CommittedFileSink`（`-checkpoint` 与 `-out` 同时提供时启用）与检查点
协议配套，保证输出文件在任意崩溃点下都可恢复到“不重、不漏、无半行”：

- **每条结果 = 一行 JSON Lines，写完立即 fsync**。结果只在其所属检查点
  提交成功后才追加，因此文件内容始终是全局发射序列（`emit_seq` 升序）
  的一段前缀，外加崩溃瞬间可能留下的**最后半行**。
- **启动扫描**：不截断文件。逐行校验（必须以换行结尾且是合法结果
  JSON），用完整行恢复“已落盘高水位 `emit_seq`”；遇到残缺/损坏行
  （只可能来自崩溃瞬间的半写）在该行起点截断——半行不会污染后续输出。
- **幂等补写**：恢复重放保留尾部时，`emit_seq` 不超过高水位的结果必然
  已在文件中，直接跳过；只有崩溃前来不及写（或半行被裁掉）的尾部才
  补写。被截掉半行的那个窗口由检查点重放恰好重写一次。
- **全新启动才清零**：无有效检查点时引擎调用 `Reset` 截断文件；
  有检查点时绝不截断——保留范围之外的历史结果只存在于输出文件中。

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
emitted:     [保留尾部内的已发布窗口结果（用于输出重建与恢复重放）],
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
先重放检查点保留尾部的已提交结果（按 `emit_seq`；文件汇按已落盘高水位幂等
跳过，只补齐缺失尾部）→ 再继续消费。恢复期间不会发布任何未提交结果。

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
| `EmittedRetention` | `-retention` | 0     | 已发布结果保留秒数（事件时间）；0=永久保留        |
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
go test ./stream/ -run 'TestLongRun' -v        # 两档窗口总量下状态体积有界 + 保留策略下恢复不重不漏
go test ./stream/ -run 'TestOutput' -v         # 重启后输出保留全部历史 + 半行崩溃恢复 + 跨重启小数逐比特一致
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

# 启用检查点（崩溃恢复；输出文件不截断，按已落盘高水位幂等续写，不重不漏）
go run ./cmd/etsr -inputs /tmp/a.csv,/tmp/b.csv \
  -checkpoint /tmp/ck.json -out /tmp/out.jsonl -errors /tmp/err.log \
  -window 60 -lateness 0 -every 1
# 再次运行（模拟重启，输入不变）：ingested=0，保留尾部重放被文件汇幂等跳过，
# 输出文件一行不多一行不少
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

# 保留 120s：检查点体积只与保留尾部相称，不随 600 个窗口的总量增长
go run ./cmd/etsr -inputs /tmp/long.csv -window 60 -lateness 5 -retention 120 \
  -checkpoint /tmp/long-ck.json -out /tmp/long-out.jsonl -every 50 -status-interval 1
ls -l /tmp/long-ck.json     # 只有几 KB；把 600 改成 6000 再跑，体积基本不变
wc -l /tmp/long-out.jsonl   # 结果不丢不重：600 窗 x 3 键 = 1800 行
# 同一检查点重启（输入不变或追加）：保留范围之外的历史结果仍留在输出文件中，
# 行数不变（或只追加新窗口），绝不回退到只剩保留尾部
go run ./cmd/etsr -inputs /tmp/long.csv -window 60 -lateness 5 -retention 120 \
  -checkpoint /tmp/long-ck.json -out /tmp/long-out.jsonl -every 50
wc -l /tmp/long-out.jsonl   # 仍是 1800 行
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
- `output_recovery_test.go`：输出文件的重启恢复语义——两档窗口总量
  （300/3000 窗）+ 有限保留范围下经“部分运行 → 追加数据重启 → 空重启”
  三个阶段，输出文件始终保留全部历史结果且与一次性运行逐窗口一致；
  模拟写入过程中崩溃（尾部半行 / 最后一行拦腰截断）后完整行原样保留、
  半行裁掉、被截窗口恰好补写一次；处理中途崩溃（窗口半开进检查点）
  跨重启续算的小数聚合值与一次性运行逐比特一致。
- `determinism_test.go`：求和原语对任意打乱逐比特稳定（含朴素累加
  随顺序变化的反例对照）；同一逻辑输入在单源顺序/多源交错/重复投递/
  启用检查点四种形态下，逐窗口聚合结果（含小数 Sum 的比特）完全一致。
- `status_test.go`：运行中并发读取状态摘要——活跃窗口、各源积压与
  偏移、分类计数满足一致性不变量，结束后与 `Report` 对齐。

失败用例的日志会打印事件时间、全局水位、检查点版本与具体判定依据；
长跑与确定性用例的日志会打印两档规模的状态体积、保留尾部大小与
对比结论（`go test -v` 可见）。
