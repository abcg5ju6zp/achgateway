# ODFI 文件分阶段消费与故障恢复 实施计划

## 背景与仓库调研结论

调度链路位于 `internal/incoming/odfi`（scheduler → download → processor → cleanup），影响面完全收敛在该包内：

- [scheduler.go](file:///Users/huangding/Documents/new-SWE/0910142/project-03/internal/incoming/odfi/scheduler.go#L149-L194)：每个 tick 对每个 shard 执行「下载全部 → `ProcessFiles` → `Cleanup` → 零字节清理 → 本地清理」。
- [download.go](file:///Users/huangding/Documents/new-SWE/0910142/project-03/internal/incoming/odfi/download.go#L110-L167)：每 tick 在 storage 目录下建**随机临时目录**，按 inbound/reconciliation/return 三类列远端目录并整文件下载；无任何持久状态。
- [processor.go](file:///Users/huangding/Documents/new-SWE/0910142/project-03/internal/incoming/odfi/processor.go#L131-L175)：逐文件 解析 ACH → `persistInboundFile` 存审计 → 各 `FileProcessor` 发事件。处理器错误会 `alerters.AlertError` 并向上返回错误；解析错误直接返回（由 tick 层统一告警）。
- [cleanup.go](file:///Users/huangding/Documents/new-SWE/0910142/project-03/internal/incoming/odfi/cleanup.go#L71-L105)：按本地临时目录内容反推远端路径调 `agent.Delete`；仅当 `ProcessFiles` 整体无错才执行。删除超时/不确定时无法区分「已删」与「未删」。
- [upload/agent.go](file:///Users/huangding/Documents/new-SWE/0910142/project-03/internal/upload/agent.go#L18-L36)：FTP/SFTP/Mock 三种 Agent 均提供 `GetInboundFiles/GetReconciliationFiles/GetReturnFiles`（底层 `ListFiles`），可用于删除不确定时**查询远端是否仍存在**；Agent 不支持字节范围续传。
- 空文件经 `ach.Reader` 解析返回「无错的零 File」（已实测），现有行为是：处理器全部跳过、审计跳过、随后 `CleanupEmptyFiles` 删远端零字节文件。
- 集成测试 `internal/test/download_test.go` 断言现有本地目录布局，但带 `testing.Short()` 跳过（依赖 SFTP 服务，本机不具备），会同步更新其断言；可在本机运行的包内单测必须全部保持通过。

### 当前故障窗口（本次要消除的）

1. 事件已发、审计未落盘（或反之）时崩溃 → 下轮重放，事件重复。
2. 远端删除超时、结果未知 → 下轮重新下载处理，事件重复；或在处理未完成时误删。
3. 下载中断 → 随机临时目录丢失，整批重下。
4. 单文件业务失败会阻断/牵连其他文件的删除；失败证据留在随机临时目录中，重启后散失。
5. 同名文件仅靠本地 `<kind>/<name>` 隐式区分，无稳定消费身份，重启后无法接续或查询结果。

## 设计要点

### 1. 稳定消费身份与持久化布局（新文件 `consumption.go`）

- 身份：`consumptionID = sha256(host + NUL + remotePath + NUL + contentSHA256)`，其中 `host = agent.Hostname()`，`remotePath` 规范化为 slash 形式（如 `inbound/foo.ach`），`contentSHA256` 为下载落盘字节的摘要。
- storage 目录（沿用现有 `ODFIStorage.Directory`，默认 `storage`，**不引入新配置、不依赖数据库/Docker**）下新增持久布局：

```
<baseDir>/
  staging/<tmp...>                       # 下载暂存，提交后删除；启动时清扫残留
  consumption/<host转义>/<remotePath>/<contentSHA256>/
      data        # fsync 的下载副本（本地证据）
      state.json  # 分阶段状态（tmp+rename 原子写、父目录 fsync）
      error.txt   # 仅失败时写入的错误证据
```

- host 用转义目录名（替换 `:` 等），remotePath 保留 `inbound/`、`returned/` 等前缀目录 → **不同类目录同名文件天然隔离**；同路径内容变化产生新的 digest 子目录（旧实例独立收尾）。
- `state.json` 记录：id、host、remotePath、kind 目录、filename、sha256、size、各阶段状态/时间戳/细节（审计路径、解析错误、逐处理器结果）、最近错误、更新时间。
- 阶段状态机：`downloaded → validated → audited → processors.<Type()> → remote_deleted`，状态值 `confirmed / failed / skipped / uncertain`；进程内按 id 加锁。

### 2. 下载阶段（改造 `download.go`）

- 仍先列三类远端目录（metrics `files_downloaded`、目录标记跳过、空列表不建目录等行为不变），逐文件写入**暂存区**并流式计算摘要，成功 `fsync` 后提交进消费槽位：原子放置 `data`、写 `downloaded=confirmed`。
- 中断/失败的下载只留下未确认暂存，下轮重下该文件；**已确认副本不重下、不重处理**（协议不支持范围续传，采用「重下未确认内容」）。
- 提交幂等：相同 (host,path,digest) 已存在则复用原状态，不覆盖证据。
- `downloadedFiles` 保留 `dir`（暂存目录）及 `deleteFiles/deleteEmptyDirs`（现有单测继续适用），新增持有 store 与本 tick 提交/恢复的 units。

### 3. 校验/审计/处理器阶段（改造 `processor.go`）

- 新增 `processUnit`：严格按阶段跳过已确认步骤；`processDir/processFile` 保留供既有单测使用，解析逻辑抽成共享函数，确保无效 ACH、容忍 `ach.ErrFileHeader` 等判定与现在完全一致。
- 审计成功后才写 `audited=confirmed`（空内容按现有逻辑记 `skipped`）；审计路径规则 `odfi/<host>/<kind>/<YYYY-MM-DD>/<filename>` 不变。
- 处理器按 `Type()` 逐个确认：`Handle` 返回 nil 立即落盘该处理器 `confirmed`；恢复时已确认的处理器**整体跳过，不再发事件**。处理器失败：记录 failed + `error.txt`、调用 `alerters.AlertError`（与现在一致），并继续处理其他文件。
- 处理器错误计数 metric、告警路径、错误向上聚合后由 tick 层再次告警的现有行为保持不变。

### 4. 远端删除阶段（改造 `cleanup.go`）

- 删除按 unit 独立门禁：只有该文件审计与全部处理器 confirmed/skipped 后才允许删；**业务失败的文件保留远端、保留本地证据**，且不牵连其他成功文件的删除。
- `agent.Delete` 结果三分：
  - nil 或 `os.IsNotExist` → `confirmed`；
  - 其他错误（超时等）→ **立即重新 List 该类远端目录查询**：文件已不在 → `confirmed`；仍在 → `uncertain` 并保留错误，下轮重试（重试时先查状态，避免盲目再删）；列举本身失败 → 维持 `uncertain`。
- `KeepRemoteFiles=true` → 记 `skipped` 收尾；`RemoveZeroByteFiles` 仍按 size==0 删远端（经过同一不确定判定），其错误仍聚合进 tick 错误。

### 5. 调度编排与重启恢复（改造 `scheduler.go`）

- tick 流程：下载提交 → 汇总「本 tick units + store 中所有未完成 units（去重）」→ 逐单元续作校验/审计/处理器 → 达门禁的单元执行删除 → 零字节清理 → 本地垃圾回收 → 始终清理暂存目录。
- 重启后：启动时清扫未确认 staging；每 tick 自动只接续未完成阶段；完成单元直接跳过；可跨 tick 完成「删除结果未知 → 查询确认」。
- 本地回收：`CleanupLocalDirectory=true` 时删除**已全部完成**单元的槽位目录（失败证据始终保留）；`false` 时保留证据（与现状一致）；同路径被新 digest 取代时，旧的已完成 digest 目录按同一规则回收。

### 6. 最终结果查询（扩展 `admin.go`）

- 新增 `GET /inbound/consumption`（可选 `?id=`）：返回各消费单元的 id、host、path、digest、各阶段状态、错误与时间戳 JSON；`/trigger-inbound` 行为不变。

## 变更文件清单

- 新增 `internal/incoming/odfi/consumption.go`：稳定身份、原子状态存储、槽位扫描/恢复/回收。
- 改 `internal/incoming/odfi/download.go`：暂存+摘要+提交，持有 store/units。
- 改 `internal/incoming/odfi/processor.go`：`processUnit` 阶段机；复用现有解析与告警逻辑。
- 改 `internal/incoming/odfi/cleanup.go`：按 unit 门禁删除 + 不确定时远端状态查询。
- 改 `internal/incoming/odfi/scheduler.go`：分阶段编排、重启恢复、证据/暂存回收。
- 改 `internal/incoming/odfi/admin.go`：消费结果只读查询端点。
- 测试：新增 `consumption_test.go`（身份/隔离/原子性/恢复扫描）；在包内新增不依赖 Docker 的恢复与幂等测试（Mock Agent + Mock Emitter：重复 tick 不重发事件、不重复删除；处理器失败保远端与证据；删除超时→远端查询两分支）；更新 `cleanup_test.go`；同步更新 `internal/test/download_test.go` 的布局断言（短测试，CI 有 SFTP 服务时运行）。

## 依赖与注意事项

- 不新增第三方依赖（仅用标准库 `crypto/sha256、encoding/json、os、path/filepath`）；不新增配置项；不需要 Docker/数据库。
- 新 `.go` 文件保留 Apache 2.0 头；gofmt；导出标识符加 golint 注释。
- 处理器粒度确认（非逐条事件）：同一处理器 `Handle` 内部多消息（如 return 分块、recon 逐笔）崩溃窗口仍可能重复，这是现有 `FileProcessor` 接口下与需求表述（“处理器”）一致的粒度，不做接口重构。
- 审计 blob 同路径重写为覆盖写，重试幂等。

## 验证

- `go build ./...`、`go vet ./...`、`go test -short ./...`（重点 `internal/incoming/odfi`、`internal/test` 编译通过）。
- 新单测覆盖：稳定身份与同名隔离、下载中断只重下未确认文件、处理器确认后恢复不重发、失败保远端与证据、删除不确定时先查远端（存在/不存在/列举失败三分支）、重启只续未完成阶段、查询端点返回最终结果。
- 回归：空目录不建目录、空文件零字节清理、无效 ACH 与容忍 FileHeader、正常一次处理路径、告警触发次数与路径保持不变。

## 风险与应对

- 状态文件写盘频率（每阶段每文件一次小 JSON 重写）：文件量小、顺序写，可接受；采用原子 rename 防半写。
- 存量部署升级：旧布局无 `consumption/` 目录，首轮按新流程重新下载一次并在删除阶段通过远端查询自然收敛，不迁移旧临时目录。
- 磁盘积累：失败证据长期保留用于排障（需求要求），成功单元遵循 `CleanupLocalDirectory` 既有策略。
