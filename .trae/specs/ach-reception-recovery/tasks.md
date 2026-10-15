# ACH 接收链路可恢复提交与幂等重试 - 实施计划

所有任务的通用约束：
- 新 `.go` 文件保留 Apache 2.0 头（复制邻近文件）；导出标识符补 golint 注释；gofmt；不引入新第三方依赖（预期零新增）；不弱化任何既有校验或 golden 数据。
- 真实 MySQL/Spanner 测试保留 `testing.Short()` 守卫，不依赖 Docker；主体证据来自 t.TempDir + 内存状态机 + 故障注入测试。
- AC 与任务映射：AC-1→T3；AC-2→T2/T4；AC-3→T1/T4；AC-4→T1/T4；AC-5→T2/T4；AC-6→T5；AC-7→T6；AC-8→T3/T4/T5；AC-9→T4；AC-10→T4；AC-11→T7；AC-12 贯穿；AC-13→T6/T8；AC-14→T9 及评审。

## Task 1: 存储原子发布原语与分片暂存区
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - TR-1.1~1.4 通过：新增 `internal/storage/move_file_test.go`（源缺失不建空目标/父目录自动创建/替换已存在目标/非法路径）与 `internal/pipeline/staging_test.go`（暂存哈希一致、发布前 mergable 不可见、发布后暂存清空、ValidateOpts 同迁、预到达 `.canceled` 直接发布、CleanupStaging 隔离性、无暂存发布报错）；命令 `go test ./internal/storage/... ./internal/pipeline/... -short` 全部 ok。
  - 实现：`Chest.MoveFile`（filesystem 原子 rename + 源/目标父目录 fsync；encrypted 委托；MockStorage 注入位），`WriteFile` 增加父目录 fsync；`XferMerging` 新增 StageXfer/PublishXfer/CleanupStaging/ListStaged 与 `StagedCommit`；修复重构中 bytes.Buffer 复用导致 NACHA 字节被 JSON 覆盖的回归（独立缓冲）。
  - gofmt 无差异。
- **Description**:
  - 在 `internal/storage` 的 `Chest` 接口新增 `MoveFile(oldpath, newpath string) error`：同根原子 rename；自动创建目标父目录；源不存在返回错误（区别于现有 `ReplaceFile` 源缺失会写空文件的语义，后者供取消逻辑继续使用，不改动）；filesystem 实现对源/目标父目录执行 fsync；`encrypted` 包装与 `MockStorage`（新增 `MoveFileErr` 注入位）同步实现。
  - 在 `internal/pipeline/merging.go` 的 `XferMerging` 接口与 `filesystemMerging` 上新增暂存/发布原语（不动 `HandleXfer`/`HandleCancel` 既有方法签名）：
    - `StageXfer(ctx, xfer incoming.ACHFile, commitID string) (contentSHA256 string, err error)`：将 NACHA 与 ValidateOpts（若有）写入 `staging/<shardName>/<commitID>/<fileID>.ach[.json]`（沿用 writeACHFile 的 Writer/JSON 编码逻辑），返回 NACHA 字节 sha256；
    - `PublishXfer(ctx, xfer incoming.ACHFile, commitID string) error`：将暂存两份文件 `MoveFile` 到 `mergable/<shardName>/<fileID>.ach[.json]`；若目标 `.canceled` 意图标记已存在，则直接发布为 `.ach.canceled`（预到达取消，供 T4 编排使用）；
    - `CleanupStaging(ctx, commitID string) error`：删除 `staging/<shardName>/<commitID>/`；
    - `ListStaged(ctx) ([]StagedCommit, error)`：枚举 `staging/<shardName>/*`，返回 commitID、文件列表与哈希所需路径（恢复扫描用，T6 消费）。
  - `mock_xfer_merging.go` 同步补齐新接口方法。
- **Acceptance Criteria Addressed**: AC-3, AC-4
- **Test Requirements**:
  - `rule` TR-1.1: t.TempDir 下 StageXfer 后暂存 `.ach/.json` 存在且 mergable 不存在；PublishXfer 后暂存为空、mergable 文件内容 sha256 与输入一致；证据：`go test ./internal/pipeline/ -run 'Stage|Publish' -short -v` 输出。
  - `rule` TR-1.2: MoveFile 源缺失返回错误且不创建空目标；父目录自动创建；证据：`internal/storage` 新增表驱动用例输出。
  - `rule` TR-1.3: 目标存在预到达 `.canceled` 标记时 PublishXfer 直接产出 `.ach.canceled`，不产生可合并 `.ach`；证据：发布用例断言 Glob 结果。
  - `rule` TR-1.4: CleanupStaging 只删除对应 commitID 目录，不影响其他暂存与 mergable；证据：隔离性用例断言。

## Task 2: 接收台账模型、003 迁移与 Repository 状态机
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - TR-2.1~2.3 通过：`internal/files/reception_mock_test.go`（`go test ./internal/files/ -short` ok）覆盖五阶段单调推进、回跳拒绝（ErrInvalidStage）、幂等重复 RecordReception、1062 唯一冲突可被 database.UniqueViolation 识别、待通知/未完成/已发布集合语义、Cancel 仅对 published 生效、RequestCancellation 幂等、MarkFailed 终态与 ErrReceptionNotFound。
  - TR-2.4：新增 `migrations/003_receptions.up.mysql.sql`（files.published_at + file_receptions 表 + uniq(shard_key,file_id)）与 Spanner 等价 DDL（NULL 列 + UNIQUE INDEX）；assets.go 既有 embed 模式 `migrations/*.up.*.sql` 自动收录；扩展 TestRepository_Receptions（dbtest/-short 守卫）。
  - 实现：Reception 模型与阶段常量；Repository 接口扩展（SQL 事务化 MarkPublished、条件阶段 UPDATE、Spanner DML/读改写等价实现）；MockRepository 升级为线程安全内存状态机（guardStage 与 SQL 守卫同义）。
  - 全仓 `go test ./... -short`：25 个包全部 ok，无回归。
- **Description**:
  - 新增 `internal/files/model_reception.go`：`Reception`（CommitID、ShardName、ShardKey、FileID、Source、Hostname、Stage、AcceptedAt、StagedAt、PublishedAt、ContentSHA256、ResultJSON、NotifiedAt、LastError、UpdatedAt）与阶段常量 `StageReceived/Staged/Indexed/Published/Failed`；`AcceptedFile` 增加 `PublishedAt *time.Time`。
  - 新增 `migrations/003_receptions.up.mysql.sql`：`ALTER TABLE files ADD COLUMN published_at TIMESTAMP NULL`；创建 `file_receptions`（列见上，commit_id VARCHAR(128) PK、shard_key/file_id 复合唯一、result_json MEDIUMTEXT、各时间戳与 stage VARCHAR(20)）。`003_receptions.up.spanner.sql` 等价 DDL（STRING(MAX)/TIMESTAMP，PRIMARY KEY(commit_id)，file_receptions 的 shard_key+file_id 唯一用 Spanner 支持的写法：主键即 commit_id，另以 NULL_FILTERED 唯一索引或把 (shard_key,file_id) 纳入唯一索引——实现时以不依赖 Spanner 模拟器的静态评审为准）。
  - 扩展 `files.Repository`（SQL/Spanner/Mock 三处）：
    - `RecordReception(ctx, Reception) error`：幂等插入（MySQL `INSERT IGNORE`/`ON DUPLICATE KEY`；Spanner InsertOrUpdate 仅在不存在时落地，不覆盖阶段）；
    - `GetReception(ctx, commitID) (*Reception, error)`（无行返回包装 `sql.ErrNoRows` 语义的哨兵）；
    - `MarkStaged(ctx, commitID, hash string, at time.Time) error`、`MarkFileIndexed(ctx, fileID, at)`（files.published_at 仍 NULL）；
    - `MarkPublished(ctx, commitID, fileID string, resultJSON []byte, at time.Time) error`（同事务/同批次置两表发布标记）；
    - `MarkNotified(ctx, commitID string, at time.Time) error`；`MarkFailed(ctx, commitID, errMsg string, at time.Time) error`（不删除行）；
    - `ListPendingNotifications(ctx) ([]Reception, error)`（stage=published AND notified_at IS NULL）；`ListUnfinishedReceptions(ctx) ([]Reception, error)`（received/staged/indexed 与 published 未通知，恢复用）；
    - files 侧 `ListPublishedFileIDs(ctx) (map[string]struct{}, error)`（T7 门控）；`Cancel` 保持签名，仅当行存在且已发布时置 canceled_at（未发布行由 T4 意图路径处理，避免「取消成功但无文件」）；`Record` 保留供回填/兼容，内部以未发布状态插入。
  - `MockRepository` 升级为线程安全内存状态机（map + mutex），实现全部新方法与条件推进（拒绝阶段回跳），保留 `Err` 注入位并新增可按方法注入错误的钩子供故障测试使用。
  - 扩展 `internal/files/repo_files_test.go`（dbtest 守卫保持）覆盖新列与阶段推进；Mock 状态机新增不依赖 DB 的单测（默认 `-short` 执行）。
- **Acceptance Criteria Addressed**: AC-2, AC-5, AC-14
- **Test Requirements**:
  - `rule` TR-2.1: Mock 状态机走完 received→staged→indexed→published→notified，时间戳与 ResultJSON 可按 commitID 读回；任何回跳尝试返回错误且状态不变；证据：`go test ./internal/files/ -short -v`。
  - `rule` TR-2.2: RecordReception 对同一 commitID 重复调用幂等（不覆盖阶段/accepted_at）；同 (shardKey,fileID) 不同 commitID 第二次冲突返回唯一冲突错误（`database.UniqueViolation` 可识别，Mock 用同样哨兵包装）；证据：单测断言。
  - `rule` TR-2.3: ListPendingNotifications/ListUnfinishedReceptions/ListPublishedFileIDs 的集合语义正确（含空集）；证据：单测断言。
  - `rule` TR-2.4（守卫）: 两份 003 DDL 与既有 001/002 风格一致、列/类型/索引自洽，assets.go 无需改动即被 embed 收录；证据：人工静态评审记录（无 MySQL 环境时不做执行要求）。

## Task 3: 提交身份解析、事件字段与 HTTP/Kafka 贯通
- **Status**: `pending`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - `internal/incoming/models.go`：`ACHFile` 与 `CancelACHFile` 增加 `CommitID string \`json:"commitID,omitempty"\``；`QueueACHFileResponse`/`FileCancellationResponse` 增加同名字段（omitempty）。`pkg/models/events.go` 无需结构变更（类型别名继承），确认 JSON 往返保留字段。
  - 新增 `internal/pipeline/commit.go`：`resolveCommitID(msg *pubsub.Message, file incoming.ACHFile) string`——优先 `msg.Metadata["commitID"]`（HTTP 控制器写入；Kafka 侧读取 gocloud 传播的消息 key，实现时核对 kafkapubsub metadata 常量并在 kafka 包内规范化到 `commitID` 键），其次事件体字段，最后 `deriveCommitID(shardKey, fileID)`（sha256(`shardKey+"\x00"+fileID`) hex，零分配以外不做内容哈希）。
  - `internal/incoming/web/api_files.go`：读取 `Idempotency-Key` 请求头（空则留空交由下游派生）；把 commitID 写入 `incoming.ACHFile.CommitID` 与 pubsub Metadata；等待器 map 改为 `map[string][]chan ...` 按 commitID 聚合并 fan-out（同 key 并发/重试请求都收到同一份响应），删除等待器时按通道实例精确删除；取消等待器同样按 commitID 聚合。
  - Kafka 入站：`internal/kafka` 订阅侧把记录 key 规范化进 metadata（若 gocloud 已提供则仅加注释与测试，不重复包装）。
  - 响应通道载荷携带 commitID（`QueueACHFileResponse.CommitID`），使接收侧与控制器能匹配重试。
- **Acceptance Criteria Addressed**: AC-1, AC-8, AC-14
- **Test Requirements**:
  - `rule` TR-3.1: 表驱动单测覆盖身份来源优先级（metadata > 事件字段 > 派生）、派生确定性与区分性（同输入相同、不同 shardKey/fileID 不同）；证据：`go test ./internal/pipeline/ -run Commit -short -v`。
  - `rule` TR-3.2: HTTP 两个相同 `Idempotency-Key` 并发 POST 最终收到逐字段一致的同一份响应（仅一次写盘，T4 完成后端到端断言；本任务先断言等待器 fan-out 行为）；证据：web 包用例。
  - `rule` TR-3.3: 不含新字段/头的旧事件与旧请求可正常解析，序列化 JSON 中 commitID 缺省省略；证据：JSON 往返与既有 web 测试输出。

## Task 4: 接收状态机编排、Ack 门控与取消适配
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 3
- **Completion Evidence**:
  - TR-4.1~4.5 通过：新增 `internal/pipeline/reception_flow_test.go`（`-short` 可运行）——HappyPath 五阶段+冻结结果+发布后 Ack；暂存失败清理且不 Ack、重试后单文件；索引失败停留 staged 且重试零重写；同 commitID 重试返回时刻一致的冻结结果且写/移各仅一次；取消先至落 `.ach.canceled` 且发布直接取消。`internal/test/cancel_test.go` 既有 e2e 零改动通过；`TestAggregateACHFile*` 改用 stage/publish/buildQueueResponse。
  - TR-4.6：InvalidQueueFile 用例（含 producer 失败）保持通过；新增 retriable 分类用例证明索引失败不发 InvalidQueueFile、ErrRetriable 不 Ack。
  - TR-4.4/Ack：新增 `ackMessage` 接缝断言 Ack 严格晚于发布、失败零 Ack、重试每次在 published 门后 Ack；AutoCommit 路径行为不变。
  - 压力佐证：`TestUploads`（需 Docker 内建 FTP，本环境不可用，-short 跳过）前置阶段成功——500 并发提交与 34 个竞速取消全部发布到 mergable，仅在外部 FTP 连接处失败（环境前置，非本次缺陷）。
  - 实现要点：processACHFile 重写为 received→staged→indexed→published 编排，终态/失败返回冻结结果；MarkPublished 同事务置两表；取消流程 RecordReception+RequestCancellation（预到达意图）+仅 published 落 canceled_at+HandleCancel；`go vet`/gofmt 干净，`go test ./... -short` 25 包全绿。
- **Description**:
  - 重写 `internal/pipeline/file_receiver.go` 的文件接收路径（保持 `processMessage` 的事件分派与 InvalidQueueFile 行为边界）：
    1. Reveal/Read/Validate/getAggregator 行为不变（解析与校验失败仍只发 InvalidQueueFile，不建台账、不 Ack 的现状保持——注意：readErr 现状返回错误，维持）；
    2. resolveCommitID；`RecordReception(received)`（唯一冲突且已有冻结结果/终态时直接进入「返回原结果」分支；中间态进入续作分支）；
    3. 台账未 staged：`agg.stageFile`（merger.StageXfer），失败→`CleanupStaging`→返回错误（不 Ack）；成功→`MarkStaged`；
    4. files 未 indexed：写 files 行（published_at NULL），失败保留 staged 返回错误（不 Ack）；成功→台账 indexed；
    5. `PublishXfer`（含预到达取消判定），成功后同批 `MarkPublished`+冻结 ResultJSON（acceptedAt 取台账首收时间，nextCutoff 以该时间确定性计算）；发布失败按现场可重试性返回错误（不 Ack），不删除暂存；
    6. 将冻结结果投入 `QueueFileResponses`（HTTP）与 outbox 待发表（T5）；**此后**且仅此后在手动提交模式下 `msg.Ack()`；
    7. 失败分类：暂存/索引/发布基础设施错误→可重试（消息不 Ack）；格式校验错误→InvalidQueueFile（维持现状）。
  - 删除现有「Record 在前、acceptFile 失败后 Cleanup 删除 files 行」的旧顺序；`acceptFile` 拆为 stage/publish 两段由 receiver 编排（aggregator 保留 nextCutoff 计算辅助）。
  - `cancelACHFile` 适配：已发布（repo 可查 published）→`repo.Cancel`+`HandleCancel` 改名；未发布→仅落地 `.canceled` 意图标记（merger 新增 `RecordCancelIntent`，复用 ReplaceFile 现状语义）与台账取消意图（不报错、successful 语义与现状一致），后续 PublishXfer 直接落 `.canceled`；重复取消幂等。
  - `processMessage` 中 Ack 决策收敛为单一门控函数 `shouldAck(stage)`；AutoCommit=true 路径保持收到即 Ack 不变。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4, AC-5, AC-8, AC-9, AC-10, AC-11(部分)
- **Test Requirements**:
  - `rule` TR-4.1: 成功路径断言台账五阶段、暂存已清空、mergable 单文件哈希一致、files 行 published_at 非空；证据：pipeline 用例输出。
  - `rule` TR-4.2: 注入暂存失败：staging 被清理、无 files 行、无 Ack；故障清除后同消息重投成功且仅一份文件；证据：故障注入用例。
  - `rule` TR-4.3: 注入 files 索引失败：staged 保留、无 published_at、不 Ack；恢复后重试不重写暂存（StageXfer 写计数=0，复用已暂存内容）并成功发布；证据：故障注入用例。
  - `rule` TR-4.4: 相同 commitID 重试返回与首次逐字段一致的冻结结果（acceptedAt/nextCutoff/error 一致），storage 总写计数=1（仅首次），消息 Ack 次数=1（晚于 published 观测点）；证据：幂等与时序用例（包装订阅记录 Ack 时刻）。
  - `rule` TR-4.5: 取消三情形（已发布取消、staged 中取消、取消先于提交）行为满足 AC-10，且 `internal/test/cancel_test.go` 不改断言语义通过；证据：相关测试输出。
  - `rule` TR-4.6: InvalidQueueFile 用例（含 producer 失败）与 shouldAutocommit 等既有 file_receiver 测试全部通过；证据：既有测试输出。

## Task 5: 结果冻结与通知 outbox
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2, Task 4
- **Completion Evidence**:
  - TR-5.1 通过：`outbox_test.go::TestOutbox_FailingEmitterRetriesWithoutRewriting`——emitter 故障期间 pump 返回 0、notified_at 保持 NULL、storage 写/移计数零变化；恢复后同一冻结结果（commitID 一致）发送一次并置 notified，再次 sweep 为空。
  - TR-5.2 通过：`TestOutbox_FrozenResultRoundTrip` 冻结 JSON 与结构体逐字段一致（时刻用 .Equal）；frozenQueueResponse 在 ResultJSON 缺失时以台账 AcceptedAt 重建并再冻结。
  - TR-5.4 通过：`pkg/models` 新增 QueueACHFileResponse 的 Read 分派与内部往返测试（含 commitID）。
  - 实现：`outbox.go`（runOutbox 启动即扫一次 + 2s ticker，Shutdown 可停；pumpOutbox 单条失败只留行重试、绝不回写文件；空 ResultJSON 跳过交恢复处理；新增 receptionResultsSent/receptionResultFailures 指标）；published/failed 终态均纳入待通知集合（MySQL/Spanner/Mock 同步）；HTTP 通道仍由 T4 内联非阻塞投递。
  - `go vet` 干净，`go test ./... -short` 25 包全绿。
- **Description**:
  - 发布点冻结 `incoming.QueueACHFileResponse`（含 CommitID）为 ResultJSON；提供从 `Reception` 无损重建响应的辅助（崩溃后恢复用）。
  - FileReceiver 新增 outbox 循环（可配置间隔，默认秒级，测试可立即触发）：`ListPendingNotifications` → `eventEmitter.Send(QueueACHFileResponse 事件)` → 成功 `MarkNotified`；单条失败只记录日志/指标并保留该行，绝不回调 stage/publish；关停时随 Shutdown 退出。Emitter 为 Mock/未配置时正常置 notified（测试与无事件配置部署）。
  - HTTP 通道：发布后向 `QueueFileResponses` 发送同一份冻结结果（控制器 T3 fan-out）；通道阻塞不得阻塞发布与 Ack（select default/缓冲策略，超时则留给客户端重试取结果）。
  - `pkg/models` 注册 `QueueACHFileResponse` 事件类型的 Read 分派（当前 switch 未含该类型——补上以支持事件往返与 MockEmitter 记录）。
- **Acceptance Criteria Addressed**: AC-6, AC-8
- **Test Requirements**:
  - `rule` TR-5.1: emitter 持续失败期间文件保持 published、storage 写计数为 0、无二次合并计入；恢复后 outbox 补发且事件仅含同一 commitID/结果，成功后 notified_at 置位、不再重复发送；证据：failingEmitter 注入用例。
  - `rule` TR-5.2: 结果可仅由台账行重建且与冻结 JSON 逐字段一致（acceptedAt/nextCutoff/error/commitID）；证据：重建单测。
  - `rule` TR-5.3: HTTP 首次同步等待与「超时后同 commitID 重试」均拿到同一份结果；证据：web+pipeline 联用用例。
  - `rule` TR-5.4: QueueACHFileResponse 经 models.Read 可往返，MockEmitter.Sent() 中类型正确；证据：events/models 用例。

## Task 6: 进程重启恢复扫描（已发布/待通知/无法判定）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 4, Task 5
- **Completion Evidence**:
  - TR-6.1/6.2 六场景全部通过（`internal/pipeline/recovery_test.go`，-short 可跑）：①已发布待通知 reconcile 幂等、不复制文件、outbox 补发；②staged 有暂存无索引→补索引+原子发布+冻结结果+通知；③rename 已发生（mergable 哈希一致）→仅补标记零二次移动；④索引称已发布但暂存/mergable 双缺→MarkFailedWithResult、冻结失败结果、alerter 告警、不产生幻影文件、失败结果进 outbox；⑤legacy mergable（files 行 published_at NULL、无台账）→回填 published_at+published 台账且保留原 acceptedAt；⑥received 无暂存→保留等待重投不告警。
  - TR-6.3 崩溃窗口矩阵（阶段×边界）覆盖：staging 写后/索引写后/publish 前后（见 recovery.go reconcileReception/resumeToPublished 分支），每个边界都有自动恢复或失败收尾；恢复于消费者启动前、outbox 首次扫描前执行。
  - 实现：新增 recovery.go（三方核对 ListUnfinishedReceptions×ListStaged×mergable Glob，按哈希判定；RecordPublishedReception/MarkFailedWithResult 三后端齐备；receptions_recovered/recoveries_failed 指标；缺失 mergable 目录按空处理；无索引孤儿仅告警不移动）。
  - `go vet` 干净，`go test ./... -short` 25 包全绿。
- **Description**:
  - 新增 `internal/pipeline/recovery.go`，在 `newFileReceiver` 成功后、消息循环 `Start` 开始前对每个 shardAggregator 同步执行一次 `reconcileShard(ctx, shardName, merger, repo)`：
    - 取 `ListUnfinishedReceptions`（按 shard 过滤）+ `ListStaged` + mergable 磁盘实况做三方核对（哈希以台账 content_sha256/暂存文件实算为准）；
    - published + 文件在位 + 未通知：交 outbox 补发；
    - published 标记但 mergable 缺失：暂存在位且哈希一致→重新 MoveFile 发布；暂存缺失→`MarkFailed`（冻结错误结果 "published file missing and unrecoverable"）、alerters 告警、outbox 投递失败结果；
    - staged/indexed：暂存在位→补索引（若缺）+PublishXfer+MarkPublished（rename 已发生的子情形先核对 mergable 哈希，一致则只补标记）；暂存缺失且未发布→MarkFailed+告警；
    - received 无暂存：保留行等待消息重投（不告警）；
    - mergable 中存在但台账/索引均无记录的 `.ach`：legacy 回填（files 行置 published + 台账 published），无法解析 fileID 的异常文件仅告警不移动。
  - 恢复期间的告警走 aggregator.alerters；结构化日志带 commitID/fileID/shard/reason；新增 recovery 结果计数 metric（published-recovered/notified-replayed/failed/legacy-backfilled）。
- **Acceptance Criteria Addressed**: AC-7, AC-13
- **Test Requirements**:
  - `rule` TR-6.1: 四种现场（待通知补发 / 暂存续作发布 / rename 后补标记 / 不可判定 failed+告警）恢复后终态与 AC-7 完全一致，且每个现场 mergable 文件数不增加；证据：`go test ./internal/pipeline/ -run Recover -short -v`。
  - `rule` TR-6.2: legacy mergable 文件回填后可被取消与合并门控识别；received 残留不被告警、不被失败化；证据：回填/等待用例。
  - `rubric` TR-6.3: 崩溃窗口覆盖完备度；scale 1-5；anchors 1=存在丢文件/重复合并窗口，3=主要窗口覆盖但有一处人工判断，5=五阶段边界崩溃全部自动分类恢复或 failed+告警；threshold >= 4；证据：阶段×崩溃矩阵对照表（在测试注释/Completion Evidence 中列出）与用例集。

## Task 7: 截止合并的已发布门控
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2, Task 4
- **Completion Evidence**:
  - TR-7.1 通过：`cutoff_gate_test.go::TestCutoffGate_MergesOnlyPublished`——mergable 中混有已发布 A、未发布 B、已取消 C；cutoff 后仅 A 进入转换（transforms>0），B 被原子 MoveFile 回 `mergable/testing/` 存活到下一轮，C 按既有 canceled 逻辑排除；索引状态不被门控改变。
  - TR-7.2 通过：publishedFileIDs 返回错误时 WithEachMerged 在合并/上传前 fail-closed 返回 "cutoff fails closed"，transforms=0（零文件移动到上传）。
  - TR-7.3：provider 为 nil 的直接构造路径保持「全部合格」，既有 merging 全套测试（mapping、canceled 排除、空/单/多文件、ValidateOpts）零改动通过。
  - 实现：filesystemMerging.publishedFileIDs 回调；FileReceiver.installPublishedGate 注入 repo.ListPublishedFileIDs；未发布文件在隔离目录中回移 live mergable（绝不 RmdirAll 删除）；新增 cutoff_excluded_unpublished_files 指标。
  - `go vet`/gofmt 干净，`go test ./... -short` 25 包全绿。
- **Description**:
  - 为 `filesystemMerging` 注入已发布集合提供方：`aggregator`/`WithEachMerged` 在列目录后通过回调 `publishedFileIDs() (map[string]struct{}, bool)`（由 FileReceiver 用 fileRepository.ListPublishedFileIDs 提供；Mock 状态机返回真实集合；查询失败时 fail-closed：记录错误并使本次 cutoff 失败，不让状态不明文件参与资金移动）。
  - `getNonCanceledMatches`/`buildDirMapping` 在现有「排除 .canceled」基础上再排除无 published 记录的 fileID（未发布/恢复未决）；被排除文件不离开被隔离目录（恢复逻辑下次仍可见）。
  - 核对空目录/纯未发布目录的既有行为：无已发布输入时与今天「空成功清理」语义保持一致或明确保留目录（实现时以「不销毁未发布文件」为准——隔离目录 rename 已发生时不得 RmdirAll 删除其中未发布文件）。
  - 注入点选择保持 XferMerging 接口最小改动（新增一个可选设置方法或 WithEachMerged 参数封装，避免破坏大量既有测试）。
- **Acceptance Criteria Addressed**: AC-10(合并侧), AC-11, AC-12
- **Test Requirements**:
  - `rule` TR-7.1: 同目录混合已发布/已取消/未发布输入时，仅已发布未取消文件进入合并并收到 FileUploaded；未发布文件在隔离目录保留、可被恢复；证据：门控用例。
  - `rule` TR-7.2: 已发布集合查询失败时 cutoff 返回错误且不上传任何文件；证据：fail-closed 用例。
  - `rule` TR-7.3: 既有 merging 全部测试（merging_test、mapping、with_each_merged、unit、manual cutoff、upload/download 集成在 -short 下可跑部分）零改动通过；证据：`go test ./internal/pipeline/... ./internal/test/... -short`。

## Task 8: 端到端故障矩阵、回归与静态检查
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4, Task 5, Task 6, Task 7
- **Completion Evidence**:
  - TR-8.2：`go build ./...`、`go vet ./...`、gofmt -l 全部干净（0 输出）；`go test ./... -short` 25 包全绿；非 short 下 pipeline（含全部 merging 既有套件、events API、staging/recovery/gate/outbox/reception 新套件）、web、storage、pkg/* 全绿。
  - 外部依赖类集成测试在本环境（无 Docker）按预期失败且与改动无关：files TestRepository*（MySQL 1045 access denied）、stream TestStreamErrors（Kafka :19092 refused）、test TestUploads（FTP :2121 refused；其 500 并发接收/34 竞速取消的发布阶段已全部通过新状态机完成）。
  - TR-8.3 故障矩阵（四类×首试/重试/恢复，均为可执行断言）：①暂存 WriteFile 失败→清理+received+不 Ack+重试单文件（TestReception_StageFailure）；②索引 Record 失败→staged 保留+零重写重试发布（TestReception_IndexFailure/RetriableIndexFailure）；③通知 emitter 失败→不写文件+pending+恢复后同 commitID 单事件（TestOutbox_FailingEmitter）；④发布崩溃四现场+legacy 回填（recovery_test 六场景）；⑤门控混入已发布/未发布/已取消（TestCutoffGate_MergesOnlyPublished）+fail-closed。
  - 新增 e2e：`internal/test/idempotent_test.go::TestIdempotentHTTPRetryReturnsSameResult`——真实 HTTP 控制器+消费者全栈：同 Idempotency-Key 两次 POST 返回一致 acceptedAt/nextCutoff、mergable 仅一份文件、SD-live shard 正确路由、取消仍 200。
  - 附带修复：发布后移除空暂存目录树（CleanupStaging + Chest.RemoveEmptyDir 三实现），消除 staging/ 根残留；TestEventsAPI_FileUploaded 改为等待「文件在位且已发布标记」后再触发 cutoff（避免发布前微秒窗口的固有竞态，该窗口下门控本就正确回移、由下一 cutoff 处理）。
- **Description**:
  - 在 `internal/pipeline` 与 `internal/test` 补齐端到端矩阵：HTTP 成功→取消→手动 cutoff；Kafka 风格重投（mem pubsub 重复 Send 同 commitID）；暂存失败/索引失败/通知失败/发布失败四类注入与恢复；重启模拟（不重进程，直接对新 FileReceiver/同一 storage+repo 执行 reconcile）；多 shard 路由各一份。
  - 执行 `gofmt -l .`（无输出）、`go vet ./...`、`go build ./...`、`go test ./... -short`；如 Makefile 的 check 可在无 Docker 下运行则执行其 lint 部分（不提交 lint-project.sh）。
  - 核对 `internal/test/upload_test.go`、`cancel_test.go`、`download_test.go` 在 -short 下行为。
- **Acceptance Criteria Addressed**: AC-7, AC-8, AC-10, AC-11, AC-12, AC-13
- **Test Requirements**:
  - `rule` TR-8.1: `go build ./...` 与 `go vet ./...` 退出码 0；gofmt 无差异；证据：命令输出。
  - `rule` TR-8.2: `go test ./... -short` 全绿，无新增 skip 的既有用例；证据：汇总输出（包名+ok/fail 列表）。
  - `rule` TR-8.3: 故障矩阵四类×（首试/重试/恢复后）断言全部通过；证据：矩阵用例输出。
  - `rubric` TR-8.4: 测试设计可信度（真实 t.TempDir 文件系统、真实状态机、故障注入而非 mock 一切）；scale 1-5；anchors 1=仅断言 happy path，3=关键路径有注入但重启只做口头描述，5=四类故障+重启恢复均为可执行断言；threshold >= 4；证据：用例清单与评审。

## Task 9: OpenAPI 与提交文档契约更新
- **Status**: `completed`
- **Priority**: `medium`
- **Depends On**: Task 3, Task 4, Task 5
- **Completion Evidence**:
  - openapi.yaml：POST/DELETE `/shards/{shardKey}/files/{fileID}` 新增可选 `Idempotency-Key` header（含语义说明与示例）；QueueACHFileResponse schema 新增可选 `commitID`；YAML 通过 yaml.safe_load 校验。
  - docs/concepts/submission.md：新增「Idempotent submissions and retries」（身份来源、received→staged→indexed→published 四阶段、Ack 时机、重启续作、outbox、cutoff published 门控）；补充预到达取消语义与 QueueACHFileResponse 指引。
  - docs/concepts/events.md：新增 QueueACHFileResponse 事件章节（冻结结果、acceptedAt 不变、失败结果也投递、at-least-once 与 commitID 去重建议）。
- **Description**:
  - `openapi.yaml`：Files.create 增加可选请求头参数 `Idempotency-Key` 说明；响应 schema（如已内联定义则补字段）增加可选 `commitID`；补充「同 key 重试返回同一结果」语义说明。不手改生成客户端（仓库内无 client/ 树）。
  - `docs/concepts/submission.md`：最小更新——Idempotency-Key 用法、提交阶段与重试语义、取消只对已发布生效（保留预到达承诺）、Kafka 成功结果事件说明；`docs/concepts/events.md` 补 QueueACHFileResponse 事件（若该文档列举事件类型）。
- **Acceptance Criteria Addressed**: AC-12, AC-14
- **Test Requirements**:
  - `rule` TR-9.1: openapi.yaml 为合法 YAML 且新增参数/字段均为可选；证据：解析检查与 diff 审阅。
  - `rubric` TR-9.2: 契约兼容性表达质量；scale 1-5；anchors 1=新字段像必填破坏性变更，3=可用但语义描述含糊，5=可选字段、重试/幂等/阶段语义一次讲清且与实现一致；threshold >= 4；证据：文档 diff 与评审。
