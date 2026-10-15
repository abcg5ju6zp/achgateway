# ACH 接收链路可恢复提交与幂等重试 - 产品需求文档

## Overview
- **Summary**: 为 ACHGateway 的文件接收链路（HTTP `POST /shards/{shardKey}/files/{fileID}` 与 Kafka/stream `QueueACHFile` 事件）引入稳定提交身份（commit identity）与可恢复的分阶段提交流程：消息读取 → 分片解析 → 临时文件耐久化 → 文件索引 → 发布（可查询、可取消）→ 接收结果通知。上游消息只在文件发布完成后确认；进程在任意阶段崩溃后，重启可区分「已发布待通知」「已暂存待发布」「无法判定」残留并继续原提交；相同身份的重试只返回原始结果，不重复写盘、不重复计入合并。
- **Purpose**: 消除当前「先写 files 索引行、再直接写 mergable 目录、结果走内存通道、消息 Ack 与发布状态无关」造成的崩溃窗口：重试后同一文件被重复处理、HTTP 成功后无法取消、索引存在但磁盘内容缺失、结果事件丢失且无法重放。
- **Target Users**: 通过 HTTP 或 Kafka 向 ACHGateway 提交 ACH 文件的上游业务系统；负责网关运维与故障恢复的运营人员。

## Goals
- 一次接收具备稳定、可跨进程携带的提交身份，HTTP 与 Kafka 重试收敛到同一提交。
- 接收过程按单调阶段推进，每个阶段的事实（含接收结果）持久化、可断点续作。
- 文件先在暂存区耐久化，再以同盘原子操作发布到 `mergable`；发布前对外既不可查询也不可取消/合并，发布后二者立即可用。
- 失败语义明确：落盘失败清理未发布内容；索引写入失败保留可重试的接收事实与暂存内容；通知失败只重发结果，不重写文件、不重复计入。
- 进程重启后自动分类残留并继续原提交；不可恢复的提交以失败结果收尾并告警，不静默丢文件。
- 取消与截止合并只作用于已发布文件，同时保留「取消先于提交到达」的既有语义。
- 正常单次提交、多分片路由、Nacha/JSON 现有格式校验行为保持不变。

## Non-Goals
- 不改变 ACH/Nacha 格式校验规则、合并条件（Conditions/MergeDir）、上传（upload agent）与审计（audit trail）逻辑。
- 不改变截止窗口（cutoff windows）调度、手动触发 cutoff 与 FileUploaded 事件的既有语义。
- 不引入跨网关实例的分布式锁；幂等以数据库唯一约束与暂存目录名为并发仲裁点（沿用现有「先 INSERT 者胜出」模型）。
- 不改造 ODFI 下载/退回/更正链路，不改造 blob 存储（blob 目前仅有 TODO，生产路径为本地/加密文件系统）。
- 不改变 Kafka `AutoCommit=true`（文档化的 at-most-once 模式）的既有行为；新保证针对默认手动提交模式。
- 不要求运行 Docker 或外部数据库即可运行单元测试；MySQL/Spanner 的真实驱动测试沿用现有 `-short` 守卫。

## Background & Context
- 当前链路（`internal/pipeline/file_receiver.go`）：`processMessage` 解密/解析事件 → `processACHFile` 先 `fileRepository.Record`（`files` 表，file_id 主键，唯一冲突即认为「已处理」直接跳过）→ `aggregator.acceptFile` → `filesystemMerging.HandleXfer` 直接把 NACHA 写入最终路径 `mergable/<shard>/<fileID>.ach` → 成功响应只写入内存 channel `QueueFileResponses`（HTTP 控制器按 fileID 等待，10s 超时）→ 手动提交模式下最后 `msg.Ack()`。
- 崩溃窗口：① 索引行已写、磁盘未写或写一半（`write()` 直接 O_TRUNC 最终路径，无暂存）→ 索引存在但磁盘缺失/损坏；② 磁盘已写、响应在途或 Ack 前崩溃 → Kafka 重投命中唯一冲突后静默跳过，不再产生结果（Kafka 成功提交本来就没有结果事件），HTTP 重试永远超时；③ acceptFile 返回普通错误时删除 files 行允许重试，但直接写最终路径意味着失败可能已留下部分文件；④ 截止时 `isolateMergableDir` 把整个目录改名搬走，此后取消必然失败。
- 取消（`HandleCancel`）：探测 `mergable/<shard>/<id>.ach`，`ReplaceFile` 改名为 `.ach.canceled`；文件不存在时写入空的 `.canceled` 标记，借此支持文档承诺的「取消先于 QueueACHFile 到达仍然生效」；`repo.Cancel` 仅 UPDATE canceled_at（行不存在时 0 行无错误）。
- 事件：`models.Read` 按 `type` 分派 `QueueACHFile`/`CancelACHFile`；成功结果类型 `QueueACHFileResponse` 目前只在内存 channel 中流动，从不经过 `events.Emitter`；失败经 `InvalidQueueFile` 事件发出。
- 持久化：迁移内嵌于 `assets.go`，现有 `002_files` 同时提供 MySQL 与 Spanner DDL；无 DB 时使用 `files.MockRepository`（当前不记录任何状态）。
- 已确认决策（2026-10-14）：
  1. 提交身份：客户端可选提供（HTTP `Idempotency-Key` 头 / Kafka 消息 key / 事件字段），缺省由网关按 `(shardKey, fileID)` 确定性派生；
  2. 持久化：新增 003 迁移与接收台账表，files 表补充发布状态；
  3. 残留恢复：自动恢复；暂存内容不可恢复时以失败结果收尾并告警；
  4. 结果通知：成功/失败结果都经 outbox 至少一次重投（HTTP 仍用内存等待器返回同一份结果）。

## Functional Requirements

- **FR-1（稳定提交身份）**：每次文件接收都携带一个 commitID。来源优先级：HTTP 请求的 `Idempotency-Key` 头；Kafka 记录 key / pubsub metadata；事件体内可选的 commitID 字段；均缺失时由 `(shardKey, fileID)` 确定性派生（与输入字节序无关、两次相同输入结果相同）。commitID 贯穿台账、暂存目录、结果事件与日志。
- **FR-2（接收台账）**：新增接收台账（migration 003，MySQL + Spanner 各一份），以 commitID 为主键记录：shard 解析结果、fileID、来源（http/stream）、hostname、阶段、accepted_at（首次接收时刻，重试不变）、staged_at、published_at、内容哈希、结果 JSON（QueueACHFileResponse 序列化，含 error/nextCutoff/acceptedAt）、notified_at、错误信息、updated_at；`(shard_key, file_id)` 唯一。无 DB 部署使用功能等价的内存状态机 Mock，且默认（无 DB）测试路径行为与有 DB 一致。
- **FR-3（阶段单调推进）**：接收按以下阶段推进，阶段只允许单调向前（失败除外置为 failed）：
  1. `received`：分片解析成功后，先耐久化接收事实（幂等 upsert/INSERT IGNORE）；
  2. `staged`：NACHA 与 ValidateOpts 写入暂存目录 `staging/<shardName>/<commitID>/` 并 fsync（文件与目录），记录内容哈希；
  3. `indexed`：files 索引行以「未发布」状态写入；
  4. `published`：暂存内容以同盘原子 rename 发布到 `mergable/<shardName>/<fileID>.ach[.json]`，随后置索引 published_at、台账 published_at 并冻结结果 JSON；
  5. `notified`：接收结果经 outbox 投递成功（events stream）并置 notified_at。
- **FR-4（上游确认时机）**：手动提交模式下，仅当文件到达 `published`（在 mergable 中可查询、可取消，且结果已冻结）后才 Ack 上游消息；通知阶段位于 Ack 之后，失败由 outbox 补偿。`AutoCommit=true` 维持现状。
- **FR-5（失败清理与可重试事实）**：
  - 暂存写失败：清理 `staging/<shardName>/<commitID>/` 全部未发布内容，mergable 中不得出现该文件，不写索引发布标记；消息不 Ack，重试从 `received` 重新暂存。
  - 索引写失败：台账停在 `staged`、暂存内容保留；重试只补索引与发布，不重写业务文件、不产生第二份内容。
  - 通知失败：台账保持 `published`、notified_at 为空；重试只重发冻结的结果，绝不重新暂存/发布/计数文件。
- **FR-6（幂等重试返回原结果）**：相同 commitID 的 HTTP/Kafka 重试：若台账已有冻结结果，直接返回/重放该结果（error、acceptedAt、nextCutoff 与首次一致），不再执行写盘或合并计入；若处于可恢复中间阶段，续作到发布后返回同一结果；若为 failed，返回冻结的失败结果。并发的相同提交由数据库唯一约束仲裁，仅一个胜出者执行写盘。
- **FR-7（结果 outbox）**：发布时冻结 QueueACHFileResponse（acceptedAt 取台账首次接收时间，nextCutoff 由分片 cutoffs 配置与该时间确定性计算，可在崩溃后无损重建）。后台循环与启动恢复扫描 `published 且 notified_at IS NULL` 的行，经 `events.Emitter` 至少一次发送 `QueueACHFileResponse` 事件（携带 commitID），成功后标记 notified；HTTP 请求另由内存等待器获得同一份冻结结果，等待器按 commitID 聚合并支持重复请求 fan-out。
- **FR-8（启动恢复分类）**：FileReceiver 在开始消费消息前对每个分片执行一次恢复扫描：
  - **已发布待通知**：台账 published、mergable 文件在位、notified_at 为空 → 补发结果；
  - **已暂存/已索引待发布**：暂存内容在位（哈希可核对）、未到 published → 补索引并原子发布；rename 已发生但发布标记未写（mergable 在位）→ 核对哈希后补记发布并冻结结果；
  - **无法判定**：索引称已发布但 mergable 缺失且暂存不可用 → 置 failed、写冻结错误结果、触发告警，不凭空重建、不参与合并；
  - 升级前遗留的 mergable 文件（无台账行）回填为已发布索引，保证旧部署平滑升级。
- **FR-9（取消门控）**：取消只对已发布（台账 published + mergable 在位）的文件执行改名 `.canceled` 与 canceled_at；对未发布文件的取消不造成 500、不删除暂存，而是耐久记录取消意图（沿用 `.canceled` 标记机制）：若文件随后发布，发布时直接落为 `.canceled`（合并跳过）；若发布前进程重启，意图与暂存一同被恢复。取消接口对同文件的重复调用保持幂等（第二次仍成功），既有 pre-arrival cancel 行为不变。
- **FR-10（截止合并门控）**：`WithEachMerged` 只合并「已发布且未取消」的输入；恢复未完成、未发布或判定失败的文件不得进入合并。该门控不得改变现有正常合并结果（空目录、多文件、取消排除、ValidateOpts 等既有测试行为不变）。
- **FR-11（校验与路由不变）**：ACH Nacha/JSON 解析与校验失败仍产生 `InvalidQueueFile` 事件、行为与现状一致；fileID/shardKey 缺失、无 aggregator 等错误路径维持现状；不同 shardKey 的分片路由、默认分片回退逻辑不变。
- **FR-12（契约兼容）**：新增字段均为可选且 `omitempty`（事件体 commitID、QueueACHFileResponse.commitID）；HTTP 新增可选 `Idempotency-Key` 请求头并在 OpenAPI 描述；无该头的旧客户端行为不变；不手改任何生成代码。

## Non-Functional Requirements
- **NFR-1（可测试性）**：全部新行为可用 `t.TempDir()` 真实文件系统 + 内存台账 Mock + 故障注入（写失败/索引失败/emitter 失败）完成单元与集成测试，`go test ./... -short` 全绿；真实 MySQL/Spanner 测试保留 `-short` 跳过守卫，不依赖 Docker。
- **NFR-2（耐久化正确性）**：暂存写与原子发布后文件内容与上游字节一致（发布前后 sha256 相等）；关键目录变更 fsync；原子性基于同一存储根内的 rename。
- **NFR-3（可观测性）**：阶段推进、恢复分类计数、补发通知计数有结构化日志字段（commitID、fileID、shard、stage），复用现有 log/telemetry/metrics 风格；不可恢复残留必须告警（复用 alerters）。
- **NFR-4（风格与规模）**：遵循 Moov 组织规范与邻近代码风格（Apache 头、golint 注释、go-kit 风格日志、无新依赖的必要性论证）；改动保持小而完整，不做顺带重构；gofmt/vet/lint 通过。
- **NFR-5（向后兼容）**：旧数据（仅有 files 行/mergable 文件的部署）升级后可运行；旧消息（无 commitID 字段）可消费；旧 HTTP 客户端无需改造。

## Constraints
- **Technical**: Go 模块 `github.com/moov-io/achgateway`，沿用 Go 1.24 与现有依赖（gocloud.dev/pubsub、sarama、moov-io/base、moov-io/ach）；新增第三方依赖须有明确必要（本改动预期零新增依赖）。数据库迁移需同时提供 MySQL 与 Spanner DDL，并由 `assets.go` 现有 embed 自动收录。
- **Business**: 金融文件不得静默丢失、不得重复计入合并；宁可失败收尾+告警，也不得让状态不明的文件参与资金移动。
- **Dependencies**: `moov-io/ach` 的 MergeDir/Writer 行为不变；存储 Chest 接口扩展需同时覆盖 filesystem、encrypted、Mock 三处实现；`files.Repository` 接口扩展需同时覆盖 SQL、Spanner、Mock。

## Assumptions
- 生产存储为同一机器/同一挂载点上的文件系统（或加密包装），暂存目录与 mergable 同根，rename 具备原子性；blob 存储本就未实现，不在本次范围。
- 单实例内「台账 upsert + 暂存目录名（含 commitID）」足以仲裁并发重复提交；多实例下由数据库唯一约束保证只有一个胜出者，落败者读取冻结结果。
- Kafka `AutoCommit=false`（默认）时 gocloud/sarama 的手动 Ack 可在处理完成后提交偏移量；启用 AutoCommit 的部署明确接受 at-most-once，不享受新保证。
- nextCutoff 是分片 cutoffs 配置与 acceptedAt 的纯函数（`schedule.NextCutoff`），崩溃后重建结果与首次一致；配置在两次尝试之间未变更。
- 现有 HTTP 10s 同步等待语义保留：客户端超时后凭同一 commitID 重试即可取到冻结结果，不需要长连接。

## Acceptance Criteria

### AC-1: 稳定提交身份的解析与派生
- **Type**: `rule`
- **Given**: HTTP 请求带/不带 `Idempotency-Key` 头、Kafka 消息带/不带 key、事件体带/不带 commitID 字段的六种组合
- **When**: 接收侧解析提交身份
- **Then**: 优先采用头/key/字段；均缺失时由 `(shardKey, fileID)` 确定性派生；同一 (shardKey,fileID) 两次派生值相同，不同 shardKey 或 fileID 派生值不同
- **Pass Condition**: 身份解析单测覆盖全部来源与回退顺序，派生函数确定性与区分性断言通过
- **Evidence**: `go test ./internal/pipeline/... ./internal/incoming/... -short` 中新增身份解析用例输出

### AC-2: 接收台账与阶段单调推进
- **Type**: `rule`
- **Given**: 一次成功的文件接收
- **When**: 接收从读取推进到通知
- **Then**: 台账按 received→staged→indexed→published→notified 单调记录，每次跳变带时间戳；files 行在发布时置 published_at；冻结结果 JSON 可被按 commitID 读回
- **Pass Condition**: 内存台账状态机单测断言完整阶段序列、不可回跳、结果可按 commitID 读回；MySQL/Spanner DDL 经静态评审含对应列与唯一约束
- **Evidence**: 台账单测输出；`migrations/003_receptions.up.mysql.sql` 与 `003_receptions.up.spanner.sql`

### AC-3: 暂存耐久化与原子发布
- **Type**: `rule`
- **Given**: 任意一次接收
- **When**: 暂存完成但发布未完成时观察 mergable 目录；发布完成后观察两处
- **Then**: 发布前 `mergable/<shard>/<fileID>.ach` 不存在、`staging/<shard>/<commitID>/<fileID>.ach` 存在；发布为同根 rename；发布后暂存副本消失，mergable 文件 sha256 与入站文件字节哈希一致
- **Evidence**: 暂存/发布单测（目录快照 + sha256 断言）输出

### AC-4: 暂存落盘失败清理未发布内容
- **Type**: `rule`
- **Given**: storage.WriteFile 在暂存阶段被注入失败
- **When**: 接收执行
- **Then**: `staging/<shard>/<commitID>/` 被清理；mergable 无该文件；台账停留/回到可重试的 received（不产生 published）；消息未被 Ack；同一消息重试后成功且 mergable 仅一份文件
- **Evidence**: 故障注入用例（失败计数、目录断言、重试后单文件断言）

### AC-5: 索引写入失败保留可重试事实
- **Type**: `rule`
- **Given**: files 索引写入被注入失败
- **When**: 接收执行，随后恢复故障并重试同一 commitID
- **Then**: 首次尝试后台账保持 staged、staged_at 与暂存文件保留、无 published_at；重试不重写暂存（内容哈希/写入路径核对），补索引并成功发布；最终 mergable 仅一份文件、files 仅一行
- **Evidence**: 索引故障注入 + 重试恢复用例输出

### AC-6: 结果通知失败不重写不重复计入
- **Type**: `rule`
- **Given**: 发布成功后 events.Emitter 被注入失败，之后恢复
- **When**: outbox 循环/重启补偿运行
- **Then**: 通知失败期间不发生任何暂存/发布写入（storage 写计数为 0），文件不进入合并计数；恢复后冻结结果被重发，事件携带同一 commitID；notified_at 仅在发送成功后置位
- **Evidence**: emitter 故障注入用例（写调用计数、阶段断言、补发事件 commitID 断言）

### AC-7: 重启后三类残留的分类与续作
- **Type**: `rule`
- **Given**: 分别构造三种崩溃后现场：①台账 published、mergable 在位、notified_at 为空；②台账 staged、暂存在位、未发布（含「rename 已发生但标记未写」子情形）；③files 已发布但 mergable 与暂存均缺失
- **When**: 在新 FileReceiver 上执行启动恢复
- **Then**: ①补发结果并置 notified_at；②原子补发布（子情形经哈希核对后补标记），文件可查询可取消；③置 failed、冻结错误结果、触发告警且不进入合并；另对无台账的 mergable 遗留文件回填索引
- **Pass Condition**: 恢复扫描对每种现场产出正确终态，且全过程无重复 mergable 文件
- **Evidence**: 恢复单测四种现场（三类 + legacy 回填）断言输出

### AC-8: 同身份 HTTP/Kafka 重试只返回原结果
- **Type**: `rule`
- **Given**: 同一 commitID 的两次提交（HTTP 两次 POST，或 Kafka 重投），第二次发生在首次发布之后
- **When**: 重试被处理
- **Then**: 两次返回的 QueueACHFileResponse 字段一致（id、shardKey、error、acceptedAt、nextCutoff、commitID）；mergable 中仅一份文件，storage 写入仅发生一次；消息在拿到 published 结果路径后被 Ack
- **Evidence**: HTTP 幂等用例（httptest）与消息层幂等用例（响应一致性 + 写计数断言）

### AC-9: Ack 仅在发布完成后发生
- **Type**: `rule`
- **Given**: 手动提交模式（AutoCommit=false），使用可计数 Ack 的包装订阅与「发布完成」观测点
- **When**: 一次接收全流程执行
- **Then**: Ack 调用严格晚于 mergable 文件可见与台账 published；处理失败路径不 Ack；AutoCommit=true 时行为与现状一致（收到即 Ack，不回退该模式）
- **Evidence**: Ack 时序单测（happens-after 断言/计数）与 AutoCommit 既有用例保持通过

### AC-10: 取消只处理已发布文件且预到达意图保留
- **Type**: `rule`
- **Given**: 三种情形：已发布文件的取消；文件尚在 staged 时取消；取消先于提交到达
- **When**: 调用 DELETE / CancelACHFile，随后（如适用）文件完成发布
- **Then**: 已发布文件取消成功（`.ach`→`.ach.canceled`，canceled_at 置位，响应 successful=true），重复取消仍成功；未发布取消返回既有语义且不破坏暂存、不报 500，发布时文件直接落为 `.canceled`；重启不丢失预到达意图
- **Pass Condition**: 现有 `internal/test/cancel_test.go` 与合并取消相关测试全部通过，新增三情形用例通过
- **Evidence**: 取消相关 `go test` 输出

### AC-11: 截止合并只含已发布未取消文件
- **Type**: `rule`
- **Given**: mergable 目录中混有已发布、已取消、未发布/恢复未决（无 published 索引）的输入
- **When**: 触发 cutoff 的 WithEachMerged
- **Then**: 仅已发布且未取消文件被合并与上传并产生 FileUploaded；未发布/未决文件不被移动、不被计入；正常场景（空、单文件、多文件）合并产物与改动前一致
- **Evidence**: 合并门控用例 + 现有 merging 测试套件全部通过

### AC-12: 现有校验、路由与端到端行为回归
- **Type**: `rule`
- **Given**: 现有测试套件（pipeline、incoming/web、incoming/test、files、storage）
- **When**: 执行 `go test ./... -short`（以及 gofmt、go vet）
- **Then**: 全部通过；Nacha/JSON 格式失败仍发 InvalidQueueFile；缺 fileID/shardKey、无 aggregator 路径行为不变；不同 shardKey 路由正确
- **Pass Condition**: 命令退出码 0，无新增跳过的既有测试
- **Evidence**: 测试命令汇总输出

### AC-13: 阶段机崩溃窗口覆盖质量
- **Type**: `rubric`
- **Dimension**: 可恢复性设计完备度——每个阶段边界的崩溃是否都有明确恢复动作，是否存在静默丢失或重复计入窗口
- **Scale**: 1-5
- **Anchors**: 1 = 仍有崩溃窗口导致丢文件或重复合并；3 = 主要窗口已覆盖但依赖人工判断一处以上；5 = 五个阶段所有边界崩溃均可自动分类恢复或以 failed+告警收尾，评审逐窗口核对无遗漏
- **Pass Threshold**: >= 4
- **Evidence**: 阶段/崩溃矩阵（spec→tasks TR 映射）、恢复测试与独立评审记录

### AC-14: 改动局部性与契约兼容
- **Type**: `rubric`
- **Dimension**: 向后兼容与改动克制度——接口扩展是否可选/默认，旧客户端与旧数据是否无感，是否避免无关重构
- **Scale**: 1-5
- **Anchors**: 1 = 破坏现有 API/事件或大面积重写无关模块；3 = 核心兼容但有一处需要客户端迁移；5 = 新字段/头全部可选，旧消息旧数据直接可用，改动集中在接收/存储/恢复相关包
- **Pass Threshold**: >= 4
- **Evidence**: OpenAPI diff、事件结构体 omitempty 检查、迁移回填设计与独立评审记录

## Open Questions
- 无（四个关键设计选择已于 2026-10-14 确认；AutoCommit 模式边界、blob 存储排除项见 Non-Goals/Assumptions）。
