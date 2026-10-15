# ACH 接收链路可恢复提交与幂等重试 - Independent Review

- Reviewer: 独立评审 Agent（fresh context）
- Date: 2026-10-14
- Spec: `spec.md` / Tasks: `tasks.md`
- 环境约束：不使用 Docker/MySQL/Kafka/FTP；以 `go test ./... -short`、`go vet ./...`、`go build ./...`、gofmt、静态代码评审为主要证据。

## Review Contract

- 用户目标摘要：ACHGateway 文件接收从「先写索引再直接落 mergable、Ack 无状态」改为「稳定 commitID 驱动的 received→staged→indexed→published 可恢复阶段机」，实现 HTTP/Kafka 重试幂等返回原结果、发布后才 Ack、启动恢复、结果 outbox、取消/截止只作用于已发布文件，同时保持正常单次提交、格式校验与既有合并行为不变。
- 运行方式：`go test ./... -short`（25 包应全绿）；`go test ./internal/pipeline/... ./internal/incoming/web/ ./internal/storage/... ./pkg/...`（非 short，不需外部服务的包）；`go vet ./...`；`go build ./...`；`gofmt -l internal cmd pkg`。
- 外部依赖失败（MySQL 1045 / Kafka :19092 / FTP :2121）属环境限制，不计为缺陷。

## Checkpoints

### CP-R1: 提交身份与幂等（AC-1, AC-8, AC-14）
- [ ] commitID 来源优先级正确（metadata/Kafka key > 事件字段 > DeriveCommitID(shardKey,fileID)），缺省为确定性 sha256 且 .ach 后缀归一
- [ ] 同 commitID 重试不再写盘/合并，返回冻结结果（acceptedAt/nextCutoff/error 一致）
- [ ] HTTP Idempotency-Key 头、等待器 fan-out、openapi.yaml、omitempty 向后兼容
- 证据（由 reviewer 填）:

### CP-R2: 阶段机与 Ack 时机（AC-2, AC-3, AC-4, AC-5, AC-9）
- [ ] 仅在 published 后 Ack；AutoCommit 路径行为保持
- [ ] 暂存失败清理未发布内容、事实保留可重试（retriable 分类，不发 InvalidQueueFile）
- [ ] 索引失败保留 staged；MarkPublished 同事务覆盖台账与 files 行
- [ ] 通知失败不重写文件、不重复计数
- [ ] MoveFile 真正同盘原子+目录 fsync；ValidateOpts 随暂存迁移
- 证据:

### CP-R3: 取消门控（AC-10）
- [ ] 预到达取消（ledger 意图 + .canceled marker）在发布时原子生效
- [ ] 已发布文件取消成功；重复取消幂等；cutoff 后无法取消语义保持
- [ ] cancel_test.go 既有行为（第二次取消 successful=true）保持
- 证据:

### CP-R4: 启动恢复（AC-7）
- [ ] published 待通知 / 暂存续作 / rename 已发生补标记 / 双缺 failed+冻结错误结果+告警 四分类正确
- [ ] received-only 不告警不失败；legacy mergable 回填；无索引孤儿仅告警
- [ ] 恢复先于消息消费与 outbox 首次扫描
- 证据:

### CP-R5: 截止门控（AC-11, AC-12）
- [ ] WithEachMerged 仅处理 published 且非 canceled 的 .ach
- [ ] 未发布文件从隔离目录回移 mergable（绝不 RmdirAll 删除），下次 cutoff 可见
- [ ] 索引查询失败 fail-closed，零上传；既有空/单/多文件合并测试保持
- 证据:

### CP-R6: 回归与兼容性（AC-12, FR-11）
- [ ] Nacha/JSON 格式校验、InvalidQueueFile 行为不变
- [ ] 不同 shard 路由、取消 API、默认 shard 回退保持
- [ ] 全量 -short 测试绿；非 short 无外部依赖包绿
- 证据:

### CP-R7: 工程质量（rubric AC-14）
- [ ] 无新第三方依赖；Apache 头/注释风格；gofmt/vet 干净
- [ ] Mock 状态机与 SQL/Spanner 语义一致（阶段守卫、唯一冲突识别、published/failed 通知集合）
- [ ] 迁移 003 MySQL/Spanner DDL 与代码列一致；无破坏式表结构改动
- 证据:

### CP-R8: 崩溃窗口完备度（rubric AC-13）
- [ ] 逐窗口核对：RecordReception 后 / StageXfer 后 / MarkStaged 后 / Record 索引后 / MoveFile 前后 / MarkPublished 前后 / 结果通知前后，均有自动恢复或失败收尾，无静默丢文件、无重复合并
- [ ] 并发同 commit（竞争落败者）路径安全
- 证据:

## Review History

### 2026-10-14 独立代码评审（fresh context，读码 + 实跑 + 故障注入探针）

**总体结论：FAIL** — 架构与主体实现质量高（阶段机、outbox、事务化 MarkPublished、Ack 门控、幂等返回均成立），全量测试真实通过；但发现 3 个 major 缺陷（其中 2 个位于本次新增的崩溃恢复/截止门控关键路径，1 个为相对旧版合并行为的回归）与 1 个 minor，均经临时探针测试实跑证实（探针文件已删除，未改动任何生产代码）。

#### 命令证据（本环境实际执行）
- `go build ./...` → exit 0
- `go vet ./...` → exit 0
- `gofmt -l internal cmd pkg` → 无输出
- `go test ./... -short -count=1` → 全部 ok（含 internal/pipeline、internal/files、internal/incoming/web、internal/test 等，无 FAIL、无新增 skip）
- `go test ./internal/pipeline/... ./internal/incoming/web/ ./internal/storage/... ./pkg/... -count=1`（非 short）→ pipeline 9.5s / web 5.7s / storage 4.0s / pkg/* 全部 ok
- MySQL/Kafka/FTP 依赖测试按约 `-short` 跳过，未计为缺陷。
- 临时探针（`internal/pipeline/zz_review_probe_test.go`，评审后已删除）：4 个用例中 3 个按预期 FAIL，直接证实 F1/F2/F3/F4。

#### Checkpoint 结果

| Checkpoint | 结果 | 证据摘要 |
|---|---|---|
| CP-R1 提交身份与幂等（AC-1/8/14） | **PASS** | [commit.go](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/commit.go#L30-L36)、[models.go ResolveCommitID](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/incoming/models.go#L43-L63) 优先级 metadata>事件字段>sha256 派生，`.ach` 后缀归一；[kafka.go:140](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/kafka/kafka.go#L136-L141) KeyName=commitID；[api_files.go:223-274](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/incoming/web/api_files.go#L223-L274) 头解析+等待器 fan-out（缓冲 1，不下阻塞发布路径）；omitempty 齐全；openapi.yaml YAML 合法、字段可选。冻结结果重试一致性有 TestReception_SameCommitRetryReturnsFrozenResult 覆盖（写/移计数=2，不再写盘）。 |
| CP-R2 阶段机与 Ack（AC-2/3/4/5/9） | **PASS** | Ack 仅在 published 后（[file_receiver.go:394-401](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L394-L401)），AutoCommit 收到即 Ack 保持；暂存失败 CleanupStaging+ErrRetriable（不发 InvalidQueueFile，有专测）；索引失败停留 staged 且重试零重写（TestReception_IndexFailure_*）；[repo_receptions.go MarkPublished](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/files/repo_receptions.go#L210-L274) 单事务置两表；[filesystem.go MoveFile](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/storage/filesystem.go#L162-L190) rename+源/目标目录 fsync，WriteFile 亦 fsync；outbox 失败零写盘有 TestOutbox_FailingEmitter* 实测。 |
| CP-R3 取消门控（AC-10） | **PASS** | 预到达意图：台账 RequestCancellation + 空 `.canceled` marker，发布时 MoveFile 替换 marker（TestReception_PreArrivalCancel_*、TestPublishXfer_PreArrivalCancel）；已发布取消走 repo.Cancel+改名；重复取消幂等；[internal/test/cancel_test.go](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/test/cancel_test.go) 未改断言，非 short 实跑通过（第二次取消 successful=true）。 |
| CP-R4 启动恢复（AC-7） | **FAIL** | 六场景测试存在且通过，但漏掉「published 且已取消、待通知」现场：[recovery.go:107](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L107) 与 [:148](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L148) 把 `mergablePaths` 的第二个返回值（实为 `.json` opts 路径）当成 canceled 路径，导致健康的已取消文件被误判 failed（见 F1）。received-only/legacy 回填/双缺 failed 分支本身正确。 |
| CP-R5 截止门控（AC-11/12） | **FAIL** | fail-closed 与「未发布 .ach 回移」成立（TestCutoffGate_* 实跑），但：回移只搬 `.ach`、ValidateOpts `.json` 被遗弃/删除（F2）；门控 fileID 归一化与接收侧不一致，`<id>.ach` 形态的 fileID 永远被门控排除（F3）。 |
| CP-R6 回归与兼容性（AC-12/FR-11） | **PASS** | -short 25 包与非 short 套件全绿；InvalidQueueFile/parse 校验、缺 fileID/shardKey、无 aggregator、默认 shard 回退、SD-live 路由均与旧版一致（已比对 HEAD 旧 processACHFile/processMessage）；旧消息无 commitID 可消费；TestEventsAPI_FileUploaded 仅把断言增强为「在位且已发布」。注意 F3 本身属于相对旧合并行为的回归，跨映射到本 checkpoint。 |
| CP-R7 工程质量（AC-14） | **PASS** | go.mod/go.sum 零 diff（零新依赖）；gofmt/vet 干净；003 MySQL/Spanner DDL 列与代码 16 列一致、embed 自动收录；MarkPublished 三后端语义对齐；Mock 阶段守卫与 SQL 同义。小差异见 N1/N5（不影响正确性）。 |
| CP-R8 崩溃窗口完备度（AC-13） | **FAIL** | 窗口覆盖整体扎实（received/staged/indexed/rename/marker/outbox 均有分类），但 F1 把健康的 published(canceled) 错分到 failed；F4「rename 已完成、MarkPublished 失败」边界在热重投路径上只能等重启恢复（启动 reconcile 正确），不满足「重试续作到发布」的意图。并发落败者路径见 N2/N4。 |

#### Actionable Findings

**F1（major｜CP-R4/CP-R8，AC-7/AC-13，FR-8）启动恢复把健康的「已发布且已取消、待通知」台账误判为 failed；legacy 回填行同理**
- 位置：[recovery.go:107](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L107)、[recovery.go:148](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L148)；被误认的返回值见 [merging.go:153-157](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L153-L157)（第二返回值是 opts 路径，非 canceled 路径）。
- 问题：`achPath, canceledPath := m.mergablePaths(...)` 中 `canceledPath` 实际是 `<id>.json`；预到达取消发布的文件位于 `<id>.ach.canceled`（opts 在 `<id>.ach.canceled.json`）。published 行在 `notified_at IS NULL` 时（outbox 2s 窗口或 emitter 故障期间重启必现）经 `diskMatches(.ach, .json)` 两路径均 miss、staging 已清理 → `failReception("published file missing from mergable and staging")`：stage 翻转为 failed、冻结失败结果、触发告警、outbox 向客户端补发「失败」结果（文件其实完好且会按 canceled 正常跳过合并）。另外 legacy 回填行 `ContentSHA256` 为空，[diskMatches](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L245-L259) 直接返回 false，emitter 未及时送达时重启同样会被错判 failed。
- 复现：探针 `TestProbe_RecoveryCanceledPublished`（预到达取消→发布→reconcileShard）实测 `stage="failed" lastError="published file missing from mergable and staging"`，断言失败。
- 最小修复：恢复侧显式计算 `achPath+".canceled"`（及其 `.canceled.json` opts）传入 diskMatches；`expectedHash==""` 时退化为「任一发布形态文件存在即视为在位」（仅存在性核对，用于 legacy 回填行）。补一条 published+canceled+pending-notify 的恢复用例。

**F2（major｜CP-R5，AC-11/AC-3，FR-10）截止门控回移未发布文件时丢弃 ValidateOpts 旁车 `.json`**
- 位置：[merging.go:569-584](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L569-L584)（只 MoveFile `.ach`），空成功路径 [merging.go:605-608](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L605-L608) `RmdirAll(dir)`。
- 问题：rename 已完成/发布标记未写的微窗口内撞上 cutoff，未发布 `.ach` 被原子回移 mergable，但其同名 `.json` 留在隔离目录；若本轮无合格输入，隔离目录随后被 RmdirAll 删除（opts 永久丢失），混合场景下 opts 永久滞留隔离目录。下一 cutoff 该文件以默认 ValidateOpts 解析/合并，可能改变校验结果甚至导致合并读取失败——金融文件输入被静默改变。
- 复现：探针 `TestProbe_GateExclusionDropsOpts`（带 CustomReturnCodes opts 的未发布文件过一次 cutoff）实测 `ach returned=true opts returned=false transforms=0`。
- 最小修复：回移每个 path 时，若同目录存在 `TrimSuffix(path,".ach")+".json"` 一并 MoveFile 回 mergable；并为「带 opts 的未发布文件被门控回移」补断言。

**F3（major｜CP-R5/CP-R6，AC-11/AC-12，FR-10）fileID 带 `.ach` 后缀时文件永远无法通过 published 门控（相对旧行为的合并回归）**
- 位置：门控键归一化 [merging.go:570](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L570)（trim 后缀）；索引行写入用原始 fileID [file_receiver.go:605-611](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L605-L611)；POST 不裁剪后缀 [api_files.go:186-189](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/incoming/web/api_files.go#L186-L189)（DELETE 在 :318 明确裁剪，两侧不对称）。
- 问题：POST `/shards/{k}/files/{id}.ach`（仓库自带 [cancel_test.go:109](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/test/cancel_test.go#L109) 与 upload_test 均用此后缀形态；DeriveCommitID 也专门做了后缀归一）使 files 行键为 `<id>.ach`，磁盘基名为 `<id>.ach`，门控 trim 后查 `<id>` → 永不命中：文件每个 cutoff 都被「回移」、永远不合并、不上传（资金滞留），仅告警计数。旧版无门控时该文件正常合并，属本次引入的回归。
- 复现：探针 `TestProbe_GateFileIDWithAchSuffix`（repo 以 `weird.ach` 记录并置 published，磁盘同名）实测 `transforms=0`、日志 `excluded unpublished file ... fileID=weird`。
- 最小修复：在接收入口统一归一化 fileID（CreateFileHandler 与 CancelFileHandler 一样 TrimSuffix `.ach"`；事件消费侧同样归一），或门控匹配时同时尝试 raw/trimmed 两种键；建议前者并补「POST `<id>.ach` 后 cutoff 能合并」用例。

**F4（minor｜CP-R8，AC-13，FR-5/FR-8）rename 成功但 MarkPublished 失败后，热重投死循环 retriable，仅重启可自愈**
- 位置：[file_receiver.go:640-655](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L640-L655)。
- 问题：MoveFile 已把暂存挪入 mergable，随后事务失败/超时；消息不 Ack，立即重投时阶段仍为 indexed → 再次 PublishXfer → 源不存在报错 → 重读仍非终态 → 持续 retriable（Kafka 下表现为毒消息循环与错误日志风暴），直到进程重启由 resumeToPublished 按哈希核对补标记。无丢文件/无重复合并（门控 fail-closed 兜底），但「重试续作到发布」在该边界不成立。
- 复现：探针 `TestProbe_RenameDoneHotRedelivery` 实测重投返回 `retriable ... source not found`、stage 停留 `indexed`。
- 最小修复：发布失败分支（或发布前）复用恢复逻辑：mergable 目标已存在且哈希等于台账 content_sha256 时，跳过移动直接 MarkPublished。

**Nits**
- N1（nit｜CP-R7）[mock_repo_files.go RecordReception](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/files/mock_repo_files.go#L140-L161) 未强制 `(shard_key,file_id)` 唯一（[reception_mock_test.go:120-122](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/files/reception_mock_test.go#L120-L122) 注释已声明）；Mock 与 SQL 的并发票据仲裁仍有一处语义差，建议补齐以避免单测给人「已仲裁」的错觉。
- N2（nit｜CP-R8）[file_receiver.go:557-563](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L557-L563)：SQL 端同一 (shard,fileID) 不同 commitID 时 INSERT IGNORE 静默成功，随后按本方 commitID GetReception 恒 NotFound → retriable 无限重投（无终态分类）。属调用方误用，建议按 (shard,fileID) 反查胜出者结果或显式失败收尾。
- N3（nit｜CP-R5）fail-closed 的 published 查询发生在 isolateMergableDir 之后（[merging.go:529 vs 562-566](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L529-L566)）：索引故障时全部输入滞留隔离目录、下轮 cutoff 不可见，仅能走手工 file-uploaded 路由。零上传满足，但把索引查询提前到隔离之前可自动留活。
- N4（nit｜CP-R8）同 commitID 单实例内两路消费并发时 StageXfer 无 per-commit 串行化（同字节 O_TRUNC 双写，实际良性）；可加 per-commit 锁与 DB 仲裁对齐。
- N5（nit）Spanner MarkNotified 的 wantStage 传 StagePublished（[repo_receptions_spanner.go:288-293](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/files/repo_receptions_spanner.go#L288-L293)），靠 rank 比较侥幸正确，建议传 failed 或特判。
- N6（nit）[merging.go:253](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L253) `len(stagedOpts) > 0` 对字符串路径恒真，属失效条件。
- N7（nit）frozenQueueResponse 重建后用 `time.Now()` 再冻结（[file_receiver.go:696](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L696)），SQL 会重写 published_at（仅崩溃窗口、无 JSON 时），建议保留原 published_at。
- N8（nit）[file_receiver.go:114-115](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L114-L115)：recoverInFlight 用 Background ctx、扫描失败仅日志无告警；aggregator cutoff goroutine 在恢复完成前已启动，存在「启动瞬间 cutoff 与恢复抢 mergable 目录」的窄竞态。

### Review R2 (2026-10-14)

**总体结论：PASS** — F1/F2/F3 三个 major 与 F4 minor 均已正确修复并有真实运行的回归测试锁定；N3/N6/N7 同步修复。`go build`/`go vet`/gofmt 干净，`-short` 25 包全绿，pipeline 非 short 全绿，4 个新回归用例全部真实执行且 PASS（无 skip）。复评发现 2 个**新记录的 minor（R2-N1/R2-N2，均为亚秒级窗口、资金安全不受影响）**与 4 个维持原判的 nit，均不阻塞。评审期间临时探针（`internal/pipeline/zz_r2_probe_test.go`）已删除，未改动任何生产代码。

#### 命令证据（本环境实际执行）
- `go build ./...` → exit 0；`go vet ./...` → exit 0；`gofmt -l internal cmd pkg` → 无输出。
- `go test ./internal/pipeline/... -short -count=1` → ok 3.559s；非 short `go test ./internal/pipeline/... -count=1` → ok 4.119s。
- 4 个新回归 `-v` 实跑：`TestRecovery_PublishedCanceledPendingNotify_NotFailed`、`TestCutoffGate_UnpublishedReturnsValidateOpts`、`TestCutoffGate_AcceptsSuffixedIndexKey`、`TestReception_RedeliveryAfterMoveFinalizesWithoutRestaging` 均 `--- PASS`，无 SKIP。
- `go test ./... -short -count=1` → 25 包 ok，无 FAIL（含 internal/test 的 cancel 用例与 internal/files Mock 用例）。
- 非 short 旁证：`internal/incoming/web` 5.37s、`internal/storage` 2.67s、`pkg/compliance|models|test` 全 ok；`internal/files` 的 TestRepository/TestRepository_Receptions 仍为 MySQL 1045 环境缺失（既有环境限制，不计缺陷）。

#### F1..F4 逐条裁决

| Finding | 裁决 | 证据 |
|---|---|---|
| **F1** 恢复误判 published+canceled+pending-notify | **RESOLVED** | [recovery.go:101-104](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L101-L104) `mergableCandidates` 显式返回 `<id>.ach` 与 `<id>.ach.canceled`（不再把 [merging.go:153-157](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L153-L157) 的第二返回值 `.json` opts 路径当作 canceled）；StagePublished 分支 [recovery.go:117-129](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L117-L129) 与 resume 路径 [recovery.go:159](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L159) 均用之。空哈希退化仅限 legacy 回填行：[recovery.go:254-265](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L254-L265) `expectedHash == "" || got == expectedHash`——正常行 ContentSHA256 由 MarkStaged 必填，哈希核对强度未削弱；空文件仍判不存在（storageHashIfExists 要求非空）。新回归 `TestRecovery_PublishedCanceledPendingNotify_NotFailed` 连续两次 reconcile 后 stage 仍 published、pending 集合仍含该结果，实跑 PASS。 |
| **F2** 门控回移丢弃 ValidateOpts | **RESOLVED** | [merging.go:584-593](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L584-L593) 回移时对 `path` 与 `TrimSuffix(path,".ach")+".json"` 逐个探测并 MoveFile；旁车缺失（Open 报错）即跳过，不产生假错误。目标固定为 `mergable/<m.shard.Name>`（[merging.go:573](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L573)），隔离目录本就由本分片目录改名而来，重命名在同一 Chest 根内，**无跨 shard 搬迁**；回移在 merge 之前，失败即整体返回错误（fail-closed），不会半并入。`TestCutoffGate_UnpublishedReturnsValidateOpts` 实跑断言 `.ach`+`.json` 双双回到 live mergable、transforms=0，PASS。 |
| **F3** `.ach` 后缀 fileID 永不过门控/回填 | **RESOLVED** | [merging.go:814-825](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L814-L825) `fileIsPublished` 同时尝试裸键与 `<id>.ach` 键；publishedFileID 先由 [merging.go:807-809](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L807-L809) 归一。误纳分析：两键只在「同一磁盘文件的别名」（`x` 与 `x.ach` 同一 basename、同一 DeriveCommitID）下重叠，磁盘只有一份文件，不可能放入额外文件。legacy 回填双查并按真实索引键更新：[recovery.go:325-336](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L325-L336) GetFile(id)→GetFile(id+".ach")，[recovery.go:354](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L354) 用 `indexKey` 调 MarkFilePublished。`TestCutoffGate_AcceptsSuffixedIndexKey` 实跑 transforms=1 且文件在隔离目录中参与 cutoff，PASS。 |
| **F4** rename 成而 MarkPublished 败 → 热重投死循环 | **RESOLVED** | 发布错误分支 [file_receiver.go:654-664](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L654-L664)：先重读台账，终态则返回冻结结果；否则仅当 `publishedContentPresent(existing.ContentSHA256, candidates)` **哈希命中**（空哈希不可达：到此阶段必经 MarkStaged）才走 [finalizePublished](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L674-L699)——内容已验证；索引行由同事务 MarkPublished（[file_receiver.go:687](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L687)）兜底，行缺失则 retriable，不会裸标 published。正常成功路径也收敛到同一函数（[:667](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L667)），冻结/标记/清暂存/pendingFiles 顺序与 R1 评审通过的内联实现一致（CP-R2 保持）。`TestReception_RedeliveryAfterMoveFinalizesWithoutRestaging` 实跑：重投 NoError、acks=1、stage=published、mergable 仅一份，PASS。 |

**附带修复核对**
- N3 **RESOLVED**：published 集合查询已提前到隔离之前——[merging.go:530-537](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L530-L537) 在 [isolateMergableDir :540](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L540) 之前；索引故障时 mergable 原封不动，`TestCutoffGate_FailsClosedWhenIndexUnavailable` 实跑 transforms=0，PASS。
- N6 **RESOLVED**：[merging.go:253](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L253) 改为 `stagedOpts != "" && m.fileExists(stagedOpts)`，以磁盘实况为准（前者恒真但无害）。
- N7 **RESOLVED**：[file_receiver.go:717-721](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L717-L721) 重建冻结结果时保留台账原 `PublishedAt`，不再用 time.Now() 重写。

#### Checkpoint 复评

| Checkpoint | R1 | R2 | 依据 |
|---|---|---|---|
| CP-R1 提交身份与幂等 | PASS | **PASS**（未受影响） | 全量 -short 绿；既有冻结结果用例仍过。 |
| CP-R2 阶段机与 Ack | PASS | **PASS** | finalizePublished 抽取后两条路径（首次发布/重投补标）行为等价：MarkPublished 同事务→清暂存→pendingFiles→返回 resp(true)，Ack 仍仅在 published 门后（F4 用例 acks=1 时序断言）。 |
| CP-R3 取消门控 | PASS | **PASS** | internal/test -short 绿；F1 修复使预到达取消的恢复形态 `.ach.canceled` 被正确承认（见 R2-N2 的残余窗口）。 |
| CP-R4 启动恢复 | FAIL | **PASS** | F1 关闭；六场景 + 新 canceled-pending 回归全绿；legacy 双查键修复；空哈希退化仅限回填行。残余见 R2-N1/R2-N2（minor）。 |
| CP-R5 截止门控 | FAIL | **PASS** | F2/F3/N3 全部关闭；回移不跨 shard、双键匹配不误纳；fail-closed 提前到隔离前；既有空/单/多/canceled/opts 合并套件非 short 全绿。 |
| CP-R6 回归兼容 | PASS | **PASS** | 25 包 -short + pipeline/web/storage/pkg 非 short 全绿；F3 修复反而消除了 R1 记录的相对旧行为回归。 |
| CP-R7 工程质量 | PASS | **PASS** | build/vet/gofmt 干净；新测试均为真实 TempDir+Mock 状态机故障注入，无 skip。 |
| CP-R8 崩溃窗口 | FAIL | **PASS（≥4）** | F1/F4 关闭后逐窗口再核：received/staged/indexed/rename 前后/MarkPublished 前后/通知前后均有自动收尾；两个残余 minor（下）不造成丢文件或重复合并——一个丢的是 opts 旁车（退默认校验），一个是误发失败结果+告警，资金移动均被门控阻断。 |

#### 新发现 / 遗留项

- **R2-N1（minor｜CP-R4/CP-R8）崩溃于「ACH rename 已完成、opts rename 未完成」之间时，恢复/热重投补标会删除暂存 ValidateOpts 旁车**
  - 位置：[recovery.go:159-185](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L159-L185) 内容在位即跳过 PublishXfer，随后 [recovery.go:202-204](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/recovery.go#L202-L204) CleanupStaging；热路径同理 [file_receiver.go:657-662](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L657-L662)+[691-693](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L691-L693)。
  - 复现：临时探针 `TestProbeR2_RecoveryMovedACH_DropsOpts`（评审后已删）——StageXfer 带 CustomReturnCodes opts，仅手动 MoveFile ACH 后 reconcile，实测 `mergable/<id>.json` 不存在、staging 被清空。
  - 影响：窗口为两次相邻 rename 之间（亚秒；注意 PublishXfer 内 opts move 失败本身被吞为 warning 且返回 nil，故仅崩溃可触发）；后续 cutoff 以默认 ValidateOpts 解析该文件。无丢 ACH、无重复合并。建议：补标前若 staged opts 存在而目标 opts 缺失，补一次 MoveFile。
- **R2-N2（minor｜CP-R4/CP-R5）零合格输入的 cutoff 会 RmdirAll 掉「已发布已取消、待通知」的 `.ach.canceled`，随后启动恢复把健康台账误置 failed**
  - 位置：canceled 文件不进 matches（[merging.go:485-499](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L485-L499)），门控只回移非 canceled 输入；空成功清理 [merging.go:618-627](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/merging.go#L618-L627) 连 canceled 一并删除。
  - 复现：临时探针 `TestProbeR2_CutoffEmptyRemovesPendingCanceled`（已删）——预到达取消→发布（`.ach.canceled`，notified 待发）→一次无合格输入的 cutoff，实测 canceled 文件 GONE；再 reconcile 后 `stage=failed`（冻结失败结果 + 告警）。
  - 影响：需 cutoff 恰好落在 outbox 补发窗口（秒级）且该 shard 本轮零 eligible；文件本就 canceled 永不合并，资金安全；代价是向客户端误发一次失败结果与误告警。该清理行为早于本轮修复存在（R1 时因 F1 必然 failed 而被掩盖），F1 修复后成为 canceled-pending 场景的残余路径。建议：空清理/门控回移时把 published 且 notified 未完的 `.ach.canceled` 一并送回 mergable，或清理前按台账豁免。
- N1（Mock 未强制 (shard_key,file_id) 唯一）、N2（异 commitID 落败者无终态分类）、N4（同 commitID 单实例无 per-commit 串行化）维持 nit，未修不阻塞；N5 Spanner MarkNotified 的 SQL 已改为 `stage IN ('published','failed')`（[repo_receptions_spanner.go:288-293](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/files/repo_receptions_spanner.go#L288-L293)），辅助函数 wantStage 仍传 published（仅风格问题）。
- N8 维持 nit：cutoff goroutine 仍先于恢复启动（[pipeline.go:55](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/pipeline.go#L55) vs [:67](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/pipeline.go#L67)、[file_receiver.go:114-115](file:///Users/huangding/Documents/new-SWE/0910142/project-01/internal/pipeline/file_receiver.go#L114-L115)）。门控已显著降低其危害（恢复未决文件不会被合并，而会被原子送回 mergable），但目录改名与恢复 MoveFile 仍可能瞬时交错；建议后续把恢复置于 aggregator.Start 之前或加分片恢复屏障。

**结论：PASS** —— 三个 round-1 major（F1/F2/F3）与 F4 全部 RESOLVED，修复均最小、对症且未引入资金安全缺口；新增 2 个 minor 属窄窗口硬化项（建议跟进但不阻塞合入）；构建/vet/gofmt 与 -short 25 包、pipeline 非 short 测试全部绿色。

