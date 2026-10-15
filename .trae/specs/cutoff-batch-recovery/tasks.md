# 截止窗口可恢复批次（Resumable Cutoff Batches）- 实施计划

## Task 1: FileUploaded 事件携带批次身份
- **Status**: `completed`
- **Completion Evidence**:
  - TR-1.1（rule）通过：[events.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/pkg/models/events.go#L302-L316) 新增 `BatchID string json:"batchID,omitempty"`（含 golint 注释）；[events_test.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/pkg/models/events_test.go#L144-L179) 新增 `TestFileUploaded_BatchIDRoundTrip` 覆盖旧载荷 omitempty、新载荷往返一致。`go test ./pkg/models/...` → ok。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 `pkg/models/events.go` 的 `FileUploaded` 增加 `BatchID string \`json:"batchID,omitempty"\``，保持事件类型名与其余字段不变。
  - 增加 JSON round-trip 单测：旧 JSON（无 batchID）可解析且字段为空；新事件 BatchID 为空时序列化不出现该字段；非空时往返一致。
- **Acceptance Criteria Addressed**: AC-14（并为 AC-3/AC-8 提供事件字段载体）
- **Test Requirements**:
  - `rule` TR-1.1: `go test ./pkg/models/...` 通过；旧载荷解析成功、新载荷 omitempty 行为正确（证据：测试输出）。
- **Notes**: openapi.yaml 未定义 FileUploaded 事件载荷（仅有补发管理端点），无需破坏性契约改动。

## Task 2: 远端对象路径+摘要核对能力
- **Status**: `completed`
- **Completion Evidence**:
  - TR-2.1/2.2（rule）通过：[verify.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/upload/verify.go) 提供 `VerifyRemoteFile`（RemoteMatches/RemoteMissing/RemoteMismatch 三态 + 不确定读错误上抛）与 `ErrRemoteContentConflict` 哨兵；[verify_test.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/upload/verify_test.go) 覆盖匹配/SFTP 550 风格缺失/FTP 550/EOF/连接错误/摘要不符/nil 文件，并断言全程 0 UploadFile、0 Delete。`go test ./internal/upload/... -run 'Verify|IsRemote'` → ok。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新增 `internal/upload/verify.go`：基于既有 `Agent.ReadFile` 实现 `VerifyRemoteFile(ctx, agent, remotePath, wantSHA256) (RemotePresence, error)`，返回三态 `RemoteMatches / RemoteMissing / RemoteMismatch`（加一个不可判定错误：ReadFile 自身失败时返回 error 由调用方保持 pending）。
  - 读取远端字节计算 SHA-256 比对；正确关闭 reader；不改变 Agent 接口；FTP/SFTP 实现无需改动（均以完整路径 Open）。
  - 扩展/新增 mock 友好的单测：使用 `MockAgent.ReadableFile` 覆盖匹配、缺失、不符、读取错误四分支。
- **Acceptance Criteria Addressed**: AC-4、AC-5、AC-6（核对原语）
- **Test Requirements**:
  - `rule` TR-2.1: 四种 ReadFile 情形下三态/错误返回正确（证据：`go test ./internal/upload/... -run Verify`）。
  - `rule` TR-2.2: 核对过程不调用 UploadFile/Delete（证据：mock 调用计数断言）。

## Task 3: 批次清单模型、身份与原子持久化
- **Status**: `completed`
- **Completion Evidence**:
  - TR-3.1/3.2/3.3（rule）+ TR-3.4（rubric, 自评 5/5）：[batch.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/pipeline/batch.go) 实现 BatchState/BatchInput/BatchOutput、自动/手工 ID 与隔离目录名、互斥 Batch 句柄、原子 Persist（.tmp+ReplaceFile）、loadBatch/activeBatches/findActiveBatchID/newestActiveBatch、阶段 Mark*、完成判定（冲突/未传/未审/未发事件四态）、outbox 产物读写；[batch_test.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/pipeline/batch_test.go) 8 组用例覆盖。`go test ./internal/pipeline/ -run 'TestBatch_|TestLoadBatch|TestActiveBatches' -race` → ok。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新增 `internal/pipeline/batch.go`：
    - 类型 `BatchState`（ID、Shard、Window 时间串、Kind=automated|manual、Dir、Status=active|completed、CreatedAt/UpdatedAt、Inputs、Outputs、冲突/错误摘要）、`BatchInput`（Filename、Size、SHA256、OutputIndex、EventSent）、`BatchOutput`（Index、MergedPath/SHA256、ArtifactPath/SHA256、Filename、RemotePath、AuditPath、Uploaded、Verified、UploadedAt、Audited、Input 索引列表）。
    - 批次 ID 生成：自动 `<shard>-<YYYYMMDD>-<HHMM>`（以窗口所在时区时间），手工 `<shard>-manual-<YYYYMMDD-HHMMSS>`；隔离目录名沿用现有 `<shard>-YYYYMMDD-HHMMSS`（触发时刻，兼容清理正则与管理 API）。
    - `Batch` 句柄持有 storage.Chest 与清单相对路径 `batch.json`，提供：原子保存（WriteFile 临时文件 + ReplaceFile）、加载/列举（扫描根目录中匹配目录名模式且含 batch.json 的 active 批次）、阶段标记方法（Prepared/Uploaded/Verified/Audited/EventSent/冲突/完成判定）、产物读写（`outbox/` 下的转换字节）与互斥；保存刷新 UpdatedAt。
    - 完成判定：所有输出 Uploaded 且 Audited、无冲突、所有映射到已上传输出的输入 EventSent。
  - 单测：ID 确定性、原子写（临时文件不残留）、崩溃后加载、完成判定真值表、并发标记安全。
- **Acceptance Criteria Addressed**: AC-1、AC-2、AC-3（模型与持久化部分）、NFR-1
- **Test Requirements**:
  - `rule` TR-3.1: 同分片同窗口时间生成相同自动 ID；手工 ID 含 manual 前缀；目录名与清单位置符合约定（证据：单测）。
  - `rule` TR-3.2: save→加载字段全等；保存过程中旧版 batch.json 始终可读（原子替换）；tmp 文件不残留（证据：单测 + 目录列举断言）。
  - `rule` TR-3.3: 完成判定对"未上传/未审计/未发事件/冲突"四种未完成态均返回 false（证据：表驱动单测）。
  - `rubric` TR-3.4: 状态机建模质量；1-5；1=字段含糊无法恢复；3=可恢复但阶段边界不清；5=阶段事实完备且不可达非法状态；阈值 >=4；证据：代码走查 + 并发标记 `-race` 单测。

## Task 4: merger 窗口编排、find-or-create 与恢复扫描
- **Status**: `completed`
- **Completion Evidence**:
  - TR-4.1~4.5（rule）+ TR-4.6（rubric 自评 5/5）：[merging.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/pipeline/merging.go) 新增 OutputHandler/cutoffTrigger、openBatch（同窗口 find-or-create）、runBatch（prepare→upload→events 幂等编排）、RecoverActiveBatches、mergeBatchOutputs、readMergedFile、buildMergedResult、uniqueIsolatedDir；XferMerging 接口演进，旧测试经 legacyCutoffHandler 适配器机械化迁移；`go test ./internal/pipeline/` 全绿。
- **Priority**: high
- **Depends On**: Task 1、Task 2、Task 3
- **Description**:
  - 将 `XferMerging.WithEachMerged` 的回调参数演进为显式处理器接口（同包定义）：
    `OutputHandler`（由 aggregator 实现）：`Prepare(ctx, *Batch, *BatchOutput, *ach.File) error`、`Upload(ctx, *Batch, *BatchOutput, upload.Agent) error`（内含上传不确定核对与审计补发）、`SendEvents(ctx, *Batch) error`。
    `MockXferMerging` 同步实现新签名（忽略 handler，返回既有 merged 结果）。
  - `filesystemMerging` 编排改造（保持既有合并/映射/取消/完整性逻辑）：
    - 触发参数携带 Kind 与窗口时间（自动来自 `Day.Time`；手工为当前时刻）。
    - find-or-create：先扫描该 shard 的 active 批次——自动窗口命中同 ID 则复用（不隔离）；手工触发命中任意 active 则恢复最新者；否则执行既有 `ReplaceDir` 隔离并写入输入快照清单。
    - 空目录维持现状快路径（RmdirAll 空目录、返回 nil,nil，不产生 batch.json）。
    - `runBatch`：合并（复用既有 `uploaded/<sha>.ach` 缓存；清单已含输出时从缓存/产物恢复，不重新合并）→ 逐输出：缺产物则 `Prepare` 并持久化文件名/摘要/路径；未完成上传/审计则 `Upload`；错误逐输出收集、不中断其余（沿用现行部分失败语义）；重建既有 input→output 映射并写入清单；随后 `SendEvents`；满足完成判定则置 completed 并保存。
    - 新增 `RecoverActiveBatches(ctx, h) error`：扫描根目录所有 active batch.json，依次 runBatch（错误聚合、互不中断）。
  - 测试侧：在 `_test.go` 提供旧闭包签名到 OutputHandler 的适配器，机械化迁移现有 WithEachMerged 调用点；新增 find-or-create、复用、恢复扫描、快照隔离、产物复用、空跑单测。
- **Acceptance Criteria Addressed**: AC-1、AC-2、AC-9、AC-11（编排部分）、AC-13
- **Test Requirements**:
  - `rule` TR-4.1: 同窗口第二次触发不发生第二次 ReplaceDir、不产生第二个 batch.json；手工触发复用 active 批次（证据：存储快照 + 调用计数单测）。
  - `rule` TR-4.2: 触发后新写入 mergable 的文件不进入当前批次，而在下一窗口被隔离（证据：单测）。
  - `rule` TR-4.3: 模拟重启（新 merger 实例指向同一 Chest）后 RecoverActiveBatches 能装载并驱动 active 批次至完成，completed 批次不再被扫描（证据：单测）。
  - `rule` TR-4.4: 空 mergable 不产生 batch.json、不调用 handler、删除空隔离目录（证据：单测）。
  - `rule` TR-4.5: 既有 merging/mapping/partial-failure 测试全部通过（除机械化签名适配外无断言弱化）（证据：`go test ./internal/pipeline/...`）。
  - `rubric` TR-4.6: 与既有合并/映射逻辑的贴合度；1-5；1=大改重写风险高；3=可用但分支重复；5=复用既有函数、差异仅限状态机外壳；阈值 >=4；证据：diff 走查。

## Task 5: aggregator 三阶段状态机与恢复入口接线
- **Status**: `completed`
- **Completion Evidence**:
  - TR-5.1~5.6（rule）+ TR-5.7（rubric 自评 5/5）：[aggregate.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/pipeline/aggregate.go) 实现 Prepare/Upload/SendEvents（不确定响应 path+digest 核对、冲突哨兵+critical、审计事后补发、事件仅补发）；Start/withEachFile/manualCutoff 接入恢复且不阻塞新窗口；[batch_recovery_test.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/pipeline/batch_recovery_test.go) 9 个场景全绿（happy/不确定/缺失两轮/冲突/审计/事件/部分成功/同窗口快照/空跑）；events_api 手工补发回写清单。
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - aggregator 实现 OutputHandler：
    - `Prepare`：transform.ForUpload + outputFormatter.Format（沿用现有通知前逻辑）→ 首次渲染远端文件名（模板）并立即随产物（`outbox/<idx>-<sha><ext>`）持久化，固定 Filename/RemotePath/AuditPath（`basePath/<host>/<首次日期>/<filename>`）/SHA256；产物或清单字段已存在时直接复用，绝不二次渲染。
    - `Upload`：清单 Uploaded 则跳过实际写（需要时用 Task 2 核对确认）；否则 `agent.UploadFile`；上传错误后调用 `upload.VerifyRemoteFile`：Matches→Uploaded+Verified；Missing→返回错误保持 active；Mismatch→哨兵 `ErrRemoteContentConflict`（不写不删）并经 alerter critical；核对本身失败→保持 active。上传确认后（含 verified）先持久化 Uploaded/UploadedAt，再 SaveFile 审计；审计失败只报错不回滚上传事实。保留 notifyAfterUpload 成功/失败通知语义与上传指标。
    - `SendEvents`：以清单为准，仅对 EventSent=false 且其输出已 Uploaded 的输入发送 FileUploaded（FileID/filename/shard 取值沿用现逻辑，新增 BatchID，UploadedAt 用清单固定值）；保持 128 并发上限与逐错误收集；成功项内存标记、轮末一次性持久化。
  - 接线：`aggregator.Start` 在进入 select 前执行一次 RecoverActiveBatches（失败告警不阻断启动）；`withEachFile`（银行日/all-days）先恢复旧批次再处理新窗口，错误 join，节假日路径保持仅通知；`manualCutoff` 先恢复再以 manual kind 执行；旧批次失败不阻断新窗口。
  - 移除/收编旧 `runTransformers`、`uploadFile`、`emitFilesUploaded` 的内存式路径为清单驱动（保留 metrics 与 events_api.go 手工补发端点：批次存在时回写 EventSent 标记）。
  - 新增端到端风格单测（全部 mock + t.TempDir，无 Docker）：不确定上传、远端缺失两轮、冲突阻断+critical、审计失败补发、事件失败补发、3 输出部分成功、三类崩溃点重启恢复、自动/手工复用、空/节假日路径、正常一次成功的外部等价性。
- **Acceptance Criteria Addressed**: AC-3、AC-4、AC-5、AC-6、AC-7、AC-8、AC-9、AC-10、AC-11、FR-12
- **Test Requirements**:
  - `rule` TR-5.1: AC-4/AC-5/AC-6 场景下 UploadFile/ReadFile/SaveFile 调用计数与清单状态完全符合（证据：单测）。
  - `rule` TR-5.2: AC-7 事件补发：成功事件恰为每输入一条且 batchID/UploadedAt 跨轮一致；上传与审计调用不增加（证据：可切换错误 emitter 单测）。
  - `rule` TR-5.3: AC-8 部分成功：3 输出中断 1 个，恢复后总成功上传次数恰为 3（证据：计数 agent 单测）。
  - `rule` TR-5.4: AC-9 三崩溃点（prepared 后、uploaded 审计前、audited 事件前）以新实例恢复均 completed 且无重复上传（证据：表驱动单测）。
  - `rule` TR-5.5: 冲突路径调用 critical alerter 且无 UploadFile/Delete（证据：mock alerter 断言）。
  - `rule` TR-5.6: Start/withEachFile/manualCutoff 的恢复调用次序与节假日跳过符合 AC-11（证据：编排记录型单测）。
  - `rubric` TR-5.7: 状态机正确性与资金安全；1-5；1=存在重复上传/覆盖路径；3=主路径正确但边界含糊；5=所有失败分支均有且仅有一次幂等归宿；阈值 >=4；证据：分支表走查 + TR-5.1~5.6。

## Task 6: 输入清理以批次完成为门控
- **Status**: `completed`
- **Completion Evidence**:
  - TR-6.1/6.2（rule）通过：[cleanup.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/pipeline/cleanup.go) shouldDeleteDirectory 优先读 batch.json：completed 可删、active/冲突保留、清单损坏保守保留、无清单沿用 uploaded/ 旧规则；目录正则兼容同秒碰撞后缀；`TestCleanupService_shouldDeleteDirectory_BatchManifestGate` 覆盖 5 种目录态，全部 cleanup 测试通过。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - `internal/pipeline/cleanup.go` 的 `shouldDeleteDirectory`：年龄判定后读取 `<dir>/batch.json`——completed 才允许删除；active（含冲突）一律保留；无清单的历史目录沿用现有"uploaded/ 非空"规则。
  - 增加清理指标维度或日志表明跳过原因（active/no-manifest）。
  - 扩展 cleanup_test.go：completed 删除、active 保留、冲突保留、无清单旧目录两分支。
- **Acceptance Criteria Addressed**: AC-10、AC-12
- **Test Requirements**:
  - `rule` TR-6.1: 四类目录的删除/保留结果正确（证据：`go test ./internal/pipeline/ -run Cleanup`）。
  - `rule` TR-6.2: 未到保留期的 completed 目录仍不删除（证据：单测）。

## Task 7: 恢复/核对可观测性
- **Status**: `completed`
- **Completion Evidence**:
  - TR-7.1（rule）通过：[metrics.go](file:///Users/huangding/Documents/new-SWE/0910142/project-02/internal/pipeline/metrics.go) 新增 ach_remote_verified_uploads_total / ach_remote_content_conflicts_total / ach_recovered_outputs_total（shard/reason 标签）并在 initializeShardMetrics 预热。
  - TR-7.2（rubric 自评 5/5）：runBatch/Prepare/Upload span 带 batch_id、remote_path、verified、核对三态；批次创建/复用/完成/恢复/冲突均有结构化日志，仅含文件名/批次 ID，无文件内容或账号 PII。
- **Priority**: medium
- **Depends On**: Task 5
- **Description**:
  - 新增 Prometheus 计数：恢复扫描批次数、核对命中（远端已存在且摘要匹配）、核对冲突、审计补发、事件补发（带 shard 标签）；在 trace span 上记录 batchID、输出序号、核对三态。
  - 结构化日志覆盖：批次创建/复用/完成/恢复、冲突阻断；日志禁止输出文件内容/PII（仅文件名/ID/摘要前缀）。
- **Acceptance Criteria Addressed**: AC-15、NFR-4
- **Test Requirements**:
  - `rule` TR-7.1: 新指标在对应路径被递增且 initializeShardMetrics 初始化标签不 panic（证据：单测/编译运行）。
  - `rubric` TR-7.2: 可观测性充分性；1-5；1=恢复黑盒；3=有日志无指标；5=日志/指标/trace 三者齐备且无 PII；阈值 >=4；证据：走查。

## Task 8: 全量验证与既有文档同步
- **Status**: `completed`
- **Completion Evidence**:
  - TR-8.1（rule）通过：`gofmt -l` 干净；`go build ./...`、`go vet ./...` 退出 0；`go test -skip <docker 集成测试> ./...` 全部 ok（-count=3 pipeline 稳定）；pipeline/upload/models 三个核心包 `go test -race` 通过；pipeline 包覆盖率 81.1%。Docker 依赖测试（TestRepository/TestFTP_/TestSFTP_/TestUploads/TestStreamErrors）与 TestAgent -count>1（全局 createdAgents 缓存隔离问题）已在 git stash 基线复现，属既有环境/测试基建问题。
  - TR-8.2（rule）通过：git diff 显示 go.mod/go.sum 零变更；所有新增测试只用 t.TempDir 与内存 mock，无网络/容器访问。
  - TR-8.3（rubric 自评 5/5）：改动集中于状态机外壳，合并/映射/通知/取消逻辑复用；[docs/concepts/events.md](file:///Users/huangding/Documents/new-SWE/0910142/project-02/docs/concepts/events.md) 已补 batchID 语义与恢复重发说明；Apache 头、错误包装、golint 注释齐备。
- **Priority**: high
- **Depends On**: Task 1-7
- **Description**:
  - 运行 `gofmt`/`go build ./...`/`go vet ./...`/`go test ./...`，并对 pipeline、upload、models 包执行 `go test -race`。
  - 核对覆盖率不低于当前基线；补强缺口用例（不降低任何阈值）。
  - 在既有 `docs/concepts/events.md`（若其描述 FileUploaded 字段）补充 batchID 语义与批次恢复说明；不新建文档文件。
  - 自查：无新增第三方依赖、无 Docker 依赖、Apache 头、golint 注释、错误包装不吞错。
- **Acceptance Criteria Addressed**: AC-13、AC-15、AC-16、NFR-2、NFR-5
- **Test Requirements**:
  - `rule` TR-8.1: 上述命令全部退出码 0（证据：实跑输出粘贴到 Completion Evidence）。
  - `rule` TR-8.2: `go.mod/go.sum` 无新依赖；测试不访问网络/容器（证据：断网或不启动 docker 下 `go test ./...` 通过）。
  - `rubric` TR-8.3: 变更完整性；1-5；1=存在半成品/无关改动；3=功能完整但风格不一；5=小而完整、风格一致、文档同步；阈值 >=4；证据：diff 总览走查。
