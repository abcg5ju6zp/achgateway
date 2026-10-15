# 截止窗口可恢复批次（Resumable Cutoff Batches）- 产品需求文档

## Overview
- **Summary**: 为每个分片截止窗口（shard cutoff window）引入固定的输入快照与批次身份，将合并产物、格式转换结果、FTP/SFTP 远端对象、审计记录与 FileUploaded 事件统一关联到该身份，并用磁盘上的批次清单（manifest）驱动一个可崩溃恢复、逐输出幂等的状态机；输入清理只在远端上传结果可确认后推进。
- **Purpose**: 消除"远端已收到但本地未记录 → 重启后重复上传同一资金文件"与"过早清理输入 → 无法补发"两类风险，保证合并、转换、上传、审计、事件在任意阶段失败或进程重启后都能从磁盘、审计与远端事实继续，且部分成功时只补未完成项。
- **Target Users**: 财务运维（依赖文件准时、不重复上传）、ACHGateway 服务运维（on-call 排障/补发）、事件下游（需要批次关联与幂等去重依据）。

## Goals
- 每个分片截止窗口有稳定、可复算的批次身份与输入快照。
- 合并产物、转换产物、远端对象、审计记录、FileUploaded 事件全部引用该批次身份。
- 每个合并输出独立记录各阶段事实（prepared → uploaded/verified → audited → events sent），任一阶段失败后只补该阶段。
- 远端结果不确定时，先以"目标路径 + 内容摘要（SHA-256）"核对再决定是否上传。
- 自动触发与手工触发同一窗口复用活动批次；重启后自动恢复。
- 部分输出成功时分别记录已完成与待恢复项，绝不整批重传。
- 输入隔离目录仅在批次完成（且满足既有保留期）后才可被清理。

## Non-Goals
- 不改变 QueueACHFile 的接收、去重（files 表唯一约束）、取消语义。
- 不引入数据库表/迁移：批次状态保存在既有 merging storage（Chest，本地文件系统或加密 Chest）上。
- 不改变 ODFI 下载（incoming/odfi）链路。
- 不改变文件名模板语法、合并条件（ach.Conditions）、GPG/base64 等转换本身的行为。
- 不引入新的第三方依赖、不要求 Docker 完成安装/测试/启动。
- 不实现 ODFI 侧远端文件删除/改名类冲突自动处置；冲突仅阻断告警。
- 不保证 FileUploaded 事件恰好一次（exactly-once）；维持至少一次 + 批次身份供下游去重。

## Background & Context
- 现状（`internal/pipeline`）：
  - `HandleXfer` 将输入写到 `mergable/<shard>/<fileID>.ach`（+.json ValidateOpts）。
  - 截止触发（`aggregator.withEachFile` 自动 / `manualCutoff` 手工）调用 `XferMerging.WithEachMerged`：先 `ReplaceDir` 把 `mergable/<shard>` 改名为 `<shard>-YYYYMMDD-HHMMSS`（按触发时刻命名，无持久身份），再 `ach.MergeDir`、保存 `uploaded/<sha>.ach` 缓存、逐输出回调 `runTransformers → uploadFile`。
  - `uploadFile` 当前顺序：渲染文件名（含 `time.Now()` 到秒）→ 格式化输出 → **先审计 SaveFile** → **后 UploadFile** → 通知；结果只存在于内存 `mergedFiles`，事件在全部上传后由 `emitFilesUploaded` 发送。
  - 失败/重启后：隔离目录成为孤儿，没有任何恢复机制；若重跑则可能重复上传，且渲染出的远端文件名会随秒级时间变化。
  - `CleanupService` 仅依据目录年龄 + 存在 `uploaded/` 且非空来删除目录。
- 已确认决策（用户）：
  1. 远端路径存在但摘要不一致 → **阻断并 critical 告警**，不覆盖、不删除。
  2. active 批次在**进程启动时 + 每次自动窗口/手工触发时**恢复；节假日窗口仍整体跳过。
  3. 旧批次恢复失败不阻塞新窗口处理，错误聚合告警。
- 远端核对能力：FTP/SFTP agent 均有 `ReadFile(ctx, fullPath)`（go-ftp/go-sftp 的 `Open`），可读取远端对象计算摘要，无需扩展 Agent 接口。

## Functional Requirements

- **FR-1 批次身份与快照**：截止处理开始时，按"分片 + 窗口（时区本地日期与 HH:MM）"生成确定性批次 ID 并固化输入快照（输入文件名、字节数、SHA-256）；手工触发无计划窗口时使用手工批次 ID。同一分片同一窗口的重复触发（自动或手工）必须复用已存在的活动批次，而不是再次隔离输入。
- **FR-2 阶段事实统一引用身份**：每个合并输出的合并缓存、转换产物（实际上传字节）、远端目标路径、审计路径与上传时间、每个输入的 FileUploaded 事件发送状态都记录在批次清单中并带批次 ID；FileUploaded 事件新增可选 `batchID` 字段。
- **FR-3 远端文件名首次固定**：远端文件名在该输出第一次 prepare 时按模板渲染一次并持久化；恢复/重试用同一文件名、同一路径、同一字节摘要。
- **FR-4 幂等上传与不确定响应核对**：上传前若清单已确认 uploaded/verified 则跳过实际上传；`UploadFile` 返回错误（响应不确定）时，读取 `OutboundPath/<filename>` 并比对 SHA-256：匹配则记为已上传（verified），不存在则保持待恢复并报错，摘要冲突则进入冲突态（见 FR-5）。
- **FR-5 冲突阻断**：目标路径存在但摘要不一致时，不覆盖、不删除、不再上传该输出，记录冲突错误并触发 critical 告警；批次保持 active，其余输出继续处理。
- **FR-6 审计可补发**：审计保存不再是上传前置条件；上传确认后审计失败必须保留已上传事实，恢复时仅对同一固定审计路径重放 `SaveFile`，不重新上传。
- **FR-7 事件可补发**：FileUploaded 按输入逐条记录 sent 标记；发送失败只重发未确认输入的事件，不重传文件、不重放审计；重发事件的 `UploadedAt` 与首次上传时间一致。
- **FR-8 部分成功分别记录**：以输出为粒度记录 prepared/uploaded/audited 与每输入事件状态；恢复时已完成输出与已发送事件一律跳过，只驱动待恢复项；禁止整批重传。
- **FR-9 重启/下轮恢复**：进程启动时扫描存储根目录，恢复所有 active 批次（从磁盘清单恢复状态、以审计存在性与远端路径+摘要核对事实）；之后每个银行日自动窗口与每次手工触发先恢复 active 批次再处理新窗口；恢复失败不阻塞新窗口。
- **FR-10 输入清理门控**：批次未完成（存在未上传/未审计输出或未发送事件的映射输入、或冲突态）时其隔离目录不得被清理；批次完成后仍遵守既有保留期策略。无 batch.json 的历史目录沿用现有"年龄 + uploaded/ 非空"判定。
- **FR-11 既有行为保持**：无待处理文件时空跑（隔离空目录后删除，不产生批次、事件、远端调用）；节假日窗口跳过并维持节日通知；一次成功的正常上传在外部表现上与现状一致（远端文件、审计副本、通知、FileUploaded 事件、指标）；已存在的合并/转换产物被复用而非重建。
- **FR-12 手工 API 兼容**：`/trigger-cutoff` 语义保持（同步返回每分片错误）；手工触发在存在活动批次时驱动其恢复；`/pipeline/{dir}/file-uploaded` 补发接口在批次存在时同步更新清单中的事件标记。

## Non-Functional Requirements

- **NFR-1 崩溃安全**：清单写入必须原子（临时文件 + 替换）；跨阶段边界持久化，保证任意时刻 kill -9 后状态可恢复且不重复上传。
- **NFR-2 向后兼容**：FileUploaded 新字段 omitempty；历史隔离目录无清单时保持现有清理行为；既有测试与管理 API 行为保持。
- **NFR-3 资金安全**：任何路径都不得在摘要不确认的情况下重复写远端，不得覆盖/删除远端已存在且摘要不符的对象。
- **NFR-4 可观测性**：恢复、核对命中、冲突、审计/事件补发有结构化日志、trace 属性与 Prometheus 计数；日志不含完整文件内容与账号等 PII（沿用现状）。
- **NFR-5 无新增依赖、无需 Docker**：仅用标准库与现有依赖；`go test ./...` 可直接运行，不依赖 FTP/SFTP/MySQL 容器（新增测试一律 mock/本地文件）。
- **NFR-6 规模**：保持 FileUploaded 发送并发上限（128）；清单保存不得按事件逐次 fsync（每轮批量持久化）。

## Constraints
- **Technical**: Go（标准 net/http 生态）、gofmt、Apache 头；Chest 接口（Open/Glob/ReadDir/ReplaceFile/ReplaceDir/MkdirAll/RmdirAll/WriteFile）为唯一批次状态存储；upload.Agent 接口不新增方法（用 ReadFile 核对）；FileUploaded 事件模型在 `pkg/models`；遵循 Moov AGENTS（小而完整、不做无关重构、表驱动测试 + testify）。
- **Business**: ACH 资金文件不得重复发送、不得静默丢失或覆盖；告警通道沿用现有 alerters/notifications。
- **Dependencies**: `github.com/moov-io/go-ftp`、`github.com/moov-io/go-sftp`（Client.Open/Reader/ListFiles），`moov-io/ach` MergeDir 行为不变。

## Assumptions
- 同一远端文件名在 ODFI 侧只由本分片写入（模板含分片名/路由号/日期秒/序号）；路径+摘要可作为"远端已收到本文件"的充分依据。
- 审计存储 `SaveFile` 对同路径是幂等覆盖写；重放审计无副作用。
- 事件下游可依据（fileID, batchID, filename）对至少一次投递做幂等。
- 单进程内分片聚合循环是单 goroutine 串行触发；跨进程的 active 批次竞争不在本次范围（沿用 files 表接收去重，不新增分布式锁）。
- 手工触发复用"当前任意 active 批次中最新者"；不同窗口各自拥有确定性批次 ID，完成后互不影响。

## Acceptance Criteria

### AC-1: 批次身份确定性与同窗口复用
- **Type**: `rule`
- **Given**: 分片配置了窗口 15:00（America/New_York），mergable 目录有输入文件
- **When**: 15:00 自动窗口触发，随后对同一分片手工触发（批次尚未完成），或在窗口时刻以任一方式触发两次
- **Then**: 两次执行使用同一批次 ID（`<shard>-YYYYMMDD-HHMM`）与同一份输入快照，只发生一次目录隔离；手工无活动批次时使用 `<shard>-manual-YYYYMMDD-HHMMSS`
- **Pass Condition**: 磁盘上只有一个该窗口的隔离目录与 batch.json，清单 ID/输入摘要两次一致；第二次执行不重新隔离 mergable
- **Evidence**: 批次恢复单测（同窗口复用断言、mergable 目录仍可接收新文件断言）+ batch.json 内容断言

### AC-2: 输入快照固定
- **Type**: `rule`
- **Given**: 窗口触发时 mergable 目录有 N 个非取消输入
- **When**: 批次创建
- **Then**: batch.json 记录每个输入的相对文件名、字节数、SHA-256 与取消排除结果；批次处理期间新增到 mergable 的文件不进入该批次
- **Pass Condition**: 快照列表与触发瞬间磁盘文件逐一匹配（含摘要），处理中写入的新文件留待下一窗口
- **Evidence**: 单测：触发后向 mergable 再写文件，清单不含该文件，且新文件仍在下一窗口被隔离

### AC-3: 五类产物引用批次身份且远端文件名首次固定
- **Type**: `rule`
- **Given**: 批次含至少一个合并输出
- **When**: 完成 prepare/upload
- **Then**: 清单记录批次 ID、输出序号、合并缓存路径与摘要、转换产物路径与字节摘要、远端文件名/完整目标路径、审计路径、UploadedAt；实际发出的 FileUploaded 事件 `batchID` 等于清单 ID
- **Pass Condition**: 恢复后重新渲染得到的文件名与清单首次记录完全一致；事件 JSON 含相同 batchID
- **Evidence**: 单测断言清单字段、事件字段；恢复前后文件名/摘要不变

### AC-4: 不确定上传响应按路径+摘要核对
- **Type**: `rule`
- **Given**: 某输出首次 `UploadFile` 返回错误，但 mock 远端 ReadFile 能在目标路径返回与转换产物相同的字节
- **When**: 同轮错误处理或下一轮恢复执行
- **Then**: 输出标记为 uploaded+verified，不发生第二次 UploadFile；审计与事件继续推进
- **Pass Condition**: mock agent 的 UploadFile 调用计数为 1，ReadFile 被调用且摘要匹配；清单 uploaded/verified 为真
- **Evidence**: 单测（上传错误 + 远端同摘要）断言调用计数与清单状态

### AC-5: 远端缺失时保持待恢复
- **Type**: `rule`
- **Given**: UploadFile 返回错误且 ReadFile 表明目标路径不存在
- **When**: 本轮与恢复轮执行
- **Then**: 输出保持未上传、批次保持 active、错误上报告警；下一轮重新上传成功
- **Pass Condition**: 首轮 UploadFile 计 1 次且整体返回错误；恢复轮 ReadFile 缺失后再 UploadFile 成功，总上传 2 次（不同轮），最终批次完成
- **Evidence**: 单测两轮驱动断言状态转换

### AC-6: 摘要冲突阻断且不覆盖
- **Type**: `rule`
- **Given**: 目标路径存在但字节摘要与清单不一致
- **When**: 上传或恢复核对
- **Then**: 不调用覆盖性上传/删除，输出与批次保持 active/冲突，错误通过 alerter 走 critical；其余输出不受影响
- **Pass Condition**: mock 记录 0 次上传/删除、1 次 ReadFile；返回哨兵冲突错误；critical 通知被触发；批次未完成
- **Evidence**: 单测断言无写操作、冲突错误类型、告警调用

### AC-7: 审计失败只补审计
- **Type**: `rule`
- **Given**: 某输出远端上传已确认，随后 SaveFile 失败
- **When**: 恢复执行（SaveFile 恢复成功）
- **Then**: 不重新上传（UploadFile 0 次），仅向同一固定审计路径 SaveFile 一次，输出转为 audited
- **Pass Condition**: 调用计数与清单状态符合；审计路径跨轮不变
- **Evidence**: 单测（计数 mock audit storage）

### AC-8: 事件失败只补事件
- **Type**: `rule`
- **Given**: 两个输入合并到一个已上传输出，首轮事件发送全部失败
- **When**: 恢复轮事件发送成功
- **Then**: 不发生上传/审计调用；每个输入恰好补发一次 FileUploaded（同 batchID、同 UploadedAt、同 filename）；清单 event_sent 转真
- **Pass Condition**: emitter 首轮 2 失败、恢复轮 2 成功，总计成功 2 条无重复成功发送；UploadFile 全程 1 次
- **Evidence**: 单测用可切换错误的 emitter 断言事件内容与计数

### AC-9: 部分成功逐输出恢复、禁止整批重传
- **Type**: `rule`
- **Given**: 一个批次产生 3 个输出，第 2 个上传失败，其余成功
- **When**: 首轮结束后执行恢复
- **Then**: 输出 1、3 不再上传（可被远端核对跳过），仅输出 2 走核对/上传；3 个输出最终完成
- **Pass Condition**: 恢复轮 UploadFile 只针对输出 2（其余以已记录事实或一次只读核对跳过）；任何输出都不被第二次成功上传
- **Evidence**: 单测：每输出独立计数 agent，断言恢复前后总成功上传次数恰好为 3

### AC-10: 重启后从磁盘/审计/远端事实继续
- **Type**: `rule`
- **Given**: 首轮在不同阶段制造崩溃（用"新 merger/新 aggregator 实例 + 同一 Chest 目录"模拟进程重启），分别停在 prepared 后、uploaded 后审计前、audited 后事件前
- **When**: 新实例启动并执行恢复
- **Then**: 从 batch.json 装载状态，复用产物字节，结合审计存在性与远端 ReadFile 核对续跑至完成；全过程无重复上传
- **Pass Condition**: 三种崩溃点均最终 completed 且每输出上传 1 次；审计、事件各补齐一次
- **Evidence**: 单测三种崩溃注入表驱动用例

### AC-11: 启动与每轮自动恢复、节假日保持跳过
- **Type**: `rule`
- **Given**: 存储中存在 active 批次
- **When**: aggregator Start；以及银行日窗口触发；以及节假日窗口触发
- **Then**: Start 时执行一次恢复；银行日窗口先恢复旧批次再处理新窗口；旧批次恢复失败时新窗口仍处理并聚合错误；节假日窗口不执行恢复或处理，仅维持节日通知
- **Pass Condition**: 单测/可测编排断言恢复入口调用顺序；节假日路径零批次操作
- **Evidence**: 编排级单测（可注入 clock/merger 记录调用次序）

### AC-12: 清理门控
- **Type**: `rule`
- **Given**: 超过保留期的隔离目录分别为：active 批次、completed 批次、无 batch.json 的历史目录（uploaded/ 非空/为空）
- **When**: CleanupService 运行
- **Then**: 仅 completed 批次目录与符合旧规则的历史目录被删除；active 与无 uploaded 的历史目录保留
- **Pass Condition**: 四种目录删除/保留结果正确
- **Evidence**: cleanup 单测扩展

### AC-13: 既有行为保持
- **Type**: `rule`
- **Given**: 四种场景：无待处理文件、节假日、现有合并/转换产物已存在、全部一次成功
- **When**: 触发窗口
- **Then**: 空输入不产生 batch.json/事件/远端调用并删除空隔离目录；现有产物被复用；成功路径产生与现状一致的远端文件、审计副本、通知、每输入一条 FileUploaded 与指标
- **Evidence**: 既有 merging/aggregate 测试全部通过（允许因接口签名演进做机械化适配，不断言弱化）；新增空跑/复用单测

### AC-14: FileUploaded 事件向后兼容
- **Type**: `rule`
- **Given**: 不含 batchID 的旧 JSON 事件
- **When**: models.Read 解析
- **Then**: 正常解析且 BatchID 为空；新事件 omitempty，空值时 JSON 不出现该字段
- **Pass Condition**: 往返（round-trip）单测通过；openapi/admin API 无需破坏式变更
- **Evidence**: pkg/models 事件单测

### AC-15: 质量与可观测性（恢复机制工程质量）
- **Type**: `rubric`
- **Dimension**: 崩溃安全、并发安全、可观测性与代码贴合度
- **Scale**: 1-5
- **Anchors**: 1 = 状态仅存内存、无原子写、无日志指标、竞态风险；3 = 状态持久但部分阶段边界缺失或日志不足；5 = 阶段边界完整、原子写、互斥清晰、关键路径有日志/trace/指标且无 PII
- **Pass Threshold**: >= 4
- **Evidence**: 代码走查 + race 测试（`go test -race` 抽测 pipeline 包）+ 指标/日志检查

### AC-16: 测试与构建门禁
- **Type**: `rule`
- **Given**: 变更完成
- **When**: 运行 `go build ./...`、`go vet ./...`、`go test ./...`
- **Then**: 全部通过且不需要 Docker/网络服务；新增逻辑被表驱动单测覆盖；覆盖不低于当前基线（不降低阈值）
- **Pass Condition**: 命令本地实跑输出成功；测试包含全部 FR 分支（成功/失败/不确定/冲突/重启/部分）
- **Evidence**: 命令输出与测试文件清单
