# Remote Hosts 产品问题清单

> 唯一事实源：`docs/product/backlog.json`。本页由 `scripts/product-backlog.py --render` 生成。

更新日期：2026-09-11。共 50 项。

已验证候选不等于线上修复；部分修复不能关闭整项。关闭必须附本项验收证据。

状态汇总：未修复 11；部分修复 19；候选已验证 12；外部阻塞 1；已验收关闭 7。

## 版本规划

**0.3.0**：已验证候选的真实发布和现场验收，不扩展成另一轮大功能重写

**0.3.1**：使用体验与可靠性：文件源交接、可观测错误、调度公平、读取改进和验收自动化

**0.3.2**：补齐Studio发布闭环、修正验收摘要、宿主能力漂移提示与调度/观察体验

**0.4.0**：双向持久续传、授权刷新、取消、工作上下文与任务恢复

**0.5.0**：协作效率与交付闭环：按目标独立发布、统一观察和错误耗时、完整变更集、工作交接、批量文件集/存储生命周期、宿主能力与返回兼容；详见docs/product/ROADMAP-0.5.0.md

**0.3.3**：资源感知调度、完整状态恢复、精确观察预算与固定输入验证；当前只发布验收NAS和MacBook，Studio延后

**0.3.4**：持久结果回执补送、投递状态与有界恢复；验证后直接构建发布，不等待逐版确认

**0.3.5**：量化并降低隔离快照的重复编译成本；补齐发布链路与回执恢复入口

**0.3.6**：隔离验证强制中断后的构建清理；启动文件跨进程恢复主线，避免继续扩大构建框架

**0.4.1**：恢复控制、授权刷新与回执代际、准确取消进度、范围下载、工作区分页、发布输入/原回执门禁

## 问题索引

| ID | 优先级 | 状态 | 目标版本 | 问题 |
|---|---|---|---|---|
| RH-001 | P0 | 候选已验证 | 0.3.0 | Agent 串行执行阻塞读取和终端 |
| RH-002 | P0 | 候选已验证 | 0.3.0 | Gateway 全局单传输锁 |
| RH-003 | P0 | 候选已验证 | 0.3.0 | PTY 输入堵塞时状态与取消也被锁住 |
| RH-004 | P0 | 候选已验证 | 0.3.0 | 固定180秒总期限误杀持续慢传输 |
| RH-005 | P1 | 候选已验证 | 0.3.0 | 长任务无阶段与字节进度 |
| RH-006 | P1 | 候选已验证 | 0.3.0 | 上传全程占工作区写锁 |
| RH-007 | P0 | 部分修复 | 0.4.0 | 双向且跨进程的持久化断点续传 |
| RH-008 | P0 | 未修复 | 0.3.1 | 原生file_upload偶发file source lookup failed |
| RH-009 | P1 | 部分修复 | 0.4.1 | 传输重试状态机与错误结果永久done |
| RH-010 | P1 | 部分修复 | 0.4.1 | 短期文件URL到期后重新授权恢复 |
| RH-011 | P1 | 部分修复 | 0.4.0 | 独立文件任务取消及清理证明 |
| RH-012 | P1 | 部分修复 | 0.3.3 | 等待资源的任务占满同类执行名额 |
| RH-013 | P1 | 部分修复 | 0.3.1 | 阻塞线程与逃逸后代的回收边界 |
| RH-014 | P1 | 部分修复 | 0.4.1 | 进度口径、停滞速率和重试代际 |
| RH-015 | P1 | 部分修复 | 0.4.1 | 统一workspace_context和事件恢复游标 |
| RH-016 | P1 | 部分修复 | 0.3.2 | 短命令与观察操作的多层轮询 |
| RH-017 | P1 | 未修复 | 0.3.1 | 分层错误码与耗时追踪 |
| RH-018 | P2 | 未修复 | 0.4.0 | 完整JSON在文本和structuredContent中重复 |
| RH-019 | P2 | 已验收关闭 | 0.3.1 | 同文件多范围重复读取和计算哈希 |
| RH-020 | P1 | 已验收关闭 | 0.3.1 | 超长单行读取无法推进及部分读取可用性 |
| RH-021 | P2 | 未修复 | 0.5.0 | 搜索分页与语法范围缺少增量缓存 |
| RH-022 | P1 | 候选已验证 | 0.3.3 | Store.list固定1000上限没有完整性提示 |
| RH-023 | P1 | 未修复 | 0.4.0 | 过期对象与崩溃staging的清理/容量 |
| RH-024 | P1 | 部分修复 | 0.3.4 | 任务回执outbox及dispatch会话fencing |
| RH-025 | P1 | 部分修复 | 0.3.1 | 源码/构建/已安装/实际运行版本未统一 |
| RH-026 | P1 | 已验收关闭 | 0.3.3 | 验证证据自动生成并绑定源码 |
| RH-027 | P0 | 已验收关闭 | 0.3.5 | 完成真实发布、运行构建确认和回滚记录 |
| RH-028 | P1 | 已验收关闭 | 0.3.5 | 验收脚本固定0.2.0及跨版本复用回执 |
| RH-029 | P1 | 部分修复 | 0.3.2 | 升级执行器与被升级Agent相互依赖 |
| RH-030 | P0 | 外部阻塞 | 0.3.3 | 工具平台安全拦截的可见性与授权边界 |
| RH-031 | P1 | 未修复 | 0.3.1 | CI故障注入、跨平台和原生网页验收门禁 |
| RH-032 | P2 | 未修复 | 0.5.0 | 快照和重复hash读取成本、内容寻址缓存 |
| RH-033 | P2 | 未修复 | 0.5.0 | 64 MiB限制提高与动态容量协商 |
| RH-034 | P1 | 未修复 | 0.3.1 | 过程反馈与产品负责人使用纪律 |
| RH-035 | P1 | 部分修复 | 0.3.2 | 工具与协议能力发现、schema变更兼容 |
| RH-036 | P1 | 未修复 | 0.4.0 | 源码脚本与安全策略的真实边界 |
| RH-037 | P1 | 未修复 | 0.4.0 | 多文件改动的部分失败和恢复记录 |
| RH-038 | P1 | 已验收关闭 | 0.3.5 | 构建资源竞争与长编译无进展反馈 |
| RH-039 | P1 | 部分修复 | 0.4.0 | 任务取消/终端结束和清理结果混淆 |
| RH-040 | P2 | 部分修复 | 0.4.1 | SQLite热点及轮询调度索引精修 |
| RH-041 | P1 | 部分修复 | 0.3.1 | NAS 未提供 SFTP 子系统导致默认 SCP 发布传输失败 |
| RH-042 | P0 | 部分修复 | 0.3.2 | Agent升级门禁缺少网关回连确认和本机超时自动回退 |
| RH-043 | P1 | 部分修复 | 0.3.1 | 升级预检依赖隐式Python与SQLite运行环境 |
| RH-044 | P0 | 候选已验证 | 0.3.2 | 更新器重复触发导致相同版本被反复安装和Agent强制重启 |
| RH-045 | P1 | 候选已验证 | 0.3.3 | 批量观察精确预算与单项结果不可用隔离 |
| RH-046 | P1 | 已验收关闭 | 0.3.5 | 升级预检吞掉底层错误且单次临时失败中断发布 |
| RH-048 | P1 | 候选已验证 | 0.5.0 | 已部署的新核心源码尚未纳入Git交付基线 |
| RH-049 | P1 | 候选已验证 | 0.5.0 | code_diff未明确未跟踪文件的审查缺口 |
| RH-050 | P1 | 候选已验证 | 0.5.0 | 缺少有清单、预览与逐文件回执的批量文件集同步 |
| RH-047 | P0 | 部分修复 | 0.4.2 | 发布等待器缺少构建任务身份核对，启动确认被误当作交付进展 |

## 逐项验收

### RH-001 · Agent 串行执行阻塞读取和终端

**P0 / 候选已验证 / 0.3.0**

现象与范围：旧版 cycle 等待文件任务结束才继续 poll；用户已遇到传输时终端排队。

当前处理：五通道与任务租约已通过本机测试；待新版实际运行链路验收。

验收：两台机器同时传输时，代码读取、终端启动仍能完成；重试不重复写入。

证据或实现位置：`crates/remote-hosts-code/tests/concurrency.rs`、`docs/iteration-0.3.0-verification.json`

依赖：无

### RH-002 · Gateway 全局单传输锁

**P0 / 候选已验证 / 0.3.0**

现象与范围：一条接收流曾占有整个网关 Mutex，影响其他设备。

当前处理：候选改为全局4、每设备2；配额先预留，网络阶段不持配额锁。

验收：挂起设备A接收流，B能完成接收；上限及并发磁盘预留均生效。

证据或实现位置：`crates/remote-hosts-code/src/scheduler.rs`、`crates/remote-hosts-code/tests/concurrency.rs`

依赖：无

### RH-003 · PTY 输入堵塞时状态与取消也被锁住

**P0 / 候选已验证 / 0.3.0**

现象与范围：同步输入曾持有 live-map 全局锁，取消还可能排在输入通道之后。

当前处理：候选有独立control通道；输入仅持有自身writer锁。

验收：真实PTY不消费输入时，查询与取消在受控测试窗口内返回，不波及其他终端。

证据或实现位置：`crates/remote-hosts-code/tests/iteration_reliability.rs`

依赖：无

### RH-004 · 固定180秒总期限误杀持续慢传输

**P0 / 候选已验证 / 0.3.0**

现象与范围：用户报告约28 MB传输失败；源码固定总期限是已确认风险，不声称唯一根因。

当前处理：候选替换为连接/DNS/idle限制，发送方向观察body活动与对端进度。

验收：持续有进展的传输可超过180秒；停滞会在idle预算内报错；端到端慢链路仍需验收。

证据或实现位置：`crates/remote-hosts-code/src/resumable.rs`、`crates/remote-hosts-code/src/transfer_watch.rs`

依赖：无

### RH-005 · 长任务无阶段与字节进度

**P1 / 候选已验证 / 0.3.0**

现象与范围：旧版只暴露pending，无法区分排队、传输、校验和停滞。

当前处理：候选有进度快照及新鲜度，不再让模型每次猜测。

验收：operation_get能观察实际接收字节、阶段、重试和新鲜度，不能伪造完成。

证据或实现位置：`crates/remote-hosts-code/src/progress.rs`、`crates/remote-hosts-code/tests/iteration_reliability.rs`

依赖：无

### RH-006 · 上传全程占工作区写锁

**P1 / 候选已验证 / 0.3.0**

现象与范围：慢网络期间同项目文件编辑被锁住。

当前处理：候选只在最终发布时获取写锁、重新校验版本。

验收：传输期间可编辑其他文件；同一目标并发改动在发布时被版本冲突拒绝。

证据或实现位置：`crates/remote-hosts-code/src/agent.rs`、`crates/remote-hosts-code/src/transfers.rs`

依赖：无

### RH-007 · 双向且跨进程的持久化断点续传

**P0 / 部分修复 / 0.4.0**

现象与范围：当前仅外部源到设备、存活操作内可Range恢复；导出完整POST重试，重启丢恢复状态。

当前处理：0.4.0新增双向持久检查点、原任务恢复、发布日志和接收端确认偏移；独立进程故障回归及三端部署通过。保留64MiB上限、24小时检查点保留和明确授权恢复边界。 0.4.1补充网关If-Range条件回退及后缀范围实现，回归通过；该版对应现场范围测试尚未执行。

验收：双向在4 MiB检查点中断并重启双方，恢复同一任务；最多重传未确认片段；最终SHA一致，副作用不重复。

证据或实现位置：`crates/remote-hosts-code/src/resumable.rs`、`crates/remote-hosts-code/src/transfers.rs`、`docs/iteration-0.3.0-2026-09-10.md`、`docs/releases/0.4.0/verification-q1.json`、`docs/releases/0.4.0/deployment.json`、`docs/releases/0.4.0/workflow-acceptance-q1.json`

依赖：RH-009

### RH-008 · 原生file_upload偶发file source lookup failed

**P0 / 未修复 / 0.3.1**

现象与范围：已有0.2.0网页附件验收成功，但后续生成源码文件上传再次出现lookup失败；根因未定。

当前处理：保留复现操作ID、平台文件交接、源取回HTTP阶段和授权时效，不记录签名URL或密钥。

验收：新对话上传、生成文件上传、延迟领取、精确重试均往返校验；定位失败阶段并返回可操作原因。

证据或实现位置：`docs/chatgpt-file-transfer-release-0.2.0.md`、`docs/iteration-0.3.0-2026-09-10.md`

依赖：无

### RH-009 · 传输重试状态机与错误结果永久done

**P1 / 部分修复 / 0.4.1**

现象与范围：可恢复传输错误当前也作为done结果；同一幂等键只返回旧失败，不能刷新授权继续。

当前处理：0.4.1短控制事务提前取得写意向；8路同幂等恢复只形成一个代际，避免读转写SQLITE_BUSY。未来代际回执不再被当作过期成功确认，缺少代际结果不能覆盖新尝试。全回归通过，NAS/MacBook已安装，Studio未安装。

验收：临时故障恢复同一任务；确定失败不可盲重放；发布后丢回执只补回执。

证据或实现位置：`crates/remote-hosts-code/src/agent.rs`、`docs/releases/0.4.0/verification-q1.json`、`docs/releases/0.4.0/deployment.json`、`docs/releases/0.4.0/workflow-acceptance-q1.json`、`docs/releases/0.4.1/verification.json`、`docs/releases/0.4.1/deployment.json`、`docs/releases/0.4.1/incidents.json`

依赖：无

### RH-010 · 短期文件URL到期后重新授权恢复

**P1 / 部分修复 / 0.4.1**

现象与范围：文件授权链接与网关对象均会过期，当前传输没有完整刷新协议。

当前处理：0.4.1精确恢复重试允许刷新同代授权URL但不创建新任务/代际；已取消/已完成或被后代取代时不覆盖新授权。新旧授权隔离测试通过；综合现场门禁未运行，不能据此关闭。

验收：链接过期后用新授权恢复原任务；换源内容被拒绝；报告needs_authorization而不是含糊失败。

证据或实现位置：`crates/remote-hosts-code/src/gateway.rs`、`crates/remote-hosts-code/src/transfers.rs`、`docs/releases/0.4.0/verification-q1.json`、`docs/releases/0.4.0/deployment.json`、`docs/releases/0.4.0/workflow-acceptance-q1.json`、`docs/releases/0.4.1/verification.json`、`docs/releases/0.4.1/deployment.json`、`docs/releases/0.4.1/incidents.json`

依赖：RH-009

### RH-011 · 独立文件任务取消及清理证明

**P1 / 部分修复 / 0.4.0**

现象与范围：目前只有terminal_cancel，文件传输不能独立取消。

当前处理：新增独立transfer_cancel，暂停任务也可以请求取消；只有本地及必要远端清理确认后才返回cleanup_complete。两台Mac暂停取消场景通过；已经提交的发布优先于迟到取消。

验收：取消不影响其他任务；已发布结果不倒退；临时空间回收可验证；取消与重试竞态可重现。

证据或实现位置：`crates/remote-hosts-code/src/tools.rs`、`docs/releases/0.4.0/verification-q1.json`、`docs/releases/0.4.0/deployment.json`、`docs/releases/0.4.0/workflow-acceptance-q1.json`

依赖：RH-009

### RH-012 · 等待资源的任务占满同类执行名额

**P1 / 部分修复 / 0.3.3**

现象与范围：通道许可先于资源锁取得；同根请求可占尽写槽阻塞兄弟项目。

当前处理：修复等待资源先占执行槽，并在网关领取前过滤同根/别名工作区和忙终端输入；完成通知加速队列恢复。真实Agent/Gateway测试中24个同根及别名积压任务不再挡住独立项目，释放后原8秒恢复窗口通过。完整快照215项通过，release构建被宿主拦截，尚未上线；queue/resource_wait分段耗时和持续加权公平性仍保留。

验收：项目A密集同根写任务不使项目B饥饿；记录queue/resource_wait时间。

证据或实现位置：`crates/remote-hosts-code/src/agent.rs`、`crates/remote-hosts-code/src/scheduler.rs`、`docs/releases/0.3.3/RELEASE.md`、`docs/releases/0.3.3/verification-g1.json`、`docs/releases/0.3.3/source-archive.json`、`docs/releases/0.3.3/deployment.json`

依赖：无

### RH-013 · 阻塞线程与逃逸后代的回收边界

**P1 / 部分修复 / 0.3.1**

现象与范围：移出全局锁并非使阻塞线程可取消；setsid后代可保留输入输出句柄。

当前处理：引入有界输入队列、超时与OS级生命周期管理，保留“非shell沙箱”的真实能力描述。

验收：填满输入、遗留输出句柄、取消和升级场景不无限积累线程/句柄；跨平台分别验收。

证据或实现位置：`crates/remote-hosts-code/src/terminal.rs`、`docs/iteration-0.3.0-2026-09-10.md`

依赖：无

### RH-014 · 进度口径、停滞速率和重试代际

**P1 / 部分修复 / 0.4.1**

现象与范围：候选已有字节/速率；接收端存储快照可能冻结，重试后计时重置，written并不等于durable_ack。

当前处理：修复取消结果被外层写成completed进度，候选回归通过并随NAS/MacBook发布。confirmed_bytes变化推进游标在0.4.0已存在，本轮仅补回归。取消现场验收本次未完成。

验收：慢流、零速、重试、校验、发布时字段一致；停滞速率归零；done和进度不可混淆。

证据或实现位置：`crates/remote-hosts-code/src/progress.rs`、`crates/remote-hosts-code/src/gateway.rs`、`crates/remote-hosts-code/src/transfers.rs`、`docs/releases/0.4.0/verification-q1.json`、`docs/releases/0.4.0/deployment.json`、`docs/releases/0.4.0/workflow-acceptance-q1.json`、`docs/releases/0.4.1/verification.json`、`docs/releases/0.4.1/deployment.json`、`docs/releases/0.4.1/incidents.json`

依赖：无

### RH-015 · 统一workspace_context和事件恢复游标

**P1 / 部分修复 / 0.4.1**

现象与范围：换对话/切Codex需重复读取历史；已有旧MCP上下文模型可复用，新网页入口未接通。

当前处理：0.4.1增加活跃过滤、状态计数、终端keyset分页及绑定工作区/过滤器的游标。MacBook只读现场3页6个唯一终端ID、active_only和汇总通过，授权已撤销；不是完整Git/对话上下文或事件日志。

验收：一次读取恢复任务/改动/证据/阻塞；跨对话不泄漏其他项目；断连不丢完成事件。

证据或实现位置：`crates/remote-hosts-mcp/src/lib.rs`、`crates/remote-hosts-code/src/tools.rs`、`docs/releases/0.4.0/verification-q1.json`、`docs/releases/0.4.0/deployment.json`、`docs/releases/0.4.0/workflow-acceptance-q1.json`、`docs/releases/0.4.1/verification.json`、`docs/releases/0.4.1/deployment.json`、`docs/releases/0.4.1/incidents.json`、`docs/releases/0.4.1/context-readonly-macbook.json`

依赖：无

### RH-016 · 短命令与观察操作的多层轮询

**P1 / 部分修复 / 0.3.2**

现象与范围：exec可能pending，read又pending，额外往返；code_read也多次需要operation_get。

当前处理：已上线最多20项批量operation_get、5秒有界等待和状态指纹，旧单ID保持兼容。线上只读验证顺序、原结果、重复ID拒绝与不变游标通过；仍非完整事件回放或统一终端观察，精确预算等后续见RH-045。

验收：短命令尽量一次返回退出码与首屏；长任务不重派发；同终端增量输出不漏不重复。

证据或实现位置：`crates/remote-hosts-code/src/gateway.rs`、`crates/remote-hosts-code/src/tools.rs`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`

依赖：无

### RH-017 · 分层错误码与耗时追踪

**P1 / 未修复 / 0.3.1**

现象与范围：tool_failed消息不足以区分宿主拦截、排队、网络、Agent执行、回执失败。

当前处理：统一error_code/stage/retryable/outcome/recovery/trace_id及queue/lock/execute/return计时；未知阶段标unknown。

验收：所有常见失败都有下一动作；日志关联同一操作且不泄漏命令敏感值；不能把平台拦截记成测试失败。

证据或实现位置：`crates/remote-hosts-code/src/agent.rs`、`crates/remote-hosts-code/src/gateway.rs`

依赖：无

### RH-018 · 完整JSON在文本和structuredContent中重复

**P2 / 未修复 / 0.4.0**

现象与范围：call_tool将相同完整结果返回两份，潜在增加模型上下文负担。

当前处理：定义outputSchema，结构化主体+短文本摘要；保留实际宿主兼容适配。

验收：网页和Codex均能完整读取字段；量化返回字节与实际token，未测前不宣称减半。

证据或实现位置：`crates/remote-hosts-code/src/gateway.rs`

依赖：无

### RH-019 · 同文件多范围重复读取和计算哈希

**P2 / 已验收关闭 / 0.3.1**

现象与范围：code_read每个range重复打开整文件、hash和拆行。

当前处理：0.3.1按同批路径复用文本与哈希；MacBook现场20个范围对应1次物理读取和19次缓存命中，版本与错误回归通过。

验收：同批多范围版本一致；并发编辑有明确冲突；文件读次数与唯一文件数一致。

证据或实现位置：`crates/remote-hosts-code/src/files.rs`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.1/features-macbook.json`

依赖：无

关闭证据：`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.1/features-macbook.json`

### RH-020 · 超长单行读取无法推进及部分读取可用性

**P1 / 已验收关闭 / 0.3.1**

现象与范围：单行超过max_bytes时可能反复返回相同next_line；批量一项失败使整体丢失结果。

当前处理：0.3.1支持UTF-8片段游标和预期版本约束；77,005字节含中文emoji文本分3页完整重建，坏路径不丢弃allow_partial批次内的成功范围。

验收：大于64 KiB单行能分段读完；合法范围不因另一坏路径消失；UTF-8边界正确。

证据或实现位置：`crates/remote-hosts-code/src/files.rs`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.1/features-macbook.json`

依赖：无

关闭证据：`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.1/features-macbook.json`

### RH-021 · 搜索分页与语法范围缺少增量缓存

**P2 / 未修复 / 0.5.0**

现象与范围：反复搜索/列举/符号查看会重复扫描，分页跨编辑没有固定快照。

当前处理：先采样定位瓶颈，再实现版本缓存、索引失效和游标冲突；不先引入庞大LSP栈。

验收：大仓库连续页无重复漏项或明确snapshot_changed；缓存不返回旧版本。

证据或实现位置：`crates/remote-hosts-code/src/files.rs`

依赖：无

### RH-022 · Store.list固定1000上限没有完整性提示

**P1 / 候选已验证 / 0.3.3**

现象与范围：恢复/清理调用可能误将前1000条当全部；读操作也持续写历史任务。

当前处理：新增有界keyset分页，旧list超过1000项明确报截断；终端重启以单条SQL恢复全部running/starting状态，不再依赖历史前缀。1105条记录完整分页与末尾活动终端恢复测试通过；未删除幂等回执，日志/清理保留策略没有擅自更改。候选通过，尚未部署。

验收：超过1000条历史仍完整恢复活跃终端/任务；清理不让旧副作用被再执行。

证据或实现位置：`crates/remote-hosts-code/src/store.rs`、`docs/releases/0.3.3/RELEASE.md`、`docs/releases/0.3.3/verification-g1.json`、`docs/releases/0.3.3/source-archive.json`、`docs/releases/0.3.3/deployment.json`

依赖：无

### RH-023 · 过期对象与崩溃staging的清理/容量

**P1 / 未修复 / 0.4.0**

现象与范围：当前惰性清理，原始日志有长期保留；部分跨进程staging可能遗留；慢活跃对象不能被清理误删。

当前处理：显式保留策略、活动租约保护、字节/对象/临时文件配额、可预览GC。

验收：空闲后可回收过期对象；活跃慢传输不误删；断电遗留可识别；隐私说明不承诺物理擦除。

证据或实现位置：`crates/remote-hosts-code/src/transfers.rs`、`crates/remote-hosts-code/src/store.rs`

依赖：RH-007

### RH-024 · 任务回执outbox及dispatch会话fencing

**P1 / 部分修复 / 0.3.4**

现象与范围：回执传输失败依赖再派发取回结果；旧会话检查已有改进，但跨实例/迟到结果与公平恢复仍需加强。

当前处理：本轮实现最终结果/待发回执同事务持久化、独立有界发送、退避重试、显式accepted确认、设备/网关绑定、旧租约确认隔离，以及队列数量和新鲜度上报。10项新回归与完整225项测试通过，两平台发布包已构建。仅补送保存结果，不重执行；完整会话fencing、blocked恢复入口、长期队列容量和生产故障矩阵仍待完成，未部署不标关闭。

验收：故障注入断连/网关重启/会话切换/回执丢失，无重复副作用且最终结果可取。

证据或实现位置：`crates/remote-hosts-code/src/agent.rs`、`crates/remote-hosts-code/src/gateway.rs`、`docs/releases/0.3.4/RELEASE.md`、`docs/releases/0.3.4/verification-h1.json`、`docs/releases/0.3.4/deployment.json`

依赖：无

### RH-025 · 源码/构建/已安装/实际运行版本未统一

**P1 / 部分修复 / 0.3.1**

现象与范围：包版本不能说明当前进程对应哪份未提交源码；候选经常被误当线上已升级。

当前处理：0.3.1构建manifest、源码验证、NAS运行哈希、MacBook安装哈希/PID/控制面就绪和功能回执已经关联。Studio升级结果和长期统一build_id仍待核对。

验收：任意时刻能区分候选、已测试、已安装、已运行；源码变化令旧验证过期。

证据或实现位置：`docs/iteration-0.3.0-verification.json`、`scripts/upgrade-code-agent.py`、`scripts/upgrade-code-gateway.py`、`docs/releases/0.3.0/macbook-recovery-confirmation.json`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`

依赖：无

### RH-026 · 验证证据自动生成并绑定源码

**P1 / 已验收关闭 / 0.3.3**

现象与范围：0.3.0已有手动生成的机器回执；缺少统一check runner持续生成与失效机制。

当前处理：声明输入范围内的快照与验证流程已实际跑通：115个独立文件、215项完整回归，包含此前未执行的9项Python快照测试；结束后核对全部输入/可执行属性，归档重新打开逐文件验hash通过。修改、增删、复制竞争和失效用例均通过。此项关闭的是本机开发验证能力，不是发布构建或线上升级；动态外部构建输入仍需显式声明。

验收：测试后修改任一输入即stale；未开始/中断/失败和全部通过可区分；旧MCP不退化。

证据或实现位置：`docs/iteration-0.3.0-verification.json`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`、`docs/iterations/2026-09-10-f2/README.md`、`docs/iterations/2026-09-10-f2/verification.json`、`docs/releases/0.3.3/RELEASE.md`、`docs/releases/0.3.3/verification-g1.json`、`docs/releases/0.3.3/source-archive.json`、`docs/releases/0.3.3/deployment.json`

依赖：无

关闭证据：`docs/releases/0.3.3/verification-g1.json`、`docs/releases/0.3.3/source-archive.json`、`docs/releases/0.3.3/logs/python_tests.log`

### RH-027 · 完成真实发布、运行构建确认和回滚记录

**P0 / 已验收关闭 / 0.3.5**

现象与范围：当前两台在线仍0.2.0；候选135测试不构成发布证明。

当前处理：0.3.5已完成所选NAS和MacBook发布、真实运行哈希确认及完整代码/终端/2MiB文件往返验收。原生64KiB文件输入/HTTP下载/Range另行通过，临时授权撤销，备份保留。Studio按用户明确新范围延期，不将延期误判成本轮发布失败。

验收：按用户当前选定范围发布NAS网关与MacBook，同版运行哈希及稳定回连确认，功能场景通过并保留备份；Studio明确延期且不修改。

证据或实现位置：`docs/iteration-0.3.0-2026-09-10.md`、`docs/releases/0.3.0/macbook-recovery-confirmation.json`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`、`docs/iterations/2026-09-10-f2/README.md`、`docs/iterations/2026-09-10-f2/verification.json`、`docs/releases/0.3.3/RELEASE.md`、`docs/releases/0.3.3/verification-g1.json`、`docs/releases/0.3.3/source-archive.json`、`docs/releases/0.3.3/deployment.json`、`docs/releases/0.3.4/RELEASE.md`、`docs/releases/0.3.4/verification-h1.json`、`docs/releases/0.3.4/deployment.json`、`docs/releases/0.3.4/publish-j1/deployment-summary.json`、`docs/releases/0.3.4/publish-j1/incidents.json`、`docs/releases/0.3.4/recovery-k1/README.md`、`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`

依赖：RH-028、RH-029

关闭证据：`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`

### RH-028 · 验收脚本固定0.2.0及跨版本复用回执

**P1 / 已验收关闭 / 0.3.5**

现象与范围：脚本写死版本并可复用旧报告中的已验收设备。

当前处理：版本、run_id、选中设备集合和临时OAuth撤销均由回执校验；0.3.5真实验收摘要准确报告1个MacBook，单/双设备及错误范围回归已纳入258项验证。

验收：0.3.0接收新版版本参数；旧版报告不得复用为新版通过；仅探测选中设备。

证据或实现位置：`scripts/check-code-gateway.py`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`、`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`

依赖：无

关闭证据：`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`

### RH-029 · 升级执行器与被升级Agent相互依赖

**P1 / 部分修复 / 0.3.2**

现象与范围：从同一Agent终端执行等待idle或重启自身会等待自己/丢完成回执。

当前处理：原失败任务与新修复任务均有完整回执。新一次性更新器运行1次退出0，MacBook升级成功并镜像证据到项目内recovery-k1。修复了预检吞异常与单次临时失败即终止；长期发布状态接口及排空协调仍属后续。

验收：活动任务结束后切换；更新器可独立确认/回滚；上下文可恢复；平台阻止时保留可执行交接。

证据或实现位置：`scripts/upgrade-code-agent.py`、`docs/chatgpt-file-transfer-release-0.2.0.md`、`docs/releases/0.3.0/macbook-recovery-confirmation.json`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`、`docs/iterations/2026-09-10-f2/README.md`、`docs/iterations/2026-09-10-f2/verification.json`、`docs/releases/0.3.4/publish-j1/deployment-summary.json`、`docs/releases/0.3.4/publish-j1/incidents.json`、`docs/releases/0.3.4/recovery-k1/README.md`

依赖：无

### RH-030 · 工具平台安全拦截的可见性与授权边界

**P0 / 外部阻塞 / 0.3.3**

现象与范围：多轮发生命令发出前的平台拦截；本轮组合检查和实际NAS网关升级分别被拦截，均未执行。精确事件见docs/releases/0.3.0/incidents.json与deployment.json。

当前处理：本次NAS传送和升级均正常执行，不能继续沿用历史暂存阻塞结论。新的拒绝仅发生在原MacBook更新器的本机状态/结果读取，未执行且未重放。另一次HTTP403来自独立通用Python探针，包内正常产品请求200并通过鉴权；这是网络响应，与宿主工具拒绝分开记录。

验收：被拦截调用不能记为执行失败/成功；仅在合法授权路径继续；需用户介入则提供精确材料。

证据或实现位置：`docs/concurrency-review-2026-09-10.md`、`docs/iteration-0.3.0-2026-09-10.md`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`、`docs/releases/0.3.3/RELEASE.md`、`docs/releases/0.3.3/verification-g1.json`、`docs/releases/0.3.3/source-archive.json`、`docs/releases/0.3.3/deployment.json`、`docs/releases/0.3.4/RELEASE.md`、`docs/releases/0.3.4/verification-h1.json`、`docs/releases/0.3.4/deployment.json`、`docs/releases/0.3.4/publish-j1/deployment-summary.json`、`docs/releases/0.3.4/publish-j1/incidents.json`

依赖：无

### RH-031 · CI故障注入、跨平台和原生网页验收门禁

**P1 / 未修复 / 0.3.1**

现象与范围：本机合成HTTP不覆盖NAS/公网/原生附件/所有OS；历史测试数不能代替场景。

当前处理：建立测试矩阵：正常、慢流、断网、重启、磁盘/权限/额度/交互取消；每门禁附证据。

验收：Mac/NAS分别通过；Windows不标支持到通过；真实网页文件往返独立核对。

证据或实现位置：`docs/iteration-0.3.0-2026-09-10.md`、`scripts/check-code-gateway.py`

依赖：无

### RH-032 · 快照和重复hash读取成本、内容寻址缓存

**P2 / 未修复 / 0.5.0**

现象与范围：导出需完整快照/计算hash再发送，重复同内容仍传整份。

当前处理：先量化后做内容寻址缓存、已有SHA复用和可选chunk hash；不删除原子校验或sync。

验收：相同内容复用可测量；权限仍按当前操作核验；缓存失效和容量限制正确。

证据或实现位置：`crates/remote-hosts-code/src/transfers.rs`

依赖：RH-007

### RH-033 · 64 MiB限制提高与动态容量协商

**P2 / 未修复 / 0.5.0**

现象与范围：当前单文件64MiB、网关512MiB；直接提高会放大恢复和磁盘问题。

当前处理：续传与GC稳定后按设备协商256MiB或更高，默认值和错误返回一致。

验收：低速大文件+断线+并发+低磁盘可靠通过；不可仅提高schema常数。

证据或实现位置：`crates/remote-hosts-code/src/tools.rs`、`crates/remote-hosts-code/src/transfers.rs`

依赖：RH-007、RH-023

### RH-034 · 过程反馈与产品负责人使用纪律

**P1 / 未修复 / 0.3.1**

现象与范围：用户多次追问是否继续；纯工具调用和最后长报告无法说明当下进展。

当前处理：约定阶段/实测变化/阻塞及时回报；不声称异步工作，不用进度百分比代替证据。

验收：每次迭代有明确目标与完成/剩余状态；新问题当场入库；暂停/拦截立即可见。

证据或实现位置：`docs/iteration-0.3.0-2026-09-10.md`

依赖：无

### RH-035 · 工具与协议能力发现、schema变更兼容

**P1 / 部分修复 / 0.3.2**

现象与范围：ChatGPT工具目录可能仍缓存旧描述；包版本不能表达所有功能/限制。

当前处理：devices_list已现场返回网关工具目录SHA及可选入参，客户端旧hash比较正确提示mismatch；Agent功能单独鉴权上报，Studio旧版明确not_reported。宿主刷新不能由服务器强制，未提供hash时不能解读成schema匹配。

验收：新Gateway旧Agent与新Agent旧Gateway行为明确；无静默忽略lane导致错调度。

证据或实现位置：`crates/remote-hosts-code/src/agent.rs`、`crates/remote-hosts-code/src/gateway.rs`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`

依赖：无

### RH-036 · 源码脚本与安全策略的真实边界

**P1 / 未修复 / 0.4.0**

现象与范围：任意shell具有本机用户权限；测试也会执行仓库代码；脱敏不是安全沙箱。

当前处理：命名检查/发布能力真实注明副作用；不把shell标只读；源URL/DNS/日志脱敏回归保留。

验收：错误scope被拒绝；自定义脚本需用户许可；凭据不进可见结果；不许为性能弱化鉴权。

证据或实现位置：`crates/remote-hosts-code/src/tools.rs`、`crates/remote-hosts-code/src/terminal_output.rs`

依赖：无

### RH-037 · 多文件改动的部分失败和恢复记录

**P1 / 未修复 / 0.4.0**

现象与范围：当前逐文件原子但不是整批事务；异常后的恢复计划依赖模型读journal。

当前处理：继续预检与版本保护，返回结构化已完成/未完成项和恢复动作，不能默认全回滚覆盖外部改动。

验收：第N文件失败时先前结果准确；重试不覆盖并发用户编辑；恢复证据可机器判断。

证据或实现位置：`crates/remote-hosts-code/src/files.rs`

依赖：无

### RH-038 · 构建资源竞争与长编译无进展反馈

**P1 / 已验收关闭 / 0.3.5**

现象与范围：MacBook曾有交换内存压力、Cargo锁等待和超时；并行盲启构建使迭代反而变慢。

当前处理：稳定构建槽与专用目标缓存、同槽互斥、原报告观察和真实子阶段CPU/日志状态已实现。19项槽回归及258项完整检查通过；122个输入再次同步写入0个文件，整套同源复验从首次1128.206秒降至79.410秒。是相同输入缓存对比，不承诺所有编辑加速倍率；强制终止后的独立子进程清理仍由RH-039跟踪。

验收：编译中可区分CPU工作/锁等待/无进展；不重复启动同输入构建。

证据或实现位置：`docs/iteration-0.3.0-2026-09-10.md`、`docs/releases/0.3.4/RELEASE.md`、`docs/releases/0.3.4/verification-h1.json`、`docs/releases/0.3.4/deployment.json`、`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`、`docs/releases/0.3.5/cache-measurement.json`、`scripts/tests/test_build_slot.py`

依赖：无

关闭证据：`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`、`docs/releases/0.3.5/cache-measurement.json`

### RH-039 · 任务取消/终端结束和清理结果混淆

**P1 / 部分修复 / 0.4.0**

现象与范围：工具完成可能仅表示终端启动；cancelled不必然证明全部输出/后代已回收。

当前处理：增加构建持有者异常退出后的拒绝重用保护，隔离进程终止测试通过；仍不把父进程退出视为全部逃逸后代清理完成，人工恢复入口和更多强杀矩阵保留。

验收：仅有terminal_id不能记测试通过；退出码、output_complete/error、cleanup状态一致。；强制结束构建父进程后，不允许在未确认旧编译子进程清理前重新同步同一构建槽；以隔离故障注入验证。

证据或实现位置：`crates/remote-hosts-code/src/terminal.rs`、`crates/remote-hosts-code/src/agent.rs`、`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`、`scripts/release-code.py`、`scripts/build_slot.py`、`docs/releases/0.3.5/incidents.json`、`docs/releases/0.4.0/verification-q1.json`、`docs/releases/0.4.0/deployment.json`、`docs/releases/0.4.0/workflow-acceptance-q1.json`

依赖：RH-013

### RH-040 · SQLite热点及轮询调度索引精修

**P2 / 部分修复 / 0.4.1**

现象与范围：通道并发后多路轮询和进度更新增加写负载；按JSON提取lane的队列筛选需测量。

当前处理：0.4.1恢复控制短事务BEGIN IMMEDIATE避免读转写锁升级竞争，8路相同动作回归无重复效果。泛化数据库热点与满负载指标仍保留。

验收：满负载下control延迟稳定；SQLITE_BUSY有界恢复；进度降级不取消真实工作。

证据或实现位置：`crates/remote-hosts-code/src/gateway.rs`、`crates/remote-hosts-code/src/store.rs`、`docs/releases/0.4.1/verification.json`、`docs/releases/0.4.1/deployment.json`、`docs/releases/0.4.1/incidents.json`

依赖：RH-017

### RH-041 · NAS 未提供 SFTP 子系统导致默认 SCP 发布传输失败

**P1 / 部分修复 / 0.3.1**

现象与范围：0.3.0 发布时已认证 SSH 正常，但 scp 返回 subsystem request failed on channel 0 / Connection closed，退出码255。

当前处理：同一受信SSH连接的传统SCP模式已传送成功，NAS二进制和升级脚本SHA-256与候选一致；自动能力协商和友好诊断仍未实现，因此只标部分处理。未修改服务器防护。

验收：部署入口识别SFTP不可用，返回准确能力诊断或明确支持的传输模式；保持主机密钥检查；传完SHA-256一致；不在错误设备上重试。

证据或实现位置：`docs/releases/0.3.0/incidents.json`

依赖：RH-017

### RH-042 · Agent升级门禁缺少网关回连确认和本机超时自动回退

**P0 / 部分修复 / 0.3.2**

现象与范围：本地PID和版本标记不证明能稳定完成网关任务。MacBook发布曾误报upgraded但连接反复中断。

当前处理：本次MacBook0.3.4更新以同一PID/会话的10次采样、约21.9秒稳定窗口和五通道继续前进确认成功。原预检底层原因因旧版本吞异常不可恢复，改为安全分类和有界重试，永久鉴权/TLS问题不重试。实际恢复不依赖关闭安全检查。完整生产故障矩阵仍待完成。

验收：新进程无法稳定轮询/上报时不报告完整发布成功；独立更新器有界恢复旧版并验证探针。；覆盖公网不可达、会话等待、更新器异常和回执丢失；不伪造心跳或盲目重放业务任务。

证据或实现位置：`docs/releases/0.3.0/macbook-recovery-confirmation.json`、`scripts/upgrade-code-agent.py`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`、`docs/releases/0.3.4/recovery-k1/README.md`

依赖：RH-025、RH-029

### RH-043 · 升级预检依赖隐式Python与SQLite运行环境

**P1 / 部分修复 / 0.3.1**

现象与范围：Studio经SSH首次SQLite预检失败，显式Homebrew Python与文件URI后成功，唯一根因未证实。

当前处理：一次性launcher固定绝对Python路径并快照辅助模块；特殊字符SQLite URI回归通过。跨全部支持解释器和平台矩阵仍待完成。

验收：替换前报告解释器/SQLite及准确失败阶段；受支持环境、空格和URI特殊字符路径测试通过。

证据或实现位置：`docs/releases/0.3.0/macbook-recovery-confirmation.json`、`scripts/upgrade-code-agent.py`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`

依赖：RH-017

### RH-044 · 更新器重复触发导致相同版本被反复安装和Agent强制重启

**P0 / 候选已验证 / 0.3.2**

现象与范围：保活更新任务多次改写部署回执，旧包哈希与候选相同仍kickstart；用户bootout后PID1927/runs644不再变化。

当前处理：本轮默认回归外另行启用真实临时launchd计数探针，退出后12秒无重入并已清理；MacBook真实升级任务runs=1退出0，安装进程稳定。保留失败/回滚证据要求，不改写冻结旧包中的脚本。

验收：重复调用和并发调用不会再次重启相同二进制，不会覆盖有效备份和回执。；升级入口明确一次性且不采用KeepAlive/周期触发；退出后不再运行，真实发布场景验收。；升级和回滚失败均有保留记录，正式Agent保活配置与更新器独立。

证据或实现位置：`docs/releases/0.3.0/macbook-recovery-confirmation.json`、`scripts/upgrade-code-agent.py`、`docs/releases/0.3.1/verification.json`、`docs/releases/0.3.1/deployment.json`、`docs/releases/0.3.2/RELEASE.md`、`docs/releases/0.3.2/deployment.json`、`docs/releases/0.3.2/verification-final.json`

依赖：RH-029

### RH-045 · 批量观察精确预算与单项结果不可用隔离

**P1 / 候选已验证 / 0.3.3**

现象与范围：冻结0.3.2批量预算使用保守占位估算，多项小结果可能被不必要省略；显式单ID预算未统一执行；单项下载产物过期等装饰失败可导致整批读取失败。并非任务执行失败，不应靠重执行恢复。

当前处理：上轮精确JSON预算、显式单ID预算和单项产物不可用隔离纳入本次固定0.3.3输入，完整215项检查通过。原单ID返回形状及权限检查保持。构建被拦截，未将工作树修复描述为已上线。

验收：20个小结果在可容纳预算内不被过度省略；实际JSON总长度不超过显式上限。；一项产物过期不丢弃其它有权访问的结果；错误不包含签名URL且不重派发原任务。；默认旧单ID形状保持兼容，显式观察参数遵守新预算，混合权限在输出前完整校验。

证据或实现位置：`crates/remote-hosts-code/src/observations.rs`、`crates/remote-hosts-code/tests/observations_032.rs`、`docs/releases/0.3.2/post-verification-source-change.json`、`docs/iterations/2026-09-10-f2/README.md`、`docs/iterations/2026-09-10-f2/verification.json`、`docs/releases/0.3.3/RELEASE.md`、`docs/releases/0.3.3/verification-g1.json`、`docs/releases/0.3.3/source-archive.json`、`docs/releases/0.3.3/deployment.json`

依赖：RH-016

### RH-046 · 升级预检吞掉底层错误且单次临时失败中断发布

**P1 / 已验收关闭 / 0.3.5**

现象与范围：原gateway_readiness_unavailable无法区分HTTP/TLS/DNS/timeout，单次只读失败就结束安装前检查。原始异常已丢失，不能倒推具体原因。

当前处理：安全错误分类、执行阶段、service_changed和临时故障有限重试正式进入0.3.5发布包，已用于MacBook真实升级。预检本次1次成功，稳定窗口约20秒、10次采样、更新器runs=1退出0；重试/拒绝边界由源码回归覆盖，未倒推旧失败的不可恢复原因。

验收：超时/暂时DNS/部分5xx只重试同一只读鉴权请求且最多3次；HTTP401/403、TLS证书和身份冲突不自动重试。；失败记录保留安全类别、phase、次数、是否已修改服务；日志不泄露凭据或带授权URL。；失败前未安装可准确证明；后续已授权恢复成功且更新器不重复运行；正式发版包纳入修复。

证据或实现位置：`scripts/agent_upgrade_support.py`、`scripts/upgrade-code-agent.py`、`scripts/tests/test_readiness_network.py`、`docs/releases/0.3.4/recovery-k1/helper-verification.json`、`docs/releases/0.3.4/recovery-k1/macbook-updater.json`、`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`

依赖：无

关闭证据：`docs/releases/0.3.5/deployment.json`、`docs/releases/0.3.5/verification.json`、`docs/releases/0.3.5/acceptance-macbook.json`

### RH-048 · 已部署的新核心源码尚未纳入Git交付基线

**P1 / 候选已验证 / 0.5.0**

现象与范围：本次git status显示crates/remote-hosts-code及新增发布脚本/测试仍未跟踪。版本化源码归档存在，但main历史不能独立交付和复现这些已部署源码，也难以统一审查与后续CI。

当前处理：0.5.0核心源码、测试和发布脚本已按白名单纳入main提交e6b68417；固定源码快照通过338项测试并保存git-baseline。当前文档/验收bug修复将作为后续提交，不改写历史基线。仍需在常规CI/干净检出中长期保持这一纪律后再关闭。

验收：本项目可发布源码/测试/脚本纳入main的可追溯提交，不包含凭据、状态库、缓存或无关用户改动。；干净检出与声明输入可重建对应源码快照；提交、快照、验证、产物和运行记录建立关联，不将任意dirty状态记成纯提交构建。

证据或实现位置：`docs/product/roadmap-0.5.0-evidence.json`、`docs/product/ROADMAP-0.5.0.md`、`docs/releases/0.4.1/source-archive.json`、`docs/releases/0.5.0/git-baseline.json`、`docs/releases/0.5.0/verification.json`、`docs/releases/0.5.0/RELEASE.md`

依赖：无

### RH-049 · code_diff未明确未跟踪文件的审查缺口

**P1 / 候选已验证 / 0.5.0**

现象与范围：在git status显示核心目录未跟踪的情况下，对crates/remote-hosts-code调用code_diff返回diff为空且truncated=false。当前实现调用普通git diff，返回未明确说明未跟踪内容未被覆盖，容易被上层误判为没有待审查改动。

当前处理：0.5.0已实现完整变更审查：code_diff可显式覆盖非忽略untracked文本/二进制、预算省略和版本冲突；候选回归通过并已部署。当前宿主可能仍缓存旧工具schema，完整原生ChatGPT验收尚未补齐。

验收：存在未跟踪新文件时明确返回新增内容或未展开清单，不把空diff表述为工作区干净。；覆盖新增/删除/重命名/二进制和输出预算，保持只读，不修改索引、工作树或忽略规则。

证据或实现位置：`docs/product/roadmap-0.5.0-evidence.json`、`crates/remote-hosts-code/src/agent.rs`、`docs/product/ROADMAP-0.5.0.md`、`crates/remote-hosts-code/src/change_review.rs`、`docs/releases/0.5.0/verification.json`、`docs/releases/0.5.0/RELEASE.md`

依赖：无

### RH-050 · 缺少有清单、预览与逐文件回执的批量文件集同步

**P1 / 候选已验证 / 0.5.0**

现象与范围：当前文件工具以单一路径为交付单位，多文件成果与发布暂存依赖临时脚本打包/解包和手工记录，未提供一致的路径清单、差异预览、冲突及取消恢复模型。

当前处理：0.5.0已实现Manifest单向文件集同步：plan绑定目标版本，bundle只包含变化文件，apply逐文件原子发布，同manifest重放只补未满足项并保护并发修改；100文件仅3项变化的回归已进入338项候选验证。生产完整协作验收因标准验收入口run-id bug未继续执行，待补现场证据后关闭。

验收：100文件只修改3个时仅传变化文件内容，保留manifest和逐文件完成/冲突状态，中断后恢复同一manifest。；过滤规则、权限和原版本检查有效；拒绝路径穿越、符号链接逃逸、特殊文件与超限展开；取消不破坏已确认交付或用户并发修改。；配额与过期清理保护活动传输/下载，清理产物不使旧操作再次执行。

证据或实现位置：`crates/remote-hosts-code/src/tools.rs`、`target/iteration-041-r1/publish.py`、`docs/product/ROADMAP-0.5.0.md`、`crates/remote-hosts-code/src/files_sync.rs`、`scripts/sync_bundle.py`、`docs/releases/0.5.0/verification.json`、`docs/releases/0.5.0/RELEASE.md`

依赖：RH-007、RH-023、RH-037

### RH-047 · 发布等待器缺少构建任务身份核对，启动确认被误当作交付进展

**P0 / 部分修复 / 0.4.2**

现象与范围：0.4.0 n2发布器启动后等待不存在的pipeline.json直至3600秒超时；targeted-result.json也缺失。启动/语法检查的成功不能证明最终验证或构建已实际执行。用户只能反复追问，且清单还保留过期Studio延期范围。

当前处理：正式包纳入已完成构建与产物检查、双方原回执有界等待和持久阶段日志，14项模块回归通过。实际发布NAS/MacBook成功；Studio因其他工作忙在安装前退出。发布编排先等所有设备版本再检查回执导致额外等待，并挡住健康目标的后续自动验收；需按目标独立推进、及早观察失败且不让观察器成为idle阻塞。综合验收另受宿主拦截，不误报整版完成。

验收：发布入口必须绑定实际构建任务标识或已经校验的产物；缺少生产者时立即返回明确needs_build，不对不存在的结果无意义等待一小时。；分别记录格式、静态检查、功能测试、构建、安装和现场验收；未运行门禁不得显示通过，started不得映射为upgraded。；格式修正后从新固定快照通过全部门禁，发布网关与两台Mac并核对每台实际版本和验收回执，不重复安装已成功目标。

证据或实现位置：`target/iteration-040-n2/publish.py`、`docs/releases/0.4.0/deployment-n2.json`、`target/iteration-040-p1/pipeline.json`、`target/iteration-040-p1/pipeline-logs/verification.json`、`docs/releases/0.4.0/verify-p1/verification.json`、`docs/releases/0.4.0/verify-p1/VERIFICATION.md`、`docs/releases/0.4.0/deployment.json`、`docs/releases/0.4.0/verification-q1.json`、`docs/releases/0.4.0/deployment-q1-before-receipt-recovery.json`、`docs/releases/0.4.0/workflow-acceptance-q1.json`、`target/iteration-040-q1/accept-completed-upgrades.py`、`docs/releases/0.4.1/verification.json`、`docs/releases/0.4.1/deployment.json`、`docs/releases/0.4.1/incidents.json`

依赖：无
