# Remote Hosts 网页代码网关评审与改进记录

日期：2026-09-09。执行设备：MacBook-M2-Max。
项目：`/Users/jinliang/Workspace/remote_hosts`。

## 结论与范围

当前网页入口已经能够完成真实的远程代码定位、按范围读取、版本校验编辑、终端执行和结果查询；本次评审本身就在这条链路上完成了代码修改和测试。它不是只能读取文件的演示，但还不能被描述成已经统一了代码操作、二进制文件传输与所有远程任务的成熟入口。

保留现有核心设计：明确选择设备、工作区固定绑定设备与目录、设备主动连接网关、OAuth 与设备权限分别校验、精确版本编辑、持久化操作和幂等键。终端执行拥有本机用户权限，不受代码根目录沙箱约束，不能把它伪装为只读工具。

本次重点审阅 `crates/remote-hosts-code` 的 gateway、agent、files、terminal、tools、store、auth，以及现有部署/验收文档。旧 SSH/MinIO 传输部分主要核对现有工作流和边界，没有在生产跳板链路重新传输文件。本次不是整个仓库的全面安全认证或全部平台的发行验收。

开始时，`Cargo.toml`、`Cargo.lock`、`README.md` 已有修改，整个 `crates/remote-hosts-code/` 及相关脚本/文档尚未被 Git 跟踪；当时 HEAD 是 `b3252b5`。这些是已有工作，本次没有把它们冒充为新开发成果，也没有重置、提交或推送它们。

尝试读取只与本项目相关的 Codex 历史元数据时，被平台工具安全检查拦截；没有绕过，也没有读取到 Codex 对话正文。以下结论以实际源码、项目文档和执行结果为依据。

## 已实施的改进

| 项目 | 原问题 | 本次处理 | 主要位置 |
|---|---|---|---|
| 回执准确性 | 未知任务、未派发任务、冲突回执也会得到成功接受响应 | 仅允许对应设备已派发任务落结果；不存在返回 404，状态/内容冲突返回 409；相同结果重传仍成功，不覆盖既有结果 | `src/gateway.rs::receipt` |
| 参数合同 | 主要只检查顶层键名；嵌套类型、必填字段等不能在入队前统一拒绝 | 网关和设备两端复用固定目录的递归校验，覆盖 required/type/enum/数组和整数边界；错误包含字段路径，不包含参数值 | `src/tools.rs::validate`、`src/agent.rs::perform` |
| 设备会话租约 | 先查询后写入，存在两个新会话同时通过检查的窗口 | 单条条件 UPSERT 原子认领/续租，保留 45 秒过期替换规则 | `src/gateway.rs::poll` |
| 内部等待 | 任务每 250 毫秒、结果每 100 毫秒查一次数据库 | 每设备事件唤醒，先持久化后通知；等待前建立通知 Future，再读取持久状态；保留一秒恢复检查和原长轮询/返回 pending 机制 | `src/gateway.rs::dispatch`、`poll` |
| 搜索准确性 | 前文过长或单行很长时，返回的文本可能完全没有真正的命中内容 | 优先保留命中行，再加入上下文；超长行围绕命中窗口截取，保持 UTF-8 边界，显式报告偏移及命中本身被截断 | `src/files.rs::search_snippet`、`search` |
| 文本编辑闭环 | 可创建含 NUL 的文件，但同一套文本读取/编辑工具随后不能读取它 | 所有文件预检阶段拒绝 NUL，在任一文件写入前结束 | `src/files.rs::apply` |
| 分页边界 | 极大游标偏移加 limit 可能溢出并 panic | 饱和加法后严格验证边界，返回错误而不是 panic | `src/files.rs::list` |
| 调用简化 | 已知路径与行号时，工具指引仍要求先搜索 | 修改服务端与工具说明：仅定位未知路径/范围时搜索；已知范围直接批读，复用当前对话工作区 | `src/tools.rs`、`Gateway::get_info` |

上述源码路径均相对于 `crates/remote-hosts-code/`。未添加运行时依赖，工具仍为 13 个，现有有效调用参数不变。固定工具目录改成进程内惰性缓存，避免每个设备操作重建目录。

参数验证器有意只实现当前静态目录使用的 schema 子集，不是通用 JSON Schema 引擎。以后增加新的 schema 关键字时，必须同步扩展验证器及测试，不能仅更新广告中的 schema。

通知只是降低等待的提示，不是业务状态。持久数据库仍决定工作是否派发/完成。保留恢复检查是为了不把一次遗漏通知、设备重连或进程重启变成永久等待。没有通过删除持久化、版本校验或幂等保护换取性能。

## 可复现验证

### 功能测试

修改前原有 5 个单元测试、3 个集成测试通过。新增 14 个回归测试，覆盖回执、设备隔离、租约竞争/过期、参数合同、搜索截断、中文 UTF-8、NUL 全文件预检和游标溢出。

其中 7 个用例在修复前实际失败，并在修复后通过：未知回执、未派发回执、冲突回执、入队前参数校验、NUL 预检、长上下文丢失命中、长行丢失命中。搜索用例最初的临时目录 fixture 还暴露了 macOS `/var` 与 `/private/var` 的规范路径差异；修正 fixture 为 canonical root 后，重新确认了真正的搜索失败。会话竞争测试在原版的一次运行中未触发竞态，原子租约修改属于源码确定存在检查/写入窗口的加固，不据此宣称已复现生产双会话故障。

首轮修复验证：22 个功能测试全部通过；性能用例默认 ignored，另行显式运行且通过。随后修改了等价的 Clippy 写法和调用指引，严格 Clippy 已通过。

最终组合验证已完成，命令链退出码为 0：网页网关 22 项功能测试、旧 `remote-hosts-mcp` 64 项测试全部通过，合计 86 项；没有功能测试失败。`cargo fmt --check`、严格 `cargo clippy --all-targets -- -D warnings` 和 `cargo check --workspace` 均通过。性能测试是额外的显式通过用例，不混入这 86 项功能测试。

最终工作树仍在 `main`，未新增分支。`terminal.rs` 和历史验收 JSON 的 SHA-256 与评审开始时一致。已验证源文件的 SHA-256 如下，**不是运行二进制的校验和**：

```text
39a10470373d1c7b5fe69c87e86b36fb4f4af44c4d1cc930f1ce8cccfa98c4f8  src/agent.rs
a4b0c637420caf4f3683b5dcb914e9306a6d7f71abbc752005f17129a6d84c21  src/files.rs
25f96325ceb2ed3f7d7cb2c816968a764423582f7072bf8babf0f0c3bd2f27d9  src/gateway.rs
0bd87846dbaf1b5021af6b1748e95ad201bf06781a6c0a4208ebf00f48ef1d3a  src/tools.rs
```

关键命令：

```sh
cargo fmt -p remote-hosts-code -- --check
cargo clippy -p remote-hosts-code --all-targets -- -D warnings
cargo test -p remote-hosts-code -p remote-hosts-mcp -- --test-threads=2 --color never
cargo check --workspace
cargo test -p remote-hosts-code --test gateway_latency -- --ignored --nocapture --color never
```

测试文件：`tests/review_regressions.rs`、`tests/gateway_latency.rs`。原有测试未被跳过或删除。

### 内部链路基准

同一台 MacBook，以 debug 构建运行 Gateway Router → Agent → SQLite/本地文件 → 回执的读取流程。HTTP 处理器通过进程内 Tower 调用；每次真实执行按行读取并检查内容。3 次预热后记录 20 次串行请求。创建临时工作区、编译时间不计入样本，不使用生产凭据或生产设备会话。

| 指标 | 修改前 | 通知/准确性修复后 |
|---|---:|---:|
| 平均延迟 | 260.2493751 ms | 8.5917146 ms |
| P50 | 231.410375 ms | 6.000416 ms |
| P95 | 313.822709 ms | 12.913208 ms |
| 样本数 | 20 | 20 |
| 总返回 JSON 字节数 | 6880 | 6880 |

这表明固定轮询造成的内部等待被明显削减，且没有靠减少返回内容取得这次结果。样本少、并非严格受控的统计实验，不应把比例直接外推为生产吞吐量。

**不包含**公网 TLS/Cloudflare/NAS 转发、真实 TCP 网络往返、OAuth 交互、ChatGPT 调度、用户确认或模型生成。不能声称整个网页操作已经快了同样倍数，也不能声称已经测得 token 消耗降低。后续只修改了等价边界表达式和指引文字，未改变该基准使用的算法。

## 尚未解决的关键问题

### P1：网页入口的文件传输缺失

证据：网页 `tools::catalog` 仅有 13 个代码/终端/操作工具，没有上传、下载或附件通道。旧 `docs/minio-relay.md` 与 `skills/remote-hosts-agent/references/mcp-workflows.md` 记录的 SFTP/MinIO 路径是另一套 SSH 主机与工作区体系。不能把 ChatGPT 沙箱路径当作 MacBook 路径，也不能把 SSH Host ID 当作网页 Device ID。

建议只向 Agent 暴露少量任务级上传/下载操作，继续复用 `operation_get` 查长任务；分块、断点续传、临时对象等细节由内部负责。文件字节应通过经过授权的连接器/HTTPS 数据通道搬运，不让模型输出 Base64 正文，不用文本编辑工具模拟二进制传输。

第一版应覆盖：附件/构建产物到选定设备，以及选定设备文件回传为可下载产物。必须具备明确的源目标身份、路径权限、默认禁止覆盖、大小限制、SHA-256 校验、同目录临时文件加最终原子放置、取消/超时/精确重试、最小权限临时对象与过期清理。任意远程 URL 不能未经源限制和重定向校验直接当作可信下载源。

复用旧传输实现的校验、原子落盘与恢复经验，而不是直接公开旧未认证 operator API 或合并两个系统的权限数据库。旧文档也明确生产交互跳板链路端到端仍有未验收部分；本次没有补做该生产验收。

### P1：非交互终端与增量输出需要独立修复

`src/terminal.rs::start` 无论 `pty` 参数如何都创建 PTY，设置 `TERM=xterm-256color`。本次真实工具输出中已经看到 Git 分页器/颜色控制序列和 CRLF。非交互编译、日志或机器输出应走真实 pipes；只有显式交互才走 PTY。应明确 stdin EOF、stdout/stderr 标记、退出码和子进程组取消语义。

`src/terminal.rs::read` 每次读取完整日志后重新进行 UTF-8 有损转换和脱敏，再按变换后文本的字节游标分页。源码上存在追加输出改变此前可见前缀/偏移的风险，也有反复处理整个日志的成本。本次没有专门复现所有分包和脱敏边界，不能宣称已验证泄漏或已修复。

建议写入阶段维护增量 UTF-8 解码与脱敏状态，形成不可变的安全输出流，再对稳定流使用游标。验收必须覆盖中文跨块、敏感模式跨块、无换行交互提示、截断、取消、进程退出后读取和重启 runtime_lost。不要为了稳定游标关闭脱敏，也不要认为模式脱敏能保证过滤任意恶意输出。

### P2：并发、调用次数、长期运行成本

`src/agent.rs::execute` 的全局互斥锁及单一 poll/execute/result 循环，让慢操作可能阻塞其他工作区读取和终端控制。后续应先分离心跳/控制优先级，再实现有上限的读并发和真实资源范围内的写串行，而不是直接无限并发所有动作。

小命令目前总是先返回 terminal_id，再另行读取；可评估有上限的短等待，短任务直接返回完成信息、长任务仍保留同一 ID。不得把等待超时误解为执行失败并用新键重跑。

搜索分页会重新扫描前面的内容，同文件多个读取范围也会重复读取/计算版本。先增加代表性大仓库测试，再按有界缓存、文件变化失效、文件/行位置游标优化，保持版本和 live-file 一致性说明。

操作、读取结果、工作区与日志需要明确容量和保留政策。清理不得删除活跃任务，更不能通过删除幂等收据使重试重新产生副作用。需要保留幂等墓碑或明确且可检验的重试窗口。

设备/网关诊断应分别报告实际运行构建标识和协议能力，避免源码、编译产物、NAS 网关、Mac Agent 和文档版本混淆。不要只依赖始终为 0.1.0 的包版本。

## 交付与部署边界

本次修改：4 个源文件（agent/files/gateway/tools），新增 2 个测试文件，更新网关说明并新增本评审记录。没有新增上传/下载工具，没有修复 terminal.rs，没有替换 NAS 或 Mac 上正在运行的二进制，没有重启任何设备 Agent，没有修改凭据、Caddy 或 NAS 服务配置，没有提交/推送 Git。

历史 `docs/chatgpt-code-gateway-acceptance.json` 保持原样，不把旧二进制的浏览器验收冒充为新源码验收。

上线前应分开完成：保存现有工作与确定提交范围；构建 Mac ARM64 和 NAS Linux 产物并记录校验和；确认当前活动终端并维护窗口更新；核对实际进程构建；重新验收 OAuth、断连/重试、双设备隔离，以及真实 ChatGPT 对话中的读、写、测试和错误恢复。当前连接承载本次工作，不应边修改边无条件重启它。

## 外部规范核对

- OpenAI MCP OAuth 指引：`https://developers.openai.com/plugins/build/auth`。保留资源绑定、S256 PKCE 与精确回调白名单；DCR 仍受支持，不必为本次局部修复强行迁移认证体系。
- MCP Streamable HTTP：`https://modelcontextprotocol.io/specification/2025-11-25/basic/transports`。Origin 校验、认证和本地绑定需要保留；网络断开不等于操作取消。
- Tokio Notify：`https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html`。`notify_waiters` 会通知此前已创建但未轮询的 Notified Future；本次等待顺序依此设计，并保留持久状态恢复检查。源码编译/测试使用仓库锁定的 Tokio 版本。
