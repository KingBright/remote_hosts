# ChatGPT 代码网关第二轮改进：终端执行与稳定输出

日期：2026-09-09。设备：MacBook-M2-Max。
项目根目录：`/Users/jinliang/Workspace/remote_hosts`。
分支：`main`。本次未提交、未推送，未替换或重启正在服务本次对话的 Agent/NAS 网关。

## 文件在哪里

这里的“留在本机”指用户的 MacBook-M2-Max，不是 ChatGPT 沙箱、Mac Studio 或 NAS。

- 第一轮历史报告：`docs/chatgpt-code-gateway-review-2026-09-09.md`，保持原样。
- 本轮报告：`docs/chatgpt-code-gateway-terminal-review-2026-09-09.md`。
- 持续更新的使用说明：`docs/chatgpt-code-gateway.md`。
- 源码与测试：`crates/remote-hosts-code/`。
- 本轮测试原始记录、性能 JSON、修改前备份：`target/review-round2-20260909/`。

以上均相对项目根目录。日志目录属于可清理的构建工作区，本报告保留关键结果；不要把 `target/` 当长期备份。Cargo 当前实际编译缓存由用户已有配置放在 `/Users/jinliang/rust-target`，与本报告的项目内证据目录不同。

## 已完成的源码改进

| 改进 | 实现及边界 |
|---|---|
| 真正的非交互执行 | `pty=false` 使用系统 pipes，stdin 为 EOF，stdout/stderr 接到同一管道；只有 `pty=true` 创建 PTY。非交互输入请求明确拒绝，不会假装可以交互。 |
| 减少终端展示干扰 | 普通命令设置 `TERM=dumb`、`NO_COLOR=1`、`CLICOLOR=0`、`PAGER=cat` 和 `GIT_PAGER=cat`。用户命令或 shell 配置仍可以覆盖这些偏好，不承诺禁止所有 ANSI 输出。 |
| 稳定的增量输出 | 新建 `terminal_output.rs`，增量处理 UTF-8 和凭据脱敏后写入只追加日志。未完成的多字节字符、可能的设备凭据前缀暂不发布，避免下一块输出反过来改变已有游标。 |
| 脱敏前移 | 新终端的已知设备凭据及支持的 password/token/secret/apikey/api_key/api-key 模式在写入日志前处理，支持跨块、引号、转义和 JSON 键。未闭合长值增量丢弃，不无限缓冲。模式脱敏不等于任意输出安全保证。 |
| 有界读取 | 新日志按字节游标 seek，只读取请求页及最多 4 字节 UTF-8 前瞻，不再为了读取尾部而重复处理全部日志。 |
| 明确的状态 | 暴露 `output_complete`、`output_error`、实时 `output_truncated`、`cursor_format` 和 `output_stream=combined`。日志缺失是明确错误，不再返回成功但空输出。 |
| 生命周期准确性 | shell 启动失败持久化为 failed；容量由 8 个运行许可管理；取消与结束状态用条件 SQL 更新协调；超时/取消杀 Unix 进程组，等待进程退出并关闭捕获。 |
| 旧数据兼容 | 旧 raw 日志使用 `legacy_transformed` 兼容读取；新日志使用 `sanitized_utf8_v1`。不把旧 raw 文件标成已脱敏，不隐式改写其游标或历史文件。 |

主实现位于 `src/terminal.rs` 和 `src/terminal_output.rs`；`src/lib.rs` 注册私有输出模块，`src/tools.rs` 更新终端调用说明。上述路径均相对 `crates/remote-hosts-code/`。

工具数量仍为 13，现有输入参数、权限范围和幂等入口不变；未增加运行时依赖，未提升仓库已有 Rust 1.94 最低版本要求。stdout/stderr 明确合流，没有伪造来源标签或宣称分别保持两个流。

## 实际复现与测试

先保存原始 `terminal.rs`、`tools.rs`，仅新增回归用例并在旧实现上运行。以下 6 项全部失败，然后在修复实现上全部通过：

| 回归 | 旧实现观测 |
|---|---|
| 非交互 pipes/EOF | 三个 fd 都是 TTY，输出带 CRLF，stdin 不具备预期 EOF。 |
| 非交互拒绝输入 | 即使 pty=false，仍然接受终端按键。 |
| 跨块设备凭据 | 首块中的合成凭据前缀被立即返回，而完整凭据在下一块到达后又会替换。 |
| 跨块 UTF-8 | 收到中文字符的第一个字节时就返回替换字符，随后已发布前缀变化。 |
| 未闭合引号值 | 读取时的正则替换随引号闭合改变文本长度，旧游标无法稳定代表同一输出前缀。 |
| shell 启动失败 | 状态停留 starting，而不是 failed。 |

所有凭据测试使用合成测试字符串，未使用生产设备凭据；测试目录在临时 fixture 中，未执行生产设备注册、认证或部署。

新增 14 个真实本地进程回归测试，加上 6 个增量输出单元测试，共 20 项。除上述 6 项外，还覆盖无换行交互提示、同组子进程清理、重复取消、超时、容量释放、旧状态重启、工作区隔离、丢失日志、UTF-8 游标边界、不同分块尺寸、长凭据值和 8 MiB 捕获上限。

已完成的整体验证：

| 验证 | 结果 |
|---|---|
| remote-hosts-code 单元测试 | 11 通过 |
| 原有网页集成测试 | 3 通过 |
| 第一轮 review_regressions | 14 通过 |
| 本轮 terminal_regressions | 14 通过 |
| 旧 remote-hosts-mcp 测试 | 64 通过 |
| 功能测试合计 | 106 通过，0 失败 |
| cargo fmt 检查 | 通过 |
| 严格 Clippy `--all-targets -- -D warnings` | 通过 |
| cargo check --workspace | 通过 |
| 独立终端读取性能测试 | 显式运行通过，不计入 106 项 |

两个性能用例在常规测试中按设计 ignored：第一轮 gateway_latency、本轮 terminal_read_latency；没有把功能回归改成 ignored。首轮 Clippy 指出了一个可合并 if 的风格问题，已修正后重跑。随后补齐了不带分隔符的 `apikey` 拼写及分块测试，并对最终源码再次完成同一组验证：组合命令退出码为 0，106 项功能测试再次全部通过，格式、严格 Clippy 和工作区检查通过。最终结果及校验和见 `final-*.log` 与 `final-source.sha256`。

本轮最终两个核心源文件 SHA-256（不是生产运行二进制）：

```text
19dc2c405e13a18a0f1de90a3def0653b99b3ef005da11b687edb17e68b6f78e  src/terminal.rs
43d078ac5c88d70777e735a01f8bbff98cf8997d2527f68582c864486de143f3  src/terminal_output.rs
```

关键命令：

```sh
cargo fmt -p remote-hosts-code -- --check
cargo clippy -p remote-hosts-code --all-targets -- -D warnings
cargo test -p remote-hosts-code -p remote-hosts-mcp -- --test-threads=2 --color never
cargo check --workspace
cargo test -p remote-hosts-code --test terminal_read_latency -- --ignored --nocapture --color never
```

可核对的记录：`baseline.log`、`terminal-first-pass.log`、`full-tests.log`、`clippy.log`、`workspace-check.log`、`terminal-read-benchmark.json` 和 `terminal-read-benchmark.log`。最终复验另存 `final-tests.log`、`final-clippy.log`、`final-workspace-check.log`、`final-source.sha256`，不覆盖第一次失败的基线证据。

## 读取开销实测

测试文件：`tests/terminal_read_latency.rs`。比较当前实现保留的旧格式兼容路径与新格式 seek 路径，不是两个生产二进制的 A/B 测试。每个路径读取 8 MiB 合成文本日志的最后 1 KiB，3 次预热后记录 10 次请求，逐次验证返回内容、游标和 has_more。

| 指标 | 旧格式整日志处理 | 新格式 seek |
|---|---:|---:|
| 平均耗时 | 625.250971 ms | 0.6957375 ms |
| P50 | 604.991542 ms | 0.248667 ms |
| P95 | 716.4715 ms | 2.515417 ms |
| 测量次数 | 10 | 10 |
| 返回文本总字节数 | 10240 | 10240 |

范围：同一台 MacBook，debug 构建，热缓存，真实 `Terminals::read`、SQLite 查询和本地文件 I/O。旧兼容路径也已经缓存正则，未靠每次重新编译正则放大差距。新日志在写入阶段完成增量处理，此测试只衡量读取，不包含首次捕获/脱敏成本。

**不包含**公网网络、NAS、TLS、ChatGPT 调度、确认和模型生成，也不是端到端吞吐基准。样本数量小，日志为简单合成文本；不能把差距外推成网页整体加速比例，更不能据此报告实际 token 节省。

## 文件传输：协议已核对，功能尚未实现

当前 13 个网页工具仍然没有二进制上传/下载。已有 SSH/MinIO 传输和网页 Device/Workspace 是不同身份体系，不能直接混用标识或绕过认证。

本轮核对 OpenAI 官方工具参考中的文件输入约定，已找到不让模型搬运 Base64 的正式入口：工具描述顶层 `_meta["openai/fileParams"]` 标记文件参数；文件对象声明 `download_url`、`file_id`、`mime_type`、`file_name` 四个字段，其中前两个必填，后两个可选。输入按临时 URL 获取真实字节，而不是把 ChatGPT 文件路径当成 MacBook 路径。

实施顺序建议：先完成文件接收的端到端闭环，再完成设备文件回传。工具面保持少量任务级操作，长任务复用 operation_get；分块/恢复/临时对象由内部通道管理，不要求模型逐块调用。

接收必须把以下条件一起实现：明确的设备/工作区绑定；授权后的文件来源校验和严格重定向策略；大小与超时限制；默认禁止覆盖；哈希校验；同目录临时文件和原子放置；同一幂等键的精确重试；失败/取消可恢复。临时下载 URL 可能承载授权，不能写入公共日志或长期响应，也不能把支持文件参数误当作允许任意 URL 请求的凭据。

回传到 ChatGPT 的可下载产物协议、临时访问权限和实际网页体验尚需独立实现与验收。本轮没有添加占位工具、开放无认证传输接口或宣称文件传输已经可用。

## 未解决的边界与部署状态

本轮测试平台是 MacBook 的原生 macOS 构建。未完成 Linux/NAS 和 Windows 跨平台执行验收；cargo check --workspace 是本机检查，不是所有目标平台验证。

Unix 进程组取消不约束刻意 setsid/逃离进程组的后代。超时关闭公开日志可以防止迟到输出改变已完成结果，但不证明所有逃逸后代、阻塞读取线程都被回收。异常进程资源回收仍需单独加固，不能将此终端称为安全沙箱。

模式脱敏有误报与漏报边界；历史 raw 日志、命令参数和其它操作日志并未被全面迁移清理。单 Agent 全局执行串行、心跳隔离、交互 stdin 写入阻塞风险、任务和日志保留政策、文件传输、实际运行构建标识仍是后续工作。

没有重新读取被安全检查阻止的 Codex 历史，没有修改认证/Caddy/生产配置，没有覆盖用户其它改动，没有提交或推送 Git，没有替换正在运行的二进制，也没有改动 `chatgpt-code-gateway-acceptance.json`。第一轮报告是历史快照，不因本轮改进重写其当时结论。

上线验收需分开核对源码提交范围、目标产物、运行构建、现有活动终端、部署回滚、OAuth/设备隔离，以及真正的 ChatGPT 网页调用。当前对话仍由未升级的服务承载，不能把本地源码测试称为生产网页验收。

## 外部依据

- Rust CommandExt::process_group：`https://doc.rust-lang.org/std/os/unix/process/trait.CommandExt.html`。
- portable-pty 0.9.0 Child 与 std::process::Child 适配：`https://docs.rs/portable-pty/0.9.0/portable_pty/trait.Child.html`。
- OpenAI 文件输入工具参考：`https://developers.openai.com/plugins/reference`，核对日期 2026-09-09。该参考支持文件参数约定，不构成本仓库已完成传输功能的证据。
