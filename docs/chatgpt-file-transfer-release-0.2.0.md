# Remote Hosts 0.2.0 文件传输实现与发布状态

日期：2026-09-09。开发设备：MacBook-M2-Max。
项目：`/Users/jinliang/Workspace/remote_hosts`，当前 `main` 工作树，未提交或推送。

## 续办部署状态（2026-09-09 20:30 Asia/Shanghai）

三台生产设备已完成 0.2.0 安装。通过托管 SFTP 传入 NAS 的二进制为 22,573,640 字节，SHA-256 与候选包一致；本次传输未被安全检查拦截。NAS 已备份二进制、SQLite 和 Caddy site，运行进程哈希与候选一致。两台 Mac 已备份旧二进制，并通过 launchd PID 与新版运行标记对应检查；网关 devices_list 确认两台设备在线且报告 0.2.0。

Mac Studio 的另一项目测试终端结束后，升级脚本的空闲门禁通过，才执行重启；没有取消该终端。其 SSH exec 通道曾发生 completion frame 缺失，遵守连接冷却后改用托管 PTY 完成升级。

备份目录：
- NAS：`/opt/remote-hosts-code/releases/before-0.2.0-20260909T201825`
- MacBook：`/Users/jinliang/.local/share/remote-hosts-code/releases/before-0.2.0-20260909T201837`
- Mac Studio：`/Users/jinliang/.local/share/remote-hosts-code/releases/before-0.2.0-20260909T202828`

公网 `/healthz` 报告 0.2.0、file_transfer=true、上限 64 MiB。ChatGPT 插件管理页刷新后已展示 file_upload/file_download。两台 Mac 的公网验收均通过：2 MiB 二进制上传/下载、HTTP Range、幂等重试、禁止覆盖、错误校验和拒绝，以及范围读取、语法树、精确编辑、过期版本拒绝、搜索和终端执行。验收临时 OAuth grant 已撤销。详细回执见 `chatgpt-file-transfer-acceptance-0.2.0.json`。用户开启 Chrome 扩展文件访问权限后，真实 ChatGPT 附件上传成功，本机独立核对 51,200 字节及 SHA-256 一致；网页实际执行 file_upload、operation_get、file_download 并返回 completed 和下载链接。独立从同一公网链接下载得到 HTTP 200，内容与原始附件逐字节相同。ChatGPT Python 沙箱的额外下载核对因 DNS 解析失败未完成，这不计为沙箱下载通过；MCP 上传/导出及公网下载已验证。测试文件保留在 `/Users/jinliang/Workspace/.remote-hosts-code-browser-020/received.txt`。网页证据：https://chatgpt.com/c/6aa15211-1ad0-83e8-bdc7-830dc65e7170 。

验收脚本默认 Python User-Agent 被 Cloudflare 以 1010 拒绝；`scripts/check-code-gateway.py` 加入明确的 `RemoteHosts-Acceptance/0.2.0` 标识后元数据请求正常，未改变防护规则。原始 dist 发布包保持不变。

以下为先前候选构建阶段的历史记录，不代表当前安装状态。

## 原候选阶段结论

0.2.0 文件传输首版源码、Mac ARM64 与 Linux AMD64 musl 发布包已完成，最终 114 项功能测试全部通过。但**生产升级未完成**。最后通过现有 SSH/本机入口重新核对：MacBook、Mac Studio 的已安装 Agent，以及 NAS 网关仍为 **0.1.0**；NAS 服务状态为 active。

平台安全检查阻止了两个实际工具调用：创建独立 launchd 更新器启动脚本，以及通过 SSH 将 NAS 发布文件传入服务版本目录。工具没有提供具体触发规则，也没有返回可执行的授权确认入口。本轮未绕过这些检查，没有创建该自动启动任务，没有替换生产二进制，没有重启生产 Agent/Gateway/Caddy，没有改写历史生产验收 JSON。

Mac Studio 已通过原有 SSH 通道收到并校验 **0.2.0 候选二进制及升级辅助脚本**，但没有安装或重启。候选包存在，不等于正在运行新版本。

## 新增两个工具

| 工具 | 首版实现 |
|---|---|
| `file_upload` | 使用文件参数中的临时下载地址，把二进制文件流式写入指定设备工作区的相对路径。默认拒绝覆盖；替换必须提供现有 SHA-256 `expected_version`。支持可选入站 `sha256` 校验。 |
| `file_download` | 将指定工作区文件制作临时快照、校验后流式传到网关，返回临时 HTTPS 下载链接、文件大小、SHA-256 和 MCP resource_link。源文件不修改。 |

源码工具目录从 13 项增加至 15 项。现有代码/终端工具参数保留，文件上传使用 `code:write`，导出使用 `code:read`；导出仍明确标为非只读、开放数据边界的操作，不能将文件外传伪装成不产生副作用的读取。

上传工具声明 OpenAI `_meta["openai/fileParams"]`，文件对象声明 `download_url`、`file_id`、`mime_type`、`file_name`。工具目录序列化测试验证元数据没有被 rmcp 丢弃。模型不需要在 JSON 中搬运 Base64 或二进制正文。

## 数据与权限边界

单文件上限 **64 MiB**，网关临时对象总量上限 **512 MiB**、数量上限 **512 个**。下载链接有效期最长 **15 分钟**，对象可访问期最长 **1 小时**。过期链接立即失效；磁盘过期对象在后续接收请求时清理，不宣称空闲网关会在整点主动擦除物理文件。

设备凭据和原操作的设备/所有者/权限均核对，文件路径通过工作区目录能力访问；拒绝路径穿越与符号链接逃逸。入站临时文件在同一目标父目录内创建，首次发布使用原子不覆盖操作；替换再次核对现有哈希并原子重命名。版本校验不是跨所有外部编辑器的文件系统 CAS。

文件来源限制为支持的 ChatGPT 文件存储域名或本人的网关文件链接。要求 HTTPS、禁止用户信息和重定向，解析后检查并固定公网目标地址，不向外部下载源转发设备凭据。生产环境的 DNS/代理兼容性尚需实测。

入站签名 URL 不进入长期任务 JSON；短期来源状态有过期时间，完成回执后删除对应键。SQLite/WAL 不提供即时物理擦除保证。下载链接本身是短期持有者权限，不能公开传播私人文件链接。

长文件操作期间发送独立会话心跳，不让慢传输自动变成设备离线。新进程写入版本/PID 标记，健康接口、设备 Hello 和工作区结果可分别报告实际版本。

首版支持流式搬运、导出内部最多三次发送尝试、精确幂等重试，以及下载 HTTP Range。**未实现任意断点的上传续传、独立传输取消工具或无限大小文件**。无法确认副作用的操作仍沿用 outcome_unknown 保护，不能换新键盲目重跑。

## 实际测试与构建

最终组合命令退出码为 0：

| 测试/检查 | 结果 |
|---|---:|
| remote-hosts-code 单元测试 | 15 通过 |
| 新 file_transfers 集成测试 | 4 通过 |
| 原有网页 integration | 3 通过 |
| review_regressions | 14 通过 |
| terminal_regressions | 14 通过 |
| 旧 remote-hosts-mcp | 64 通过 |
| 功能测试总计 | **114 通过，0 失败** |
| cargo fmt --check | 通过 |
| 严格 cargo clippy --all-targets -- -D warnings | 通过 |
| cargo check --workspace | 通过 |
| macOS ARM64 release 构建 | 通过 |
| x86_64-unknown-linux-musl release 交叉构建 | 通过 |

两个既有性能用例仍按原设计 ignored，不计入上述功能测试数量；本轮没有宣称重新测得公网性能提升。

新增测试包含真实本地 HTTP 的 **2 MiB 二进制导出、完整下载、Range、重复发送、到期链接和续发链接**，以及错误设备、错误哈希、超限、路径逃逸、默认不覆盖、并发创建、临时文件清理和文件参数元数据。入站工作区写入边界有单元测试；**真实 ChatGPT 附件传入、生产 HTTPS 往返、NAS 运行及两台设备新版在线链路尚未验收**。

测试实际暴露并修复了重复大文件发送时网关过早返回成功、发送端仍在写 body 导致连接中断的问题。现在重复请求也完整限量读取并核对字节，验证后再返回相同结果。一次整套编译达到旧执行超时，另一次回归发现历史测试写死了 13 个工具；已分别重跑、更新到 15 项，并保留原有类型/参数测试，没有删除失败测试。

关键日志在项目 `target/file-transfer-020/`：`transfer-tests-final.log`、`tests-verified.log`、`clippy-verified.log`、`workspace-verified.log`、`build-macos.log`、`build-linux.log`。早期失败日志保留以便审计。

## 发布包位置

MacBook 上可直接使用的发布目录：

```text
/Users/jinliang/Workspace/remote_hosts/dist/remote-hosts-code-0.2.0/
```

包含：

```text
remote-hosts-code-macos-arm64
remote-hosts-code-linux-amd64
upgrade-code-agent.py
upgrade-code-gateway.py
check-code-gateway.py
SHA256SUMS
SOURCE_SHA256SUMS
```

候选二进制 SHA-256：

```text
1d66662d52186a5678b8422ab03d406d3ed98a4abcb6560751c294112af90b08  remote-hosts-code-macos-arm64
e3d439348403096bd970698c5ac376d14db8ed7045fe8f97cfe5c6ba26fc80cf  remote-hosts-code-linux-amd64
```

Mac Studio 暂存目录：`/Users/jinliang/.local/share/remote-hosts-code/releases/0.2.0/`，Mac 候选包校验和与上面相同，尚未安装。NAS 包未通过本轮被拦截的传入操作送达。

升级辅助脚本与新版公网验收脚本已生成并通过 Python 语法检查，但**升级和公网验收脚本没有实际运行通过**。不能将脚本存在当作生产已升级、回滚已验证或原生网页附件功能已验收。

后续生产验收顺序仍为：审核并通过部署操作授权；核对候选哈希；备份现有二进制/网关数据库；确认没有需要保留的活动终端；替换并检查实际运行版本；验证 Caddy 下载响应头；运行新版双设备代码、终端和二进制往返验收。已有 `chatgpt-code-gateway-acceptance.json` 仅是历史版本记录，不属于 0.2.0 的生产验收证据。
