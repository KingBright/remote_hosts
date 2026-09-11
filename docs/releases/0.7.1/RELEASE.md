# remote-hosts-code 0.7.1 发布记录

## 结论

0.7.1 是 0.7.0 文件通道升级后的热修复版本。NAS Gateway、MacBook-M2-Max、Mac Studio 最终均运行 0.7.1。固定源码候选通过 348 项测试（238 Rust + 110 Python，0 失败）、格式检查、严格 Clippy、workspace check，以及 macOS/Linux release 构建。

- 固定快照：`c10c0d4e424b2b2f410fa57b065ac01b4c10903915ad79c8cda9f4a77b4d1312`
- Manifest SHA-256：`5945696eba2a1ecc19a2a3da74e1b1706ef83c6d404b8e36d42ba69d2aafca36`
- macOS binary SHA-256：`c1fb187610e97e01e6e5a5ea2019f9c412aed86101b3c9bdde9318c1b2ed7bb4`
- Linux binary SHA-256：`e940cb24d09fdc9cdb8d8ecdad60324c54b8492d829f8713f26f392973d2257d`
- 核心提交：`7071d9d fix(code): retry transient gateway receiver failures`
- 验收器修复：`2b1619c fix(release): compare stable idempotent receipts`

## 0.7 主线能力

- 文件传输默认上限保持 64 MiB；0.7+ Agent 显式协商后，服务器允许请求最高 256 MiB。
- 设备上报 `transfer_limits`：协议版本、默认值、硬上限、4 MiB checkpoint。
- 接收端和 Agent 保留至少 256 MiB 的存储安全余量检查。
- `operation_get` 对上传源授权返回 `available / expired / required`，过期授权给出结构化恢复动作。
- 新 runtime feature：`large_file_transfer_v1`、`source_authorization_status_v1`。
- 0.6 已有的持久 change-set、`change_resume`、`workspace_gc`、operation lifecycle 和 Workspace 事件继续保留。

## 现场发现并修复的问题

### 1. 旧 Agent → 新 Gateway 的永久 409 误判

0.7.0 发布时，MacBook 0.6 Agent 向 Gateway 导出约 9 MiB 的公共 Agent 包。前 8 MiB 已确认；最后一段数据也完整写入 staging，且三段 SHA 与源文件完全一致，但 durable offset 仍停在 8 MiB。接收端把 offset/进度持久化阶段的内部错误统一映射成 HTTP 409，旧 Agent 将其视为永久失败。

0.7.1 将错误分层：真正的 identity / offset / checksum / 已提交 artifact 冲突继续返回 409；数据库、IO、状态提交等暂态内部错误返回可重试 5xx。发送端随后重新读取 durable offset，并安全重发同一 chunk。

修复后真实发布链路再次传输 9,089,298 字节：操作 `9e1717bf-20be-4b9f-9fa8-e8219d842cbf` 实际 `retry_count=1`、`resumed_bytes=4194304`，最终 SHA 为 `dc4b0ddecc55110be8aab02049ab6b89bcfdfdbfe41046cbafc8e546242fd059`。同一 artifact 随后通过操作 `c7784092-8680-4a48-ab7c-9e0eaf7640f4` 完整导入 Studio，字节数和 SHA 一致。

### 2. 验收器把实时 lifecycle 当成持久回执

MacBook 已成功升级后，旧验收器要求重复 `code_apply_edits` 返回的整个对象完全相等。0.6+ 的 `operation_lifecycle` 含实时的设备回执队列观察，不属于持久业务回执，因此这个断言过严。

修复后只从幂等业务回执比较中排除 `operation_lifecycle`；`operation_id` 和其余业务字段仍严格一致。修复通过完整 Python 测试：112 项通过，0 失败，1 项显式跳过。

### 3. Studio 排空与网络窗口

首次自动 Studio 更新在安装前发现 1 个活动 terminal 并安全停止，未修改服务。该 terminal 对应已经结束网关任务但本地仍残留 running 状态的浏览器回归；经精确识别后使用 `terminal_cancel` 收口。

第一次 fresh updater 随后在候选已切换后遇到五个 lane 连续 `device polling unavailable`，readiness 超时并自动回滚到 0.6.0；回滚验证成功。网络恢复后再次使用同一 SHA、不同不可变候选路径创建 fresh one-shot job，最终升级成功：PID 54296、9 次稳定采样、约 22.26 秒、五个 lane 全部验证，maintenance lease 已释放。

## 最终现场状态

- NAS Gateway：0.7.1，Linux binary SHA 与发布包一致。
- MacBook-M2-Max：0.7.1，PID 58852，10 次稳定采样，五 lane 全验证。
- Mac Studio：0.7.1，PID 54296，9 次稳定采样，五 lane 全验证。
- 两台 Mac 均通过 Remote Hosts 原生代码创建/读取/符号解析/幂等编辑/终端执行/清理验收。
- 实际文件通道完成 9,089,298 字节 device→Gateway（含一次 4 MiB checkpoint 恢复）和 Gateway→Studio 往返，SHA 一致。

## 明确未宣称通过的边界

从当前对话执行修正版整包 OAuth/MCP 验收脚本时，宿主在执行前进行了安全拦截，脚本没有运行，因此不把该 wrapper 记为通过；原生 Remote Hosts 功能验收单独记录。

Gateway 与两台 0.7.1 Agent 已上报 256 MiB 协商能力，但当前会话的宿主 `file_upload/file_download` schema 仍把 `max_bytes` 限制在 64 MiB。源码门禁覆盖了 65+ MiB 协商逻辑，线上也验证了双向 9 MiB 和 checkpoint 恢复，但 **>64 MiB 的宿主原生现场双向往返仍待宿主 schema 刷新后补验收**。

详细机器可读记录见 `deployment-final.json` 与 `native-acceptance.json`。原始 publisher 的临时工作目录不进入 Git；关键失败与恢复事实已经提炼进这两份长期证据和本文，不用大型 Agent 包或临时 lock 充当发布证明。
