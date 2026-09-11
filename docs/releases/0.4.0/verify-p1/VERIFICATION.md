# Remote Hosts 0.4.0 实际核验

日期：2026-09-11（UTC）。项目：`/Users/jinliang/Workspace/remote_hosts`。

## 结论

**0.4.0 尚未上线，当前最终候选没有通过完整验证。** 本次确认网关和 MacBook 仍上报 0.3.5，Studio 上报 0.3.0，两台设备在线。本次未替换任何生产二进制、重启服务或取消其他任务。

## 原任务核对

`docs/releases/0.4.0/deployment-n2.json` 明确为 `needs_recovery`，阶段为 `await_verified_build`，错误类型为 `TimeoutError`，`steps` 为空。发布器是在等待构建结果时退出，不是完成升级后缺少回执。

未找到原等待器所需的 `target/iteration-040-n2/targeted-result.json`、`target/iteration-040-n2/pipeline.json`，也未找到 `workflow-acceptance-n2.json`。原操作输出分别只能证明一次读取因缺文件失败、发布脚本语法检查通过、等待器启动和收尾发现发布未完成。不能据此报告最终构建或现场验收完成。

发布器代码在缺少管线结果时允许持续等待最多3600秒，却没有先验证实际构建任务的身份。应记录为产品问题 RH-047，不能继续用启动确认代替任务产出。

## 本次新执行的验证

确认旧构建槽已释放、没有存活编译器之后，已创建独立源码快照：

- 路径：`target/source-snapshots/040-p1`
- 输入：133 个文件，3,240,971 字节。
- snapshot_id：`336d06e6ee42e68db8e15227b18959f8884a53f409e58a8339f54c04a0b3f19e`
- 原执行操作：`704a95a2-e360-4510-917f-7babff7d6541`，退出码1，输出捕获完成。

管线 `target/iteration-040-p1/pipeline.json` 的第一道格式检查退出1。具体位置：

| 文件 | rustfmt差异附近行号 |
|---|---:|
| `crates/remote-hosts-code/src/agent.rs` | 319 |
| `crates/remote-hosts-code/src/transfer_control.rs` | 146 |
| `crates/remote-hosts-code/src/transfer_receiver.rs` | 386 |

这些是格式差异，并非已经证明业务代码错误。但门禁停止后，Clippy、Rust/Python功能测试、工作区检查、两平台发布构建和现场验收均未执行。旧一轮通过的测试不能覆盖最后追加的源码改动。

原始格式日志：`target/release-slot/checkout/target/source-verification/20260911T025042Z-5b932e3e/fmt.log`，SHA-256为 `ffd8eca7fd684b593655c2c7362754fe34e4ba476ef189e819e85a8fd3b9e26e`。

## 修正尝试的准确边界

随后提交的“确认输入未变、格式修正、新快照及完整验证构建”调用被宿主在执行前拒绝，返回：`因 OpenAI 无法确定请求的安全状态，已拦截此工具调用。` 该调用未取得操作ID，不能声称格式已经修复或复验正在进行。未通过更换工具或包装重放。常规发布授权仍然有效，问题不是等待用户重新批准。

## 接下来应如何完成

1. 在正常授权的执行环境完成三个文件的格式修正，保留原快照和所有失败证据，不关闭任何校验或放宽权限。
2. 从修正后的源码捕获新快照并实际启动验证/构建生产者，保存其操作ID和管线报告。不要先启动等待一个不存在报告的发布器。
3. 取得完整通过回执、快照身份和产物清单后，再按先网关后Agent的既有流程发布。用户已明确选择NAS、MacBook和Studio，无需再以旧“Studio延期”规则阻止它。
4. 核对运行哈希、会话和就绪记录，对两台Mac分别完成代码、终端、文件及新工作流验收，最后更新发布状态。

当前无证据需要回滚现有正常运行的0.3.5/0.3.0，也不应再等待已经超时退出的旧发布任务自行完成。JSON记录见同目录 `verification.json`。
