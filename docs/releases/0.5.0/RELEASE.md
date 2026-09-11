# Remote Hosts Code Gateway 0.5.0

日期：2026-09-11。本文记录 0.5.0 的实现、验证、运行状态和仍未闭合的验收，不把“安装成功”与“现场验收成功”混为一谈。

## 交付结果

固定源码快照 `68dc1227a55c7c1305235a07198da13dbd9292043c3a92e4a883c71eab2b95d8` 完成完整流水线：

- Rust：230 项通过。
- Python：108 项通过。
- 合计：338 项通过，0 失败。
- `cargo fmt`、严格 Clippy、workspace check 通过。
- macOS arm64 与 Linux amd64 musl release 构建通过。
- 发布 manifest SHA-256：`d29ead4d6c2f30fde934cab86e29f5dc91c74cd33eec476c9348712938920c17`。
- 源码交付基线提交：`e6b68417f01850e37b5c53394002670dffadbf6c`。

## 0.5.0 主要能力

1. **按目标独立发布**：网关先升级，两台 Agent 各自 drain、安装、就绪与验收；一个目标失败不撤回健康目标。
2. **维护排空协议**：升级前停止领取新的执行/写入/传输工作，状态、取消和结果回执仍可继续；不强杀未知业务任务。
3. **终端状态复制**：终端退出状态可进入原 `operation_get`，减少为了知道退出码而创建新的状态读取任务；实际输出仍使用独立字节游标读取。
4. **工作区事件**：按工作区保存有界状态转换，支持断线后的 cursor replay，并隔离不同工作区。
5. **完整变更审查**：`code_diff` 可包含非忽略的未跟踪文本/二进制文件，不再让核心未跟踪代码表现成“空 diff”。
6. **Manifest 文件集同步**：`files_sync` 先 plan，再只携带变化文件的 bundle；目标版本被绑定，重放同一 manifest 不覆盖并发用户修改。
7. **结构化诊断**：稳定 `error_code / stage / outcome / recovery_action`，默认禁止自动重放未知副作用。
8. **紧凑响应与 schema 能力发现**：结构化内容保持完整，可选压缩文本通道；网关报告工具目录哈希和 Agent 实际功能。

服务端工具目录由 18 项增至 19 项，新增 `files_sync`。宿主如果仍缓存旧 schema，可能继续只暴露较少工具；这是宿主能力暴露状态，不代表服务端未升级。

## 当前运行状态

最后通过原生 `devices_list` 核对：

- NAS gateway：0.5.0。
- MacBook-M2-Max：0.5.0，升级器报告稳定就绪、五通道通过。
- Mac-Studio：0.5.0，升级器报告稳定就绪、五通道通过。
- 两台 Agent 的 receipt delivery 均无 pending/blocked/sending 积压。

## 为什么首次自动验收显示 partial

两个 Agent 的安装与稳定回连都成功，失败发生在**功能验收入口**。`publish-code.py` 生成：

```text
release-0.5.0-<device-id>
```

而 `check-code-gateway.py` 的 `--run-id` 只接受字母数字与连字符，因此两个标准验收子进程都立即退出，日志只有：

```text
run-id must be alphanumeric with optional hyphens
```

所以 `deployment.json` 中记录的是 `CalledProcessError` / `needs_recovery`，不是 Agent 没有返回、服务崩溃或升级回滚。

修复将语义版本中的点规范化为连字符，例如：

```text
release-0-5-0-<device-id>
```

并增加专门回归测试，测试已通过。随后从当前对话补跑完整标准/协作验收的组合命令被宿主安全检查在执行前拦截，因此**不能把完整自动验收改写成 passed**。当前事实是“三端运行 0.5.0，完整自动验收待补”。

## 证据

- 构建流水线：`target/iteration-050-s5/pipeline.json`。
- 首次发布聚合状态：`publish-s5/deployment.json`。
- 各 Agent 升级回执：`publish-s5/<device-id>/updater.json`。
- 首次标准验收日志：`publish-s5/<device-id>/acceptance.log`。
- 源码提交基线：`target/iteration-050-s5/git-baseline.json`。
- 0.5.0 路线与验收目标：`../../product/ROADMAP-0.5.0.md`。

## 后续

- 在正常获准的执行环境补跑标准验收与 `check-collaboration.py`，只验收，不重复安装已健康目标。
- 继续解决宿主工具目录仍可能停留在旧快照的问题；直连 MCP 通过不能替代原生 ChatGPT 工具暴露验收。
- 将按目标独立发布器从版本脚本继续收敛为长期稳定发布入口，并保持未知副作用不自动重放。
