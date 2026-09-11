# Remote Hosts 文档索引

[项目中文 README](../README.md) | [English README](../README_EN.md)

本目录按照“产品与迭代、架构、部署、ChatGPT 代码网关、发布证据”组织。不要用旧版本说明覆盖当前运行事实；版本状态以对应 `docs/releases/<version>/` 的机器回执为准。

## 首选入口

- [架构与运行模型](architecture-and-runtime.md)：SSH 连接复用、Workspace、PTY、资源协调、传输、安全边界。
- [部署与运维](deployment-and-operations.md)：服务安装、更新、路径、诊断和故障恢复。
- [ChatGPT Code Gateway](chatgpt-code-gateway.md)：OAuth/MCP 网关、设备 Agent、代码/终端/文件能力。
- [产品迭代流程](product/README.md)：问题清单、验证、自动发布与验收纪律。
- [统一问题清单](product/BACKLOG.md)：所有 RH 编号问题的阅读版，事实源是 `product/backlog.json`。
- [0.5.0 路线图](product/ROADMAP-0.5.0.md)：协作效率、独立发布、变更集、批量同步和宿主能力闭环。

## 平台与基础设施

- [Infrastructure Topology](infrastructure-topology.md)
- [Instance Sync](instance-sync.md)
- [Windows Installation and Operations](windows.md)

## 发布证据

版本目录位于 `docs/releases/`。其中：

- `RELEASE.md`：面向维护者的人类可读说明。
- `verification*.json`：固定输入、测试门禁和日志哈希。
- `deployment*.json`：真实安装/运行/验收状态。
- 设备子目录：该目标的升级器、验收和恢复证据。

当前代码网关版本为 **0.5.0**，NAS、MacBook 与 Mac Studio 均已在线运行 0.5.0。固定源码快照通过 338 项测试；首次自动标准验收因 run-id 格式 bug 未执行，该 bug 已修复并有回归测试。完整验收重跑仍必须以实际执行回执为准。

## 文档纪律

1. “源码实现”“候选验证”“已安装”“实际运行”“现场验收”是五种不同状态。
2. 被宿主在执行前拦截的动作记录为未执行，不能算测试失败或成功。
3. 运行中的原任务优先继续观察，不为取结果重新执行副作用。
4. 任何密码、设备 token、签名文件 URL、完整私有命令不得写入公开文档或提交。
5. 生成的大型二进制发布包、临时锁和缓存不进入文档提交；保留哈希和机器回执即可。
