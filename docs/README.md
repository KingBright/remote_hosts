# Remote Hosts 文档索引

[项目中文 README](../README.md) | [English README](../README_EN.md)

本目录按照“产品与迭代、架构、部署、ChatGPT 代码网关、发布证据”组织。公共仓库不承担实时运行状态台账职责；真实部署拓扑、设备状态和现场 acceptance 应由独立私有 ops/runtime 系统保存。

## 首选入口

- [Repository Content Model](repository-content-model.md)：公共产品源码、私有部署配置、实时运行状态三层边界及提交规则。
- [架构与运行模型](architecture-and-runtime.md)：SSH 连接复用、Workspace、PTY、资源协调、传输、安全边界。
- [部署与运维](deployment-and-operations.md)：服务安装、更新、路径、诊断和故障恢复。
- [ChatGPT Code Gateway](chatgpt-code-gateway.md)：OAuth/MCP 网关、设备 Agent、代码/终端/文件能力。
- [Code Gateway 从零部署与新设备接入](code-gateway-deployment.md)：Gateway、公网/Cloudflare、ChatGPT OAuth、Linux/macOS/Windows 新设备加入、验收与安全注意事项。
- [产品迭代流程](product/README.md)：问题清单、验证、自动发布与验收纪律。
- [统一问题清单](product/BACKLOG.md)：所有 RH 编号问题的阅读版，事实源是 `product/backlog.json`。
- [0.5.0 路线图](product/ROADMAP-0.5.0.md)：历史规划与设计依据；当前执行重点见 `product/NEXT.md`。

## 平台与基础设施

- [Infrastructure Topology](infrastructure-topology.md)
- [Instance Sync](instance-sync.md)
- [Windows Installation and Operations](windows.md)

## 发布证据

版本目录位于 `docs/releases/`。新的公共 release evidence 只应保存固定输入、测试门禁、artifact manifest/SHA 和不绑定具体实例的发布说明。早期版本目录中还保留了一批真实部署 acceptance/deployment/targets 记录，它们属于历史 **legacy evidence**，不是当前推荐的数据模型，也不能作为实时状态事实源；后续应在保留审计链的前提下迁往私有 ops/archive。

当前最近一次不可变 packaged release 为 **0.9.0**。该固定源码 pipeline 通过 **372 项测试：239 Rust + 133 Python，0 失败**，并完成 macOS ARM64 与 Linux x86_64-musl release 构建；package manifest SHA-256 为 `8e40977a4fc83d6999c89bcccf70bae160b5aca58cf89f5f82b4302ba4b9f5e2`。这些都是 release facts；当前 `main` 还可以包含尚未重新打包的后续源码改动，也不描述任何一套实例当前 installed/running/accepted 状态。

## 文档纪律

1. “源码实现”“候选验证”“已安装”“实际运行”“现场验收”是五种不同状态。
2. 被宿主在执行前拦截的动作记录为未执行，不能算测试失败或成功。
3. 运行中的原任务优先继续观察，不为取结果重新执行副作用。
4. 任何密码、设备 token、真实设备 UUID、私有 endpoint、个人绝对路径、签名文件 URL、完整私有命令不得新增到公共文档或提交。
5. 生成的大型二进制发布包、临时锁和缓存不进入文档提交；公共仓库只保留可复现的 release hash/verification，不新增真实 deployment receipts。
