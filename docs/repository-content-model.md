# Repository Content Model

Remote Hosts 的公共仓库、某一套真实部署的配置，以及实时运行状态是三种不同的数据域。它们不能互相代替，也不应该继续混在同一个 Git 历史里。

本文定义从现在开始的仓库边界。

## 1. 三层模型

### A. Public product repository

公共产品仓库回答的问题是：**Remote Hosts 是什么、如何构建、如何部署一套新的实例、代码是否通过可复现验证。**

这里可以放：

- `crates/`、`migrations/`、通用 `scripts/`、测试和 fixtures；
- 协议、架构、权限模型、平台支持和通用运维文档；
- 使用 `example.com`、`YOUR_USER`、假 UUID、保留测试网段的示例；
- 可复现的 release 元数据，例如版本号、固定源码指纹、测试数量、artifact SHA-256；
- 不绑定某个组织或某台机器的 Caddy/systemd/Cloudflare 配置模板；
- 已脱敏、任何人都能在自己的环境复现的故障案例；
- **脱敏后的真实部署案例**，用于解释网络拓扑、选型理由、端口关系、故障经验和容量边界。

脱敏案例允许保留真实的**结构**，例如“Cloudflare 443 → origin 8443 → Caddy → loopback 18787 → Gateway，两个 Agent 主动出站连接”，因为这些信息具有可复用价值；但域名、设备名、UUID、个人路径、账号、凭据、真实 IP 以及“当前在线/当前已部署版本”等实例身份和实时状态必须替换或删除。

公共仓库**不负责回答**“我们自己的 Gateway 现在在哪、哪台机器在线、部署了哪个 SHA”。

### B. Private deployment profile

私有部署配置回答的问题是：**某个团队/个人自己的这一套 Remote Hosts 实例具体怎么连。**

它应该独立于公共仓库，推荐放在单独的私有 ops/config 仓库或配置管理系统中。即使使用私有 Git，也不要直接提交 secret 明文。

这里包括：

- 真实公网域名、DDNS、源站地址和端口；
- Cloudflare zone/account/record/Tunnel/Origin Rule 的实例配置；
- NAS/VPS 主机名、SSH 地址、用户名、管理端口；
- 实际 UID/GID、绝对安装目录和存储路径；
- 真实设备名称、稳定的 device UUID；
- 每台设备授权的真实 root；
- Gateway/Agent service override；
- `gateway.json` / `agent.json` 所需的部署参数与 secret reference；这些文件本身含敏感材料时不进入 Git；
- 组织自己的发布目标清单和回滚目标。

建议私有 ops 仓库采用类似结构：

```text
remote-hosts-ops/                 # private repository
  README.md
  environments/
    production.yaml               # no plaintext secrets
  gateway/
    topology.yaml
    caddy/
    systemd/
  devices/
    inventory.yaml
  cloudflare/
    README.md
  release-targets/
  runbooks/
```

密码、token、私钥、Cloudflare API token、`gateway.json`/`agent.json` 中的敏感字段等仍放密码管理器、Vault、OS keychain 或 CI secret store，私有 Git 只保存引用。Workspace/operation/PTy 等运行时 ID 也不应写入私有 Git，而应在运行时动态获取。

### C. Live runtime state

实时状态回答的问题是：**现在到底运行得怎么样。**

它的事实源应该是运行中的 Gateway、Agent、systemd/launchd、数据库、runtime snapshot 和现场验收器，而不是 Git。

包括：

- device online/offline；
- 当前安装/运行版本；
- PID、service state、readiness；
- OAuth client/session/token family 状态；
- Workspace/Workspace ID、PTY、operation/operation ID、write lease；
- 当前 transport handshake/reuse；
- 正在进行的升级、drain、rollback；
- 某次现场 acceptance/deployment receipt。

这类状态会过期。把它写进 README 会让几小时后的仓库变成“看起来权威、实际上已经陈旧”的假仪表盘。

## 2. Release evidence 与 deployment evidence 必须分开

公共仓库可以保存 **release evidence**：

- source snapshot / source fingerprint；
- compiler/toolchain；
- fmt/clippy/test/check 结果；
- artifact manifest 和 SHA-256；
- 平台矩阵；
- 已知限制。

这些证据描述的是“这份源码和这份 artifact 经过了什么验证”。

以下属于 **deployment evidence**，默认不再提交公共仓库：

- `NAS gateway upgraded`；
- `MacBook-A online`；
- 某个真实 device UUID 的 acceptance JSON；
- 私有域名的 health check；
- 某次 production publish 的 SSH 命令、目标路径或设备列表；
- 某套实例的 rollback/resume 操作记录。

deployment evidence 应进入私有 ops/archive，或由部署系统保留。公共 release notes 最多写成不带实例标识的汇总，例如“production acceptance is tracked separately”。

## 3. 公共示例的命名规则

公共文档、测试和模板默认使用：

- 域名：`example.com`、`mcp.example.com`；
- 用户：`YOUR_USER`、`remote-hosts-code`；
- home：`/home/YOUR_USER` 或 `~`；
- UUID：明显的 fixture UUID，不使用真实设备 ID；
- IP：RFC 5737 / RFC 3849 文档地址，或 loopback；
- 机器：`Linux-Workstation`、`Gateway-Host`、`Device-A`。

测试如果需要 hostname，也应该使用 `.test` / `.example`，不要使用维护者自己的公网域名。

## 4. 公共仓库推荐保留什么

### 源码

保留所有实现产品行为的 Rust/Python/Shell/PowerShell 源码，只要它们是通用实现而不是某个实例的硬编码运维脚本。

### 通用部署模板

可以保留：

- generic systemd unit；
- generic Caddy template；
- generic Cloudflare/Tunnel instructions；
- generic installer/updater；
- example config schema。

不应该保留：

- 写死个人域名的 DNS 脚本；
- 写死某台 NAS 路径、UID/GID、SSH 端口的安装脚本；
- 只能给一套具体生产环境使用的 publish driver。

如果一个脚本确实值得公开，应参数化后再留在产品仓库。

### 文档

公共文档描述：

- capability；
- protocol；
- threat model；
- generic setup；
- reference topology；
- sanitized real-world case study；
- upgrade contract；
- troubleshooting methodology。

可以描述从真实部署抽象出的拓扑，但必须明确标注为 **sanitized case study / reference architecture**，不能让读者误以为它是维护者当前的实时拓扑。

## 5. 本仓库的历史遗留

早期迭代为了快速闭环，把产品开发、发布证据和维护者自己的生产实例验收都放进了一个仓库，因此历史 `docs/releases/`、部分旧脚本和测试中仍然存在真实域名、设备名称、绝对路径、设备 UUID 等实例标识。

这些文件是 **legacy evidence**，不是当前推荐的数据模型，也不能作为当前 live state 的事实源。

迁移原则：

1. 不在没有审计副本的情况下批量删除历史发布链；
2. 先停止新增 instance-specific evidence；
3. 将仍有运维价值的现场记录迁入独立私有 ops/archive；
4. 公共仓库保留必要的、已脱敏的 release verification；
5. 如果历史内容包含真正的 secret，应执行 secret rotation，并单独评估 Git history rewrite，而不是只删除 HEAD 文件。

## 6. 新内容的提交规则

提交前问四个问题：

1. **换一个公司/一个域名/一批设备后，这个文件还成立吗？** 如果不成立，多半是 deployment profile。
2. **这个事实一小时后可能变化吗？** 如果会，多半是 live runtime state。
3. **文件是否包含真实 endpoint、设备身份、个人绝对路径或 credential reference？** 如果有，默认不进公共仓库。
4. **它是否是重现某个 release 所必需的源码或验证证据？** 如果是，可以进入公共仓库，但仍需脱离真实部署实例。
5. **它是不是有教学价值的真实案例？** 如果是，可以保留结构和决策，先做实例身份与实时状态脱敏。

## 7. 本地防误提交目录

公共仓库预留以下本地目录并通过 `.gitignore` 排除：

```text
ops/private/
deploy/local/
docs/live/
.local-deployment/
```

它们只用于临时本地运维材料，不作为团队同步方案。需要多人协作的真实部署配置，应使用独立的私有 ops 仓库。

## 8. 状态表述规范

以后统一使用以下词语，避免一句“已经发布”混淆五种状态：

- **repository version**：仓库源码声明的版本；
- **verified release candidate**：固定源码门禁已通过；
- **packaged artifact**：不可变 artifact 已生成并有 manifest；
- **installed version**：某台目标机器磁盘上已经安装；
- **running version**：目标当前进程实际运行；
- **accepted deployment**：现场功能验收已通过。

前 3 项可以由公共产品仓库记录；后 3 项属于具体部署实例和实时状态。

这条边界是产品架构的一部分，不只是文档风格。
