# HO5 受限维护闭环设计与验收合同

状态：只读评估及隔离协调器候选完成；29项HO5 fixture、85项组件测试通过。真实OS写操作、特权服务、权限配置、DNS修改及重启均未部署。评估时间 2026-10-10 07:09–07:16 UTC。证据见 [只读评估](ho5-maintenance-assessment-20261010.json)，测试合同见 [测试合同](ho5-maintenance-test-contract.json)。

## 分工与快照有效期

用户后来取消了外部agent的后续部署测试安排；当前HO5环境及Linux构建核验由RP任务负责。本任务处理RH源码提交/推送与隔离维护候选，不重复RP的实机维护或环境检查，不接管任何外部operation。上述07:09–07:16观察仅在各自时间成立，旧staged/权限结果不能当作当前验收。未来实际维护前须明确唯一执行方并取得原回执。

## 结论

HO5 的包依赖维护和重启可优先使用已经存在的 rpm-ostree / logind 系统 D-Bus 服务。当前受限 Agent 的非交互检查返回包操作允许、CanReboot=yes；无需关闭 NoNewPrivs，也无需为这两项先安装通用 root helper。该结果只覆盖当前会话的检查，未证明一次真实维护成功，更未证明退出登录或重启后的持久权限。

DNS 的准确范围是现有 Sing-box 的指定域名规则，不是 NetworkManager 全局 DNS。当前未发现标准 Remote Hosts 管理 helper；没有验证到可供 RH 写入该配置的受限服务。DNS 仍需要后续一次性本机配置或直接由管理员执行明确动作。本评估没有启用它。

实现优先复用普通维护 Store、计划摘要与 Gateway 原 operation/回执，再用成熟 OS 服务执行。不要引入另一个审批平台、root shell、任意 sudoers 或网络管理器。

## 实机证据与边界

- 设备 UUID：02f29fa0-48c1-4e31-a345-90aa88467323；Bazzite 44 / Fedora；UID/GID 1000；Agent 0.10.25；PID 3526722。
- Agent 及子进程 NoNewPrivs=1、CapEff=0；/proc/self/uid_map 与 gid_map 仅映射 1000，setgroups=deny。其他主机账户显示 65534 是 namespace overflow，不能当作真实 host owner。
- 系统 bus 上 rpm-ostree 服务的 GetConnectionUnixUser 返回 UID0。rpm-ostreed、polkit、logind 存在。服务元数据中的 rpm-ostree 用户名与 bus owner 输出应分别保留，不能用 namespace /proc UID 猜测 host 身份。
- 同一受限通道中，pkcheck install-uninstall-packages 不请求用户交互，退出0；logind CanReboot=yes，非交互 reboot 权限检查退出0。未调用 PkgChange/Reboot。
- 发行版公开 rpm-ostree 规则允许 active/local/wheel 的包操作；账号 liang 被列入 wheel。/etc/polkit-1/rules.d 与 /etc/sudoers.d 不能读取，完整有效规则来源未知。因此不把公开规则当作唯一授权原因，也不把本次 yes 当作永久授权。
- /var/lib/remote-hosts-admin、标准 binary/policy、/run/remote-hosts-admin/socket、固定 service 均未找到；systemd unit LoadState=not-found。浅层 /usr/local/bin、/usr/local/sbin、/opt 没有额外 RH 管理 helper。不是全盘“不存在任何 helper”的证明。
- Sing-box 现有服务在运行，binary 为 /usr/local/bin/sing-box；固定配置候选 /etc/sing-box/config.json 为0644，业务配置内容未读取。未重启、改写或检查其DNS规则。
- 新用户级 remote-play-current.service 为 active/running。旧系统 remote-play.service 在 auto-restart，easytier-remoteplay.service active；不在本轮处置范围，不据此判断串流或输入故障原因。

07:12 观察到外部 PkgChange；07:16 事务为空，新增8个包已进入 staged 部署：
alsa-lib-devel、clang、clang-libs、cmake、fontconfig-devel、freetype-devel、libxkbcommon-x11-devel、pipewire-devel。
staged checksum 为 f540a61bc44714e48d98cd23935f4f3f554e5f3c24c7bcb82ef9cb568cb03032；
booted checksum 仍为 9c4755a6e38e11675d1c31697480046e87d9aa2d729c33ffd36993e6c018a16b。
只报告外部工作已 staged、尚未 booted；没有接管事务、重放原命令或重启。

## 可复用源码及明确缺口

| 现有组件 | 可复用内容 | 需要的最小改动 |
| --- | --- | --- |
| crates/remote-hosts-admin/src/filesystem.rs:65–130 | 私有 JSON Store、原子替换、文件和目录 fsync | 增加版本化 HO5 记录，普通与 root 记录分别存储 |
| crates/remote-hosts-admin/src/engine.rs:109–169 | 计划摘要、写前 running、重复请求返回原结果 | 非清理动作单独后端；增加 OS transaction/boot 恢复状态 |
| crates/remote-hosts-admin/src/protocol.rs:9,99–117 | 封闭请求、UUID、摘要 | 当前只有 remove_legacy_remoteplay；新增固定 profile 与 task_id 关联，不能赋予旧动作新含义 |
| crates/remote-hosts-code/src/maintenance_tasks.rs:25–37,163–178 | 原 Gateway 队列与普通维护适配边界 | 当前只有 macOS 能力；Linux 必须实现并实测后再声明新能力 |
| crates/remote-hosts-admin/src/transport.rs:118–125 | 本机 socket 的身份检查、拒绝非root peer | HO5 namespace 不映射 root；现有 client uid==0 验证会失败。不能把65534加入信任列表 |
| scripts/prepare-admin-helper.py:51–74 | 现有系统服务准备、单独安装机制 | 旧模板只授权 legacy cleanup，不能直接安装为DNS/包/重启执行器 |

普通任务已有的 approve/TaskGrant 不是 Linux 管理员认证，也不能使 macOS 动作在 Linux 上可用。task_id 仅关联已有任务；能力、账号、设备、本机旗标与 OS 权限分别核验。

## 阶段A：普通协调器调用现有系统服务

此阶段不新增系统权限或特权服务。候选普通 CLI 走既有 Remote Hosts terminal/operation，复用 Store 和稳定 request_id。实现后才声明 Linux 维护 capability；线上当前不支持，不通过修改 task_id 或其它通道规避拒绝。

新增固定动作：
1. inspect_ho5_maintenance：只读服务、namespace、非交互权限、OS事务/部署/boot_id。
2. ensure_ho5_system_deps：profile ho5-system-deps-v1 的精确8包，只添加、不卸载。
3. reboot_ho5_staged_deployment：仅已验证并在同一接受计划中授权的目标部署；最多一次 logind 普通重启请求。
4. receipt/verify：恢复同一请求的事实；不会自动重放动作。

包 profile 的8包来自本次外部 staged 差异，只是后续候选范围。profile 最终还需对实际 SDK 需求核验，并绑定版本/SHA；本评估未授权执行它。任何 shell/argv/env、客户端完整配置、任意路径、URL、本地RPM、override、rebase、treefile、live apply、包卸载及隐式升级/重启均拒绝。

使用 OS 的 PkgChange，或严格类型化的 UpdateDeployment（install8、remove空、no-pull-base=true），不要复制发行版解析器。具体 host 版本的 D-Bus 签名及 options 必须固定后通过测试。先检查当前 OS transaction 和 staged baseline；已有用户事务返回 foreign_transaction_busy，仅观察，不能取消或接管。已有 staged 不自动采用，明确 external_effect_observed；如用户后续明确要求只验证它，可以记录 external provenance，不能生成“本任务执行安装”的回执。

调用前核验系统 bus unique owner 及 OS 非交互权限。拒绝、challenge 或 bus 身份变化均停在 awaiting_platform_authorization；保持原请求，不 sudo/pkexec/run0，不产生反复密码提示。后续实现应通过 D-Bus 身份 CheckAuthorization 检查实际客户端，不仅凭进程式 pkcheck 预检；方法执行仍以 OS 最终拒绝为准。

NNP 只限制 execve 提权，系统服务在自己的权限边界执行。现有 polkit 已赋予此账号较宽权限，RH profile 只是应用边界，不能声称它缩小了同 UID 其它程序已有的系统权限。若需要内核/OS 强制仅8包，不能靠这条已有 wheel grant 实现。

## 回执、协调与断线

沿用一个 canonical task_id + 每动作一个 canonical request_id、稳定 idempotency_key；request_id 绑定 task/device/caller/profile/planSHA，task_id不授权。普通协调器以当前 UID 的0700状态目录持久化，record0600；复用 Gateway 原 operation，不给恢复发第二个任务。

准备时记录 boot_id、booted/staged checksum、当前 OS transaction、精确 profile/SHA、namespace和权限检查时间。先 fsync dispatch_intent，再发 OS 调用；接受后保存 transaction owner 的 unique name、地址摘要、阶段及目标 checksum。每步前后都 fsync。超时、断连或回执送达失败先观察原 operation/receipt，不能换 UUID 或 idempotency_key。

维护锁与 OS rpm-ostree 串行事务并用。RH 主机维护范围序列化本任务；不能靠用户态锁阻止用户的另一个 OS 操作，因此每次 dispatch 前仍复查。只允许取消本请求明确确认尚未 dispatch 的工作；不取消外部事务。

OS 接受调用与写入 transaction 之间有不可消除的空窗，不能承诺跨崩溃的 exactly-once。恢复查询真实部署和 OS transaction；无法证明归属时保留 outcome_unknown/recovery_required，禁止自动 attach、重放或取消。外部同名包状态不是本请求执行证明。

保持至少4GiB可用磁盘；预检使用更保守余量。磁盘不足或写前 fsync 失败时不发副作用。不要自动删其它 target/cache/回执，也不建立新的 Cargo target。普通回执不是不可篡改审计，同 UID 进程可修改；root回执也可由本机管理员修改。

## 重启及接续

包事务完成只达到 staged_pending_reboot，不能声明 SDK 已在当前根文件系统可用。先验证目标 checksum、包集合、没有外部事务、计划包含重启、RH当前其它活动已正常排空、logind inhibitors/会话边界允许；不 force、不忽略 inhibitors、不关闭他人终端。

先 fsync reboot_intent 与 reboot_dispatch_count=1，再调用普通 Reboot(false)。丢失响应时保留该count；相同 boot_id 或离线不是“未执行”的证明，不能重发。

重连后只读同 task_id/request_id 回执，比较新 boot_id 和实际 booted checksum：
- boot_id改变且实际checksum/精确包验证通过，才能记录系统依赖步骤 succeeded。
- checksum不同、同 boot_id 或无法观测，分别为 boot_target_mismatch、awaiting_reboot、awaiting_reconnect/recovery_required；不会再自动重启。
- 权限在登录/重启后失效，只继续只读恢复；不自动安装服务或放宽 polkit。
- 原 Agent 用户服务已运行、user@会话/linger安排是否保证无人登录时自启动尚未核实。不修改 linger；后续一次性安装方案必须验证 cold boot 实机接续。

不要新增常驻审批/调度平台。普通 worker 随既有 Agent 启动做只读 reconcile（不重放）；Gateway 按相同operation读取状态。当前能力没有实现此接续，不能把设计当成已闭环。

## 阶段B：后续一次性配置的精确范围

当前包/重启检查允许时，阶段A不需要管理员配置。持久无人登录权限或 DNS 执行才需要后续管理员在本机确认，且必须先准备可审阅、固定摘要的候选包，本轮不安装。

优先扩展现有 remote-hosts-admin 源码组件的封闭后端和持久 Store，新增 Linux 系统 D-Bus 入口 com.remotehosts.Maintenance1；使用现有 systemd/polkit，不创建第二套授权数据库。由于当前 helper 未安装，不能称为“复用已装服务”。

建议一次性安装资源，仅限：
- /var/lib/remote-hosts-admin/bin/remote-hosts-admin（root:root0755；固定二进制与manifestSHA）。
- /var/lib/remote-hosts-admin/policy.json、profiles/ho5-v1.json（root:root0600；目录0700）。
- /var/lib/remote-hosts-admin/receipts、backups（root:root0700；record0600）。
- /etc/systemd/system/remote-hosts-admin.service（root:root0644，Type=dbus、固定BusName、仅本组件）。
- /etc/dbus-1/system.d/com.remotehosts.Maintenance1.conf（root:root0644，仅root拥有名称；仅UID1000发送本组件消息）。

身份固定HO5 UUID、host UID/GID1000；管理员通过实际Agent配置仅核对device_id，不导出token；拒绝策略/二进制的用户可写祖先、symlink、覆盖未知安装和未核对摘要。普通Agent NNP、namespace和服务flags保持，不新增 sudoers、empower组、全局polkit YES、网络权限或 root shell。

系统 D-Bus 客户端通过 bus daemon 的 unique name→GetConnectionUnixUser 校验host root服务；server验证真实发送方hostUID1000、root策略及固定请求。不依靠客户端声明的uid/device/task_id，不接受 namespace overflow65534作为root。现有 root socket 协议保留严格验证；需要单独 namespace测试，不能静默降低验证。

最小策略仅启用实际需要的固定动作。若需无人登录包权限，allowlist为上述8包、install-only；禁止repo刷新/修改、系统升级、卸载、清理、rebase、rollback、kernelargs、localRPM等。若需重启，独立enable，仅对应已记录目标checksum的一次正常reboot；不授予ignore-inhibit/multiple-session强制权限。DNS grant默认disabled；旧 remove_legacy_remoteplay默认disabled，不能用旧安装flag顺带清理。

账户 UID 授权意味着该UID下其它进程可请求允许的固定动作，不承诺逐应用隔离。已有 OS wheel 权限不会被这个helper自动撤销；helper是更窄的RH执行入口，不是账户整体权限收缩。

## DNS的固定动作合同（尚未具备可安装策略）

仅 dns_ho5_singbox_rule_v1：
- 服务必须精确 sing-box.service；真实service fragment、binary及config路径须管理员本机验证并绑定。当前元数据候选为 /etc/systemd/system/sing-box.service、/usr/local/bin/sing-box、/etc/sing-box/config.json。
- 精确域名集合、DNS server/tag、rule锚点目前未知；不得猜测或从敏感完整配置输出。策略在未知时 disabled，不能生成宽泛替换权限。
- 服务端读取原配置，在root侧只修改系统策略指定规则；客户端只有profile_id，不接受JSON Patch/任意配置正文。其它规则及所有代理凭据保持，不进模型/审计输出。
- 固定root worker fd pin/O_NOFOLLOW，拒绝symlink/硬链接/inode或权限漂移；先备份/hash/fsync，再由精确binary校验候选，原子替换并保持owner/group/mode。
- 只有确需新配置生效时执行一次该unit的既定reload/restart；当前服务是否支持reload尚未核实，不能承诺无restart。
- 本地固定DNS探测及RH连通性检查，记录配置摘要、服务generation与结果，不导出配置内容。失败仅在本请求写入版本仍匹配时恢复精确备份，并最多一次恢复该unit；外部修改时停 recovery_required，不覆盖。
- DNS改变可能影响本任务链路，必须系统worker独立于Agent连接收尾；断线读原持久回执，不盲重启Sing-box/VPN/网络。授权只覆盖此规则和其服务，不包含NetworkManager、/etc/resolv.conf、VPN、TUN权限或其它服务。

DNS规则精确值、现有服务身份与本地恢复条件未核实前，不具备“精确范围已准备完毕”的安装条件。后续可由管理员提供/确认root侧策略元数据完成这一项；本轮没有请求密码、读取业务配置或修改DNS。

## 测试与交付顺序

机器可读合同列出32项权限、请求、事务、磁盘、断线、boot、DNS、配置与回归用例。实际实现前，用fake OS bus/transaction与临时私有Store测试副作用前/后崩溃、并发重复、未知归属及拒绝；不做大构建。复用已有 leased Cargo target、固定Rust工具链；每次真实目标执行单独保存证据。

本次已完成只读核查、合同及普通用户协调器候选。模拟故障测试已通过；真实包调用/重启/DNS、无人登录接续均未执行。真实HO5验收仍须分别验证booted包、cold boot Agent恢复、同回执接续及独立应用功能。

系统依赖完成不能直接证明RemotePlay GUI/串流恢复；Pixel source_address_policy_rejected 与 Android404/真机或本地执行器断连要分别验证，不能从主机维护相关性推断根因。

## 本次最小实现与已验证范围

新增 ho5.rs、ho5_bus.rs、ho5_cli.rs，复用 tasks::existing_store / locked_store 与 Store.save_json 的私有文件、原子替换、fsync及fs2锁；复用 executor 的固定环境和有界进程输出。CLI仅有 ho5 inspect、prepare、receipt、verify，没有执行/安装/重启入口。SystemReadBus仅发送固定只读查询，ReadOnlyBus的mutation_enabled=false；默认后端拒绝dispatch，不能以fixture结果开放线上能力。NoNewPrivs及Gateway/Agent flags保持。

计划绑定规范task/request UUID、HO5标签、调用UID、固定8包profile摘要、300秒TTL及boot/staged基线。同请求返回原回执；相同task/action换UUID也不能绕过未闭环记录。typed FixedCall仅在fake后端验证install8/remove空/no_pull_base=true与普通reboot(interactive=false, ignore_inhibitors=false)。写前DispatchIntent/RebootIntent先落盘，超时或接受后保存失败保留unknown且不重发。外部事务及外部staged不接管；恢复需已知本请求事务完成或新boot/checksum/实际8包证据，观察到postcondition不等于证明调用归属。

scripts/check-ho5-maintenance.py复用已有管理组件build slot及Cargo target、普通UID501、Rust1.94.1、offline/locked、jobs1；29项HO5 fixtures包含fake D-Bus解析、无交互固定查询、重复、换UUID、unknown、事务归属、权限失效、NNP/owner变化、锁/symlink、接受后写失败、断线、boot/checksum/包对账。组件总计85测试通过；fmt、tests、clippy -D warnings、build、CLI help、missing receipt均退出0。报告位于 /Users/jinliang/Workspace/Codex/2026-10-08/task/ho5-maintenance-adapter-evidence-02/report.json，SHA256为 0985463a4839b487b6d3183bc0d767b1ffb014a72ced6680768a7dec8df8ba2d，源码快照为 db9f4e12c3445c7a949fefedaf93764d7b7cb5a476d3fb732de08d7ceea22fb8。首轮锁文件不匹配退出101的原证据保留；随后离线生成独立组件锁，未改变仓库Cargo.lock。

明确缺口：当前权限预检使用进程式pkcheck，尚非实际D-Bus客户端授权；没有真实写方法/Finished信号收集、Linux Gateway capability、Agent启动自动接续、RH其他活动排空。HO5 namespace把root祖先显示为65534，现有Store的owner检查会拒绝，不能将65534视为可信root来绕过；实机journal方案尚待验证。完整ENOSPC/fsync/rename/磁盘门槛、并发进程、cold boot、特权helper及DNS故障合同未完成。测试合同逐项标出部分fixture/未实施，不能称32项全部通过或HO5实机闭环。

## 官方依据

- [Linux no_new_privs](https://docs.kernel.org/userspace-api/no_new_privs.html)：继承、不可撤销及execve边界。
- [rpm-ostree daemon model](https://coreos.github.io/rpm-ostree/architecture-daemon/)：系统D-Bus、polkit及串行事务。
- [rpm-ostree administrator handbook](https://coreos.github.io/rpm-ostree/administrator-handbook/)：分层与staged/booted。
- [rpm-ostree v2026.2源代码](https://raw.githubusercontent.com/coreos/rpm-ostree/v2026.2/src/daemon/rpmostreed-os.cxx)：install-uninstall-packages给polkit的details为NULL；单条polkit grant不能按包名约束。这是上游版本机制证据，不是本机RPM字节一致证明。
- [systemd logind官方接口](https://raw.githubusercontent.com/systemd/systemd/main/man/org.freedesktop.login1.xml)：只读CanReboot、正常Reboot及独立inhibitor权限。实现须匹配主机systemd259.9，不能直接假定main的新flags存在。
