# Remote Hosts 受限管理员维护组件

## 当前交付边界

`remote-hosts-admin 0.1.0` 是单独准备、尚未安装的候选组件。普通 Remote Hosts / remote-hosts-code 代理仍以普通用户运行；不修改它们的安全配置，不增设通用 sudoers 规则，不接收管理员密码，不升级 Gateway，不安装其他远程桌面或组网软件。

本轮范围是代码、测试、安装准备。**没有运行安装器的 `--apply`，也没有启动 root helper 或执行真实清理。** 代码编译和不带 `--apply` 的安装预览不是系统安装验收。

两种 Remote Hosts 入口通过已有终端工具调用同一个 CLI，使用同一套 Rust 协议、状态机与执行器。现阶段没有增加新的 MCP 工具名，避免误称当前 Gateway 已经提供新的管理员接口。Windows 不在 0.1 的管理员实现范围；本组件不改变 cube 的 UAC 或代理权限。

## 授权与通信

```text
已授权的 Remote Hosts 普通用户终端
        ↓ 同一个 remote-hosts-admin CLI
本机 Unix socket，内核提供 peer UID，客户端也验证服务端为 root
        ↓ root 所有且不可由普通用户改写的策略
独立、首次需要管理员授权安装的系统服务
        ↓ 编译时固定的维护动作
系统服务管理 / 精确进程停止 / 指定文件备份并移除
```

不开放 TCP 端口，没有 root shell、任意 argv、任意文件路径、环境变量透传或脚本执行请求。UID 授权的边界是整个本机用户账户，**不是**对该 UID 下各应用的分别认证。该账户的其他进程也可请求已授权的固定维护动作；因此动作范围保持很小。

Linux 的普通代理保留 `NoNewPrivileges`。已经由管理员单独安装、从系统服务管理器启动的 helper 本身为 root，不依赖让代理的子进程重新获得权限。网络恢复不等于已获得首次安装授权。

安装时校验独立记录的 manifest SHA-256、本机 OS/CPU、用户 UID/GID/home，以及显式提供的本机 agent 配置中的 `device_id`。不输出配置中的 token。管理员必须确认传入的是该设备实际使用的 agent 配置。

## 0.1 唯一写动作

动作名：`remove_legacy_remoteplay`。请求不能替换动作中的路径、服务或进程名称。

### Linux

只处理以下两个旧系统服务，先旧 RemotePlay，再 EasyTier：

```text
remote-play.service
easytier-remoteplay.service
```

停止并禁用后，将 `/etc/systemd/system/` 下这两个精确服务文件保存在 root 私有回执目录，再移除原文件并执行 daemon-reload。服务文件必须受 root 控制；不同 FragmentPath、未审阅的 drop-in、符号链接、硬链接等情况拒绝处理，不做猜测。

`remote-play-current.service` 是受保护的新版本**用户级**服务。开始前和结束后检查它仍为 active。不会禁用它，也不会修改 Remote Hosts 的服务。

### macOS

以安装时绑定的 home 为前缀，只允许以下三个旧可执行文件：

```text
remote_play_test/RemotePlay Unified.app/Contents/MacOS/remote_play
remote_play_test/RemotePlay Unified.app/Contents/Resources/bin/easytier-core
workspace/vpn/bin/macos/easytier-core
```

先停止旧父进程，再停止两份旧 EasyTier。匹配完整可执行路径、UID、PID 和启动时间，在发 SIGTERM 前重新核对。不会执行批量 pkill 或强制 SIGKILL。macOS 的这次身份复核与发信号不是一个原子内核操作，不能宣称彻底消除 PID 复用竞态；首次管理员实机验收仍应检验它。

三个文件的精确字节备份到 root 私有目录，核对哈希后才移除源文件。文件操作逐层以目录描述符固定父目录，拒绝跟随符号链接，不递归删除目录。

保留 `Applications/RemotePlay.app/Contents/MacOS/remote_play`、`com.remoteplay.host` 新版启动项和 `/Applications/UURemote.app/`。新版 RemotePlay 必须运行；如果原先存在 UU 进程，结束后还必须能观察到 UU。这里的进程状态检查**不等于**已完成串流和输入功能验收。

这项动作只验收上面明确列出的旧对象，不代表全盘残留或未知启动入口已全部清除。

## CLI 与断线处理

macOS 安装位置：

```text
/Library/PrivilegedHelperTools/com.remotehosts.admin
/Library/Application Support/RemoteHostsAdmin/helper.sock
/Library/Application Support/RemoteHostsAdmin/policy.json
/Library/Application Support/RemoteHostsAdmin/receipts/
```

Linux 安装位置：

```text
/var/lib/remote-hosts-admin/bin/remote-hosts-admin
/run/remote-hosts-admin/helper.sock
/var/lib/remote-hosts-admin/policy.json
/var/lib/remote-hosts-admin/receipts/
```

macOS 使用持久化 socket 父目录，防止重启清空临时运行目录后无法启动；Linux 由 systemd 的 RuntimeDirectory 重建运行目录。

由原授权普通用户调用 CLI：

```text
<helper> status
<helper> plan --request-id <固定的小写 UUID>
<helper> apply --request-id <同一 UUID> --plan-sha256 <plan 返回的摘要>
<helper> receipt --request-id <同一 UUID>
<helper> revoke
```

`plan` 会保存只读检查产生的计划；不执行清理。`apply` 绑定设备 ID、调用 UID、策略版本、原始对象快照和 5 分钟有效期，执行前重新检查。每一步执行前后持久化回执；在真实副作用发生前，先 fsync `running` 状态。

连接中断后先查询同一 UUID 的 `receipt`。重复 apply 返回原结果，不重复动作。进程崩溃后遗留的 `running` 回执返回 `recovery_required`，不能自动换 UUID重试；应重新检查真实状态并完成明确审阅。失败不自动恢复 EasyTier，不为了“回滚”重新启用刚停掉的旧服务。

`revoke` 只收回后续写请求授权，不会撤销已经接受且正在执行的动作，也不卸载现有服务。重新授权需要管理员处理 root 策略、更新 grant_id 并审阅撤销记录；普通代理没有重新给自己授权的接口。

每个计划的 JSON 包含原文件哈希、权限与所有者、进程身份、每步结果及错误；备份为 `<UUID>-<index>.bak`。这不是不可篡改的审计系统，root 管理员仍能修改文件。日志和策略不存放管理员密码。

## 安装准备与实际安装分离

先从仓库捕获仅包含 helper 的独立快照，排除其他并行任务未提交的存储/升级代码：

```bash
python3 scripts/admin-helper-snapshot.py --destination target/admin-helper-preparation/snapshot
cargo test --manifest-path target/admin-helper-preparation/snapshot/Cargo.toml
cargo build --release --manifest-path target/admin-helper-preparation/snapshot/Cargo.toml
```

构建后用 `scripts/prepare-admin-helper.py` 提供目标设备 ID、真实本机用户信息、平台和二进制，生成离线候选包。不得假设所有设备 UID 相同。

包内包含 binary、policy、systemd/launchd 模板、安装器、卸载器和校验清单。默认运行 `python3 -I install.py` **只预览**。正式安装同时要求：

```text
--apply
--grant-legacy-cleanup
--agent-config <该设备实际使用的 agent.json>
--accept-manifest-sha256 <通过另一可信记录核对的摘要>
```

安装器要求 Python 的 `-I` 隔离模式，拒绝从用户可写包目录导入同名标准库模块。安装器不调用 sudo、不收集密码、不联网下载、不自动清理旧服务。它必须从另外获得系统认可的管理员会话运行。首次安装以外的覆盖更新默认拒绝；部分安装失败后需要检查固定目标路径，不得盲目反复安装。

卸载器默认也只预览。经管理员明确执行后只移除这个 helper 的服务和二进制，保留策略、回执及备份，不修改 UU、新版 RemotePlay 或 Remote Hosts，不恢复 EasyTier。

## 下一次继续时的验收顺序

1. 核对源快照与包摘要、目标设备身份和实际用户信息；确认这是此设备应安装的候选包。
2. 验证正式管理员通道可用，再安装 helper。仅网络恢复时不直接认定有管理员权限。
3. 以普通 Remote Hosts 通道执行 status，确认 UID、设备及 enabled 状态；检查未授权 UID 不能调用。
4. plan 后核对精确目标与新版服务保护条件，才 apply；模拟断线后读取同一回执。
5. 验收 EasyTier 旧对象、新版 RemotePlay、UU，以及重启后 helper 的可用性。

不能把单元测试、交叉编译或安装预览当作已经通过以上实机管理员验收。

## 依据

- Linux kernel: https://docs.kernel.org/userspace-api/no_new_privs.html
- Tokio UnixStream peer credentials: https://docs.rs/tokio/latest/tokio/net/struct.UnixStream.html#method.peer_cred
- Apple launchd system services: https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html
