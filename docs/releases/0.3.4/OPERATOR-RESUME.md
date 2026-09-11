# 0.3.4 发布接续方案：本机操作与自动发布改造

## 当前任务与责任

本文件是用户审阅后，在 MacBook 本机独立 Terminal 或本机 Codex 正常授权会话中执行的运维交接单，不是已经执行的发布回执。不要让 ChatGPT 经已拒绝的 Remote Hosts 路径调用本文件的发布命令，也不要关闭宿主确认或把写操作标成只读。

当前选定 NAS 网关和 MacBook-M2-Max；不升级、不重启、不取消 Mac Studio 的任务。0.3.4 已包含前序候选改进，先核对兼容和冻结包，按网关优先顺序发布，不需要为发布这个累计候选先重复安装0.3.3。用户无需再次批准每版的常规发布，但本机客户端自己的权限与审核仍须遵守。

源码根目录：`/Users/jinliang/Workspace/remote_hosts`。
冻结包：`target/source-snapshots/034-h1/dist/remote-hosts-code-0.3.4/`。
源码验证：`docs/releases/0.3.4/verification-h1.json`，225项通过。
精确manifest哈希：`0badf65e625f349b8384c3c0a694760b889156e20f304363836bdfdabb3983d9`。

本次已通过Remote Hosts重新读取包清单、当前发布状态、升级器参数和验收器参数；没有运行下面的生产命令。历史拒绝来自0.3.3的暂存调用，不等于0.3.4已经实际调用失败，也没有给出可据此修复的规则编号。

## 给本机 Codex 的完整任务

请接续发布冻结的remote-hosts-code 0.3.4，不重新构建、不修改已验证Rust代码、不更改旧包。先阅读本文件和deployment.json，再使用本机正常授权的终端执行以下步骤。发生权限拒绝时走正常审批；不要禁用安全检查或换包装重放。不要经Remote Hosts发起Agent自我升级。

最终必须提交：NAS升级回执、MacBook升级回执、只选择MacBook的现场验收JSON及真实运行版本/哈希，保存到本文件规定的operator-resume目录。失败时写明失败阶段、实际是否切换、原任务标识和明确恢复动作；不要只写blocked。已获得成功回执的步骤不重复执行。

## 1. 核对冻结包

以下代码块在同一个MacBook本机终端会话中执行。每段任何一条命令报错都停止，不继续下一段。

```bash
ROOT=/Users/jinliang/Workspace/remote_hosts
PKG="$ROOT/target/source-snapshots/034-h1/dist/remote-hosts-code-0.3.4"
REPORT="$ROOT/docs/releases/0.3.4/operator-resume"
cd "$PKG"
printf '%s  manifest.json\n' '0badf65e625f349b8384c3c0a694760b889156e20f304363836bdfdabb3983d9' | shasum -a 256 -c -
shasum -a 256 -c SHA256SUMS
./remote-hosts-code-macos-arm64 --version
mkdir -m 700 "$REPORT"
```

版本必须为0.3.4。目录已存在时先检查其中原回执，不删除它，不重复升级。不要只验证manifest内部声明的哈希而忘记上面固定的manifest哈希。

## 2. 核对NAS身份并暂存

SSH使用既有目标与已信任主机键，不改变SSH配置。先确认主机身份、已安装和运行版本、活动发布/传输；若NAS已经运行同哈希0.3.4，不再次安装，只补读回执和验收。若正在执行另一发布操作，先结束那个明确的操作，不并行切换。

首次暂存的命令如下；目录已存在时停止，检查已有文件/回执后仅恢复缺失步骤，不用覆盖或随机新目录规避原任务状态。

```bash
ssh -p 222 -o BatchMode=yes -o StrictHostKeyChecking=yes root@hackerlife.fun \
  'hostname; /opt/remote-hosts-code/remote-hosts-code --version; systemctl show remote-hosts-code-gateway.service --property=MainPID,ActiveState'

ssh -p 222 -o BatchMode=yes -o StrictHostKeyChecking=yes root@hackerlife.fun \
  'umask 077; mkdir /opt/remote-hosts-code/releases/0.3.4'

scp -O -P 222 -o BatchMode=yes -o StrictHostKeyChecking=yes \
  "$PKG/remote-hosts-code-linux-amd64" \
  "$PKG/upgrade-code-gateway.py" "$PKG/manifest.json" \
  root@hackerlife.fun:/opt/remote-hosts-code/releases/0.3.4/

ssh -p 222 -o BatchMode=yes -o StrictHostKeyChecking=yes root@hackerlife.fun \
  'cd /opt/remote-hosts-code/releases/0.3.4 && sha256sum remote-hosts-code-linux-amd64 upgrade-code-gateway.py manifest.json'
```

远端输出必须分别匹配：

```text
c41bb5639cbc374c70dbab8e5925cd08992220bd36765df6094c6c4f3943385b  remote-hosts-code-linux-amd64
086ab4e906b576ca308b9e46219d937331db552800d73ed1eee5c4941d696b05  upgrade-code-gateway.py
0badf65e625f349b8384c3c0a694760b889156e20f304363836bdfdabb3983d9  manifest.json
```

`-O`沿用此NAS已验证的传统SCP兼容模式，因历史现场检查显示未提供SFTP子系统；它不关闭主机身份校验。

## 3. 先升级NAS，再确认服务

确认远端哈希无误后才执行。发布会短暂影响网关连接，不能在需要保持连续传输的关键任务中强行切换。只操作既有remote-hosts-code网关，保留其凭据、注册与数据库；包内升级器会备份并执行既有回退逻辑。检查脚本实际行为，任何回退失败需按回执定位，不宣称始终自动恢复成功。

```bash
ssh -p 222 -o BatchMode=yes -o StrictHostKeyChecking=yes root@hackerlife.fun \
  'python3 /opt/remote-hosts-code/releases/0.3.4/upgrade-code-gateway.py --candidate /opt/remote-hosts-code/releases/0.3.4/remote-hosts-code-linux-amd64 --sha256 c41bb5639cbc374c70dbab8e5925cd08992220bd36765df6094c6c4f3943385b --version 0.3.4 --result /opt/remote-hosts-code/releases/0.3.4/deployment.json'

ssh -p 222 -o BatchMode=yes -o StrictHostKeyChecking=yes root@hackerlife.fun \
  'cat /opt/remote-hosts-code/releases/0.3.4/deployment.json' > "$REPORT/nas-updater.json"

curl --fail --silent --show-error --max-time 15 https://mcp.hackerlife.fun/healthz
```

检查NAS回执state=upgraded、运行二进制哈希一致，公网health版本0.3.4、dispatch_protocol=2、resource_dispatch_protocol=1、readiness_protocol=1。任一失败，不升级MacBook。

若SSH中断而不能确认执行结果，首先读取原deployment.json并核对当前systemd PID和`/proc/<PID>/exe`，不能新开一次升级赌结果。回执不存在也不能单凭这一点认定没执行。

## 4. 从独立本机终端升级MacBook

这里直接同步运行包内升级器，调用进程不是被替换的code-agent，所以不需要由Remote Hosts自我重启。不要使用launchctl submit，不改正式Agent的KeepAlive，不中断其他工作区活动。

```bash
/opt/homebrew/bin/python3 "$PKG/upgrade-code-agent.py" \
  --candidate "$PKG/remote-hosts-code-macos-arm64" \
  --sha256 073989b51289d78cc81c7052670e05c537a82dad6536def3a359544ce17820e0 \
  --version 0.3.4 \
  --result "$REPORT/macbook-updater.json"
```

检查state=upgraded、gateway_verified=true、实际运行PID/版本与回执一致，稳定窗口内五通道均继续前进。若state=no_change，只表示同哈希未重复安装，还需独立健康检查。若报active work，保留线上进程，查看任务归属和自然结束状态后再处理；不清空数据库里的running记录。

若确有已变更后失败，读取rollback和rollback_readiness，核对是否恢复；恢复失败时使用回执指定的已校验备份做人工恢复，不把“最新备份”一律当旧版。

## 5. 只验收MacBook，并完成记录

验收会创建专用临时文件、执行测试命令和传输测试文件。凭据由脚本在本机读取，不输出密码或token。独立本机会话的正常授权范围需覆盖这些动作。

```bash
/opt/homebrew/bin/python3 "$PKG/check-code-gateway.py" \
  --origin https://mcp.hackerlife.fun \
  --password-file /Users/jinliang/.local/share/remote-hosts-code/setup/gateway-login-password.txt \
  --report "$REPORT/acceptance-macbook.json" \
  --run-id operator-034-resume \
  --expected-version 0.3.4 \
  --dispatch-protocol 2 \
  --device-id ba3bf113-2390-466e-88bc-40d5b4f02884
```

成功必须由JSON证明：选中设备只有MacBook，agent_version=0.3.4，规定读写/终端/文件场景完成，临时OAuth撤销。再读取设备能力和回执投递统计，确认durable_receipts_v1可见、正常请求的待发记录可收敛。标准验收不替代断网/掉电的全故障矩阵。

保存原始运行回执，在保留历史deployment.json后更新发布状态及backlog。源测试、发布包、运行状态、现场验收保持独立标识。主仓库存在其他未提交改动，不自动提交全部工作树。

## 成功、失败与返回结果

返回一个摘要JSON：version、NAS/MacBook的实际version/pid/hash、updater state、acceptance报告路径、temporary OAuth revoked、Studio untouched、未满足门禁。不要回传密码、cookie、带授权参数的URL或整份agent.json。

任何步骤失败都返回：失败阶段、原命令/任务标识、已完成的步骤、是否有副作用证据、保留产物位置、下一条明确恢复动作。网络失败只恢复传送；发布结果未知先查询；哈希冲突停止覆盖；服务不健康先恢复；宿主拒绝走正常审核/支持，不经另一伪装入口自动重放。

## 后续产品改造，不冒充已实现

下一优先项是独立发布执行器：release_prepare生成哈希绑定的具体计划，release_apply只接受受限release_id/plan_hash/批准策略，不接受任意命令或任意URL，operation_get观察整个发布任务。目标、服务名、路径和凭据来自用户管理的固定配置，实际权限在服务端验证。发布结果不依赖承载对话的Agent存活。

准备计划和应用计划的副作用必须如实声明，不能把它们标记为只读来求放行。新工具也不保证不被宿主拒绝。宿主拒绝时只生成清晰交接方案供操作员主动审阅执行，不能自动转移到其他客户端继续同一被拒绝动作。

同时给应用维护者准备宿主诊断资料：现有拒绝事件时间、工具名、输入的脱敏摘要/哈希、用户授权范围、服务端是否接收的证据、工具目录hash和界面截图。没有平台request ID时写not_available，不把idempotency_key伪装成平台ID。工具定义刷新和宿主风险审核是两件事，刷新不能承诺消除安全拦截。
