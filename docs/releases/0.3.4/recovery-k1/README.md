# MacBook 0.3.4 升级预检恢复与诊断修复

## 当前结论

原升级任务已明确失败于网关预检，尚未进入备份、安装或服务重启。修复版独立更新器已成功将 MacBook 从0.3.2升级到原冻结的0.3.4二进制；NAS没有重复安装，Studio未操作。

最终原始回执见 `macbook-updater.json`：state=upgraded，PID34639，安装SHA-256为 `073989b51289d78cc81c7052670e05c537a82dad6536def3a359544ce17820e0`，五通道持续前进，10次样本、稳定约21.9秒。`runtime-observation.json`另行核对正式Agent与数据库PID一致、更新器只运行一次且退出0、待发回执pending/blocked均为0。

## 根因证据与未知范围

旧辅助模块把HTTP拒绝、连接超时、TLS、DNS和响应解析问题全部转换成同一条 `gateway_readiness_unavailable`；最初失败已经无法从旧日志还原出底层原因。不能据此声称原问题就是Cloudflare、代理、TLS或launchd环境。

这次分别在普通终端和原更新器相同的解释器/launchd环境，用相同配置、设备鉴权和产品User-Agent发出只读请求，均返回200，耗时约1.76秒和1.45秒。没有修改网络设置、代理、TLS验证或设备凭据。两份独立探针结果见 `terminal-probe.json` 和 `launchd-probe.json`。临时诊断任务已卸载。

## 实际修复

`scripts/agent_upgrade_support.py`保留原鉴权、HTTPS与禁止重定向，添加安全错误分类。错误记录只含预定义类型、次数、数值状态和经过字符验证的CF-Ray；不打印原异常消息、带签名URL、响应体或Authorization。

超时、暂时DNS错误、连接重置及部分5xx仅对同一只读请求做最多三次退避重试；401/403、TLS证书、身份/协议错误不重试，429保留限速说明而不进入快速重试。成功记录次数及耗时。稳定就绪循环已有总观察窗口，因此其中每次只发一次探针，避免嵌套三重重试拖长观察。

`scripts/upgrade-code-agent.py`记录明确phase、service_changed和结构化diagnostic。预检失败的回执能够直接证明尚未安装；有副作用后仍使用原有回退与哈希校验。

新增14项回归，完整Python集合64项通过，1项需主动启用的launchd探针跳过；helper-verification.json绑定实际测试文件与部署辅助脚本哈希。原225项验证对应未改写的Rust二进制发布包，不能与本次64项相加宣传。

此次成功预检只请求1次、耗时832ms。因此，生产成功本身不证明重试路径被触发；重试和错误分类由隔离回归覆盖。原失败历史与原准备任务均保留。修复模块和更新器以本目录helpers快照部署，不覆盖冻结0.3.4发布包的旧辅助脚本。后续发版需收录本次修复。

## 剩余门禁与解决办法

完整代码/命令/2MiB传输验收调用 `rh034-recovered-macbook-full-acceptance-20260911-k1` 在宿主派发前被拒绝，命令没有执行，没有创建该轮临时OAuth或测试文件。未拆分、改名或转发重放。当前结论是“所选生产目标已升级且控制面/哈希已确认”，不是“整套场景已验收”。

实际工具已通过新Agent取得运行记录并正常读写本轮证据，但这些工作不代替被拒绝的综合回归。后续正常授权的独立本机运维会话可审阅后执行以下原验收，不需要重新安装任何目标或重跑构建；若其客户端要求授权，应正常审核。

```sh
cd /Users/jinliang/Workspace/remote_hosts
/opt/homebrew/bin/python3 target/source-snapshots/034-h1/dist/remote-hosts-code-0.3.4/check-code-gateway.py \
  --origin https://mcp.hackerlife.fun \
  --password-file /Users/jinliang/.local/share/remote-hosts-code/setup/gateway-login-password.txt \
  --report docs/releases/0.3.4/recovery-k1/acceptance-macbook.json \
  --run-id recovery034k1 --expected-version 0.3.4 --dispatch-protocol 2 \
  --device-id ba3bf113-2390-466e-88bc-40d5b4f02884
```

报告只验MacBook，并明确临时授权撤销。已有报告时先核对原范围和状态，不删除或用新编号假装第一次验收。

## 后续维护

RH-046跟踪预检错误分类/临时失败恢复及默认发版包收录，关联RH-029、RH-042。RH-027继续保留完整场景验收门禁。源码和历史包分别保留；本轮没有提交或推送Git，没有修改Studio，也没有再重启NAS。

外部依据仅用于异常分类实现：Python urllib.error官方文档说明URLError.reason及HTTPError.code/headers；`https://docs.python.org/3.14/library/urllib.error.html`。本机故障结论以本目录实际回执为准，不由外部文档推断。
