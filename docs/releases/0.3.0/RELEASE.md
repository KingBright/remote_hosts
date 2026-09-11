# Remote Hosts 0.3.0 发布交接

> **2026-09-10 恢复更新：不要直接重复执行下方历史升级命令。** 网关和两台 Mac 已观察到运行 0.3.0。MacBook 的重复升级任务经用户 bootout 后恢复，远程代码读取与终端执行成功；2026-09-10T10:23:44Z 至 10:33:15Z 的复查中 PID=1927、runs=644 均未变化，升级任务不再加载。证据见 `macbook-recovery-confirmation.json`。完整双设备验收仍未完成。
>
> 工作树 `scripts/upgrade-code-agent.py` 已增加相同哈希不重启、升级互斥、保留独立回执及顶层 PID 解析，`scripts/tests/test_upgrade_code_agent.py` 的 10 项隔离回归通过。**冻结的 dist/0.3.0 发布包和已暂存的旧脚本未被改写，仍不具备这些防护。** 本轮没有再次升级、回滚或重启服务。真正一次性更新入口与网关稳定就绪门禁分别继续按 RH-044、RH-042 推进，不能因为此次恢复就关闭整项。
>
> 下文保留先前被拦截时的历史记录；当前状态以 `deployment.json` 为准，旧状态已归档到 `deployment-before-recovery-confirmation.json`。

日期：2026-09-10。状态：**候选已构建并暂存，网关升级被工具平台拦截；0.3.0 尚未上线。** 实际状态以 `deployment.json` 为准，不以文件存在或包版本代替运行证明。

## 已完成

0.3.0 源码已有135项功能测试、格式、严格Clippy与工作区检查通过，发布打包时核对了原回执中的全部源码/依赖指纹。Mac ARM64与Linux AMD64 musl release构建均退出0。脚本语法检查和产品清单校验通过；本轮没有再次将135项旧测试说成新增脚本的运行测试。

MacBook完整候选包：

```text
/Users/jinliang/Workspace/remote_hosts/dist/remote-hosts-code-0.3.0/
```

NAS暂存：`/opt/remote-hosts-code/releases/0.3.0/`，Linux二进制及升级脚本SHA-256已独立核对，现网仍0.2.0、PID26915、active。
Mac Studio暂存：`/Users/jinliang/.local/share/remote-hosts-code/releases/0.3.0/agent-release-0.3.0.tgz`，传送命令退出0，归档两端SHA-256均为 `8e8f39c42cc8d43c4718dc2c6e7385120a6386dba725091639ee7816d8a8d8d7`；不是已安装。

| 候选 | SHA-256 |
|---|---|
| macOS ARM64 | `588a353b2f00cd6ef10434e09e994b870cc6463fa0bef2e7b29c780907b28d85` |
| Linux AMD64 musl | `bdc2fce190911f45cc1f25ac4d31ece5ca3d482e4e6efdc58d900cc934e0fab4` |
| manifest.json | `27978ff5c35a6fb31abda2a16ed8607e4e769980d0a1b15f33b9380fbdff42e9` |

## 唯一待续办主线

先在用户正常授权且独立于被升级Agent的运维会话执行网关升级，再分别升级空闲的Agent。不要在Remote Hosts自己的终端内同步等待“自己的终端变空闲”。任何平台拦截都应停在该动作并保留记录，不通过包装或替换客户端自动重放。

NAS（已确认主机为DS）上，候选与脚本已经齐备，待授权执行：

```sh
python3 /opt/remote-hosts-code/releases/0.3.0/upgrade-code-gateway.py \
  --candidate /opt/remote-hosts-code/releases/0.3.0/remote-hosts-code-linux-amd64 \
  --sha256 bdc2fce190911f45cc1f25ac4d31ece5ca3d482e4e6efdc58d900cc934e0fab4 \
  --version 0.3.0 \
  --result /opt/remote-hosts-code/releases/0.3.0/deployment.json
```

该步骤本次尝试被宿主安全检查拦截，未执行；没有备份新一轮数据库、替换生产二进制或重启网关。脚本会保存旧二进制/数据库/site并验证新进程，但其本次成功和回滚结果都尚不存在。

网关健康接口必须实际返回version=0.3.0、dispatch_protocol=2、progress_protocol=1，然后才能执行Agent升级。两台Mac分别从独立会话调用包内 `upgrade-code-agent.py`，提供本机候选、上述Mac哈希、`--version 0.3.0` 和独立结果文件；它会等待活动任务完成，再检查launchd PID与运行标记。Mac Studio需先解开已暂存的自有归档并核对候选哈希。

最后在MacBook项目根运行新版验收脚本（仅探测两台明确授权设备）：

```sh
python3 scripts/check-code-gateway.py \
  --origin https://mcp.hackerlife.fun \
  --password-file /Users/jinliang/.local/share/remote-hosts-code/setup/gateway-login-password.txt \
  --report docs/releases/0.3.0/acceptance.json \
  --run-id release030-20260910 \
  --expected-version 0.3.0 --dispatch-protocol 2 \
  --device-id ba3bf113-2390-466e-88bc-40d5b4f02884 \
  --device-id 8af88e35-f316-4dd9-9810-fa5bd3a22196
```

密码由脚本在本机读取，不要放进对话或工具参数。该脚本为显式变更验收，会在选定设备授权根目录创建并清理测试文件。报告绑定版本与run_id，不能复用0.2.0回执。公网验收仍不替代真实ChatGPT原生附件的独立往返验证。

补验实际使用场景：传输进行中读代码、启动终端、观察进度并测试另一个终端取消；原生附件分别做小文件和此前失败量级的文件往返。保存操作ID、哈希、运行版本和失败阶段，再按产品清单逐项关闭。不要把“网关版本已变”当作所有场景已通过。

## 已知限制和后续版本

0.3.0支持存活操作内的入站Range恢复，不支持双向跨进程续传，导出仍完整POST重试。独立文件取消、原生source lookup偶发问题、调度公平、工作上下文等仍在 `docs/product/backlog.json`。没有删除SHA-256、原子发布或权限检查来通过验收。

本次新发现的NAS SFTP缺失问题为RH-041：默认scp的SFTP请求失败；相同认证与主机密钥检查下的传统SCP传送成功。自动兼容处理尚未进入产品。平台升级拦截为RH-030，不能由工具承诺消除。

不要重建或覆盖当前候选来继续0.3.1开发；使用新版本目录。发布成功后将实际目标回执汇总到本目录，更新 `deployment.json`、产品清单及生成视图；失败则保留失败和回滚证据，不修改历史源码验证回执。
