# Terminal evidence continuation experiment — 2026-10-09

本实验对应 RH-016。机器可读结果只保存在 [verification.json](verification.json)；本文说明方法、原始片段和复现边界，源码调用约定见 [native-release-client.md](../../native-release-client.md)。

## 假设、版本与环境

假设：Gateway 的预算省略响应可能完全移除 terminal 元数据。旧发布客户端因此在首次观察时丢失身份，或在运行中继续轮询；保存原 operation、只读恢复身份并读取原日志，应能接续且不重发命令。最终输出还必须有完整持久回执，不能把“查询成功”当成“原执行成功”。

基线提交为 `87a5d3cf898d72495a5024fb4c89f5d8812d860b`。RED 与最终控制器/测试 SHA-256 见 JSON 的 `red`、`legacy_compatibility_red`、`source_sha256`。当前控制器提交可用下面的 Git 命令核对，不与已安装程序版本混用。

实际环境为 Darwin 25.5.0、arm64、Python 3.14.3。既有 Gateway 产物为 0.10.27，冻结源码提交 `b55e061a1d88a9975125cbacc6761c1b321600c3`；其 Linux-only 包和快照校验值由 [产品事实源](../../product/backlog.json) 的 `last_component_packaged_release` 保存。本轮只修改 Python 控制器和文档，没有重建或替换 Rust 产物。

## 方法与关键步骤

1. 从既有恢复计划和原回执确认观察必须沿同一 operation，不使用新命令代替未知结果。
2. 用实际 0.10.27 响应形状固定 12 个用例，先在原客户端运行 RED。夹具身份和授权值均为合成数据，断连用例不访问网络。
3. 增加 `observe_terminal(workspace_id, original_operation_id)`。初始身份缺失时只查询原 operation 一次，预算上限 131072；已有身份后从原 terminal 的字节零恢复日志，校验 UTF-8 游标、退出码和协议 2 的最终持久回执。
4. 全量检查发现旧协议兼容回归，补三项用例并保存第二个 RED。仅保留协议 1 的历史 terminal-ID 别名与无 receipt 日志契约；显式恢复和协议 2 不允许猜测 operation。
5. 在既有 Gateway 上进行只读观察：4096 预算复现无 terminal 元数据的省略响应；131072 查询恢复原身份；完整历史取回 5785 字节，退出码 0，SHA-256 为 `b10ee88c9ea5d1991a2d72c4cffac3f5aa3460d03666361a0a070e28b4a4bd98`。完整部署字段与既有最终验收一致。原 operation、请求身份和实例信息保存在私有 runtime 交接，未重执行构建或安装。
6. 执行仓库 `release_python` 门禁：diff、JSON、backlog 生成一致性、Python 编译和完整 unittest。测试前后所有 Python 输入指纹一致。

## 原始小型指标与片段

第一次 RED：12 项，1 个断言失败、11 个错误，unittest 退出 1。旧协议 RED：15 项，1 个失败、1 个错误，退出 1。最终相关用例为 **53 项全通过**：15 个接续用例及 38 个原生通道/路由用例。

```text
RuntimeError: terminal_identity_unconfirmed:original
AttributeError: 'Client' object has no attribute 'observe_terminal'
AssertionError: Lists differ: ['terminal_exec', 'operation_get', 'operation_get'] != ['terminal_exec', 'operation_get', 'terminal_read']
Ran 12 tests in 0.667s
FAILED (failures=1, errors=11)

Ran 15 tests in 0.412s
FAILED (failures=1, errors=1)

Ran 444 tests in 54.242s
FAILED (failures=1, skipped=1)
```

最终完整门禁执行 56.363 秒，退出 **1**：442 通过、1 个既有公共仓库实例边界检查失败、1 个未启用的可选 launchd 探针跳过。失败涉及的四个文件与基线 HEAD 字节一致；文件 SHA 与日志 SHA 见 JSON。没有跳过失败检查，也没有把完整门禁标为通过。原始片段摘自小型本地执行日志；仓库只保留片段与校验值。

## 复现

在仓库根目录，使用记录的 Python 环境：

```sh
git log -1 --format=%H -- scripts/release_client.py scripts/tests/test_terminal_observation_recovery.py
python3 -W error::ResourceWarning -m unittest discover -s scripts/tests -p 'test_terminal_observation_recovery.py' -v
python3 -W error::ResourceWarning -m unittest discover -s scripts/tests -p 'test_*native*.py' -v
python3 scripts/product-backlog.py --check
git diff --check
python3 -W error::ResourceWarning -m unittest discover -s scripts/tests -v
```

对照旧客户端可在临时目录运行，原工作树保持不变：

```sh
terminal_recovery_fixture=$(mktemp -d)
git show 87a5d3cf898d72495a5024fb4c89f5d8812d860b:scripts/release_client.py > "$terminal_recovery_fixture/release_client.py"
git show 87a5d3cf898d72495a5024fb4c89f5d8812d860b:scripts/native_release_client.py > "$terminal_recovery_fixture/native_release_client.py"
mkdir "$terminal_recovery_fixture/tests"
cp scripts/tests/test_terminal_observation_recovery.py "$terminal_recovery_fixture/tests/"
python3 -W error::ResourceWarning -m unittest discover -s "$terminal_recovery_fixture/tests" -v
```

这里运行最终的 15 项用例，对旧代码预期失败；其分组不同于开发时保存的 12 项 RED，不应照抄历史失败计数。在线只读复现需要自己已授权且保留日志的原 terminal operation：先查询 4096，再查询 131072，取得明确 terminal ID 后从 cursor 0 读取 full history。不得为获得省略响应重新执行原副作用，也不要把文中的私有日志 SHA 当作自己环境的预期值。

## 结果、局限与缺失证据

元数据省略、运行中省略、UTF-8 分页、身份变化、非零退出、最终持久性、断连不降级及旧协议兼容均有固定回归。只读接口已取回既有部署的最终历史。

本轮没有强制断开生产传输，也没有通过新的真实 native Client 会话重新做整条生产观察；native 断连由隔离 mock 和已有 stdio 故障夹具覆盖。完整门禁仍被既有公共仓库边界失败阻塞。原日志若已 GC，或最大观察预算仍无法确认身份，客户端明确失败并保留原 handle；这不是可无限恢复的保证。

生产 TaskGrant 到期验收仍缺支持版本绑定的宿主接口；用户批准与签发/到期结果属于私有交接。本轮没有签发授权、启动 OAuth、重部署，亦不证明 RemotePlay、Pixel 或 Android 已恢复。RH-016 的全部事件回放与现场故障矩阵仍未关闭。

入口只保留本文和一个 JSON，不归档 target、二进制或全部执行日志。实际文件数与总字节在任务交接中报告。
