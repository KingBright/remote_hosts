# 后续迭代

当前建议见 [2026-09-22 桌面接入与工具评估](DESKTOP-REVIEW-2026-09-22.md)：优先修复时钟偏差阻断轮询（RH-053），再完成发布器的原操作续传（RH-009）、错误诊断（RH-017）和跨平台宿主验收。下面保留历史计划，不能据此判断当前缺口。

以下是 0.7.1 时的历史计划，不能作为当前运行版本或未完成项的依据。
桌面接入方式见 [desktop-code-mcp.md](../desktop-code-mcp.md)。当前评估应结合
`fleet_status`、各版本现场验收回执和本轮交付记录，分别判断源码、部署与业务验收。

## 0.7.1 历史计划

0.7.1 已把 0.6.x 的恢复/GC 主线和 0.7.0 的文件通道升级一起落地：服务端 21 个工具、持久 change-set、显式 Workspace GC、文件源授权状态，以及“默认 64 MiB、显式协商最高 256 MiB”的传输能力。NAS、MacBook、Mac Studio 当前均运行 0.7.1；固定源码快照 348 项测试通过。

下一轮仍以“少往返、可恢复、真实宿主可用”为准，不以继续增加工具数量为目标。迭代流程本身已开始收敛到 `scripts/iteration-code.py` 单入口，详见 [`ITERATION-EFFICIENCY.md`](ITERATION-EFFICIENCY.md)；后续应优先修产品问题，而不是反复人工拼接验证/发布命令。

## P0：宿主 schema 与 >64 MiB 原生闭环

Gateway 和两台 Agent 已报告 `transfer_limits_protocol=1`、默认 64 MiB、上限 256 MiB，但当前已有 ChatGPT 会话的宿主工具 schema 仍把 `file_upload/file_download.max_bytes` 固定在 64 MiB。

下一轮要把 server catalog / Agent feature / host-visible schema 三者做成可直接诊断的状态，并在宿主真正刷新后执行至少 65 MiB 的原生双向往返、Range、断线恢复和 SHA 校验。不能用服务器自报 256 MiB 代替宿主现场验收。

## P0/P1：升级就绪对网络抖动更稳健

Studio 0.7.1 一次 fresh retry 在所有 lane 暂时无法 poll Gateway 时触发 `readiness_timeout`，随后自动回滚成功；网络恢复后的下一次 fresh job 正常升级并通过九次稳定采样。

继续改进 updater：区分“候选进程崩溃”和“网关/网络暂时不可达”，记录每 lane 最近一次成功与错误类别；在保持有界总时长的前提下允许短暂网络抖动，而不是过早判候选失败。回滚仍必须保留。

## P1：接收端事务与故障注入

0.7.1 已修复“chunk bytes 已落盘但 offset/进度持久化失败却返回永久 409”的问题，并在真实 0.6 Agent → 0.7.1 Gateway 发布链路观察到一次 4 MiB checkpoint 重试成功。

下一步把 DB busy、fsync、rename、complete metadata commit、低磁盘、Gateway restart 等故障加入明确注入矩阵；409 仅用于可证明的身份/offset/checksum 冲突，内部暂态错误必须可安全观察和重试。

## P1：发布/验收器与动态观察字段分层

标准验收器已修复：幂等业务回执比较不再把实时 `operation_lifecycle` 当成持久结果的一部分。继续把 durable receipt / live observation / device-wide queue snapshot 三层在 schema 和文档里标清，避免调用者对动态字段做不合理全对象比较。

发布器应能在某个目标验收脚本失败时继续保留其他目标的成功结果，并提供独立的“仅重跑验收、不重装服务”路径。

## P1：终端陈旧状态自动对账

本轮 Studio 升级被一条已经完成网关任务、但本地仍标记 running 的旧浏览器测试 terminal 阻塞。下一轮补齐 terminal 进程/状态的启动与周期对账，自动收口已死亡或孤儿化状态，同时绝不误杀仍真实运行的任务。

## 发布纪律

固定规则继续保持：开发 → 按改动范围选最小安全门禁；runtime 候选再固定源码快照并跑完整门禁 → commit + push `main` → 三端发布 → 每目标现场验收。验证通过后不逐版等待人工确认。release-Python 或 docs-only 修复不再无条件重编 Rust，但任何 runtime 输入变化仍必须重新完成全量发布门禁。

任何 `started`、版本字符串、候选测试通过或单个健康检查都不等同于发布完成；最终结论来自运行身份、升级回执、传输/功能现场证据以及明确记录的未验收边界。
