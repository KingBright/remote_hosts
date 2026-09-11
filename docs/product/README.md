# Remote Hosts 持续使用与版本规划

我们同时是使用者和维护者。实际使用中暴露的问题进入本目录，不再只留在对话或某一次长报告中。

## 一个事实源

`backlog.json` 是问题清单的唯一可编辑事实源；`BACKLOG.md` 是生成的阅读视图。每项有稳定 RH 编号、现象、来源、当前处理、验收条件、优先级、依赖和目标版本。使用中发现新问题时先检查是否已有对应项，避免重复记录；不能确定根因时明确写未定位。

```sh
python3 scripts/product-backlog.py --render --check
```

校验会拒绝重复编号、未知状态、循环依赖及没有验收证据的关闭项。暂时移出当前版本只能修改目标版本并说明原因，不删除问题。

## 每轮迭代

开始时读取清单和最新发布回执，选定一组可交付问题；实际观察到的新失败立即记录操作ID、设备、版本、发生阶段和可公开的诊断，不保存密码、签名下载URL或完整私人命令。实现后添加针对问题的回归与场景验收，更新当前处理和证据路径。

候选测试通过标记 candidate_verified；部分修复仍为 partial。只有满足该项验收条件才可 closed，并填写 closure_evidence。跨进程续传不能用进程内Range测试关闭，原生ChatGPT附件不能用模拟HTTP客户端验收替代，安装二进制不能代替运行构建确认。

## 当前版本边界

0.7.1 是当前主版本：在 0.5 的独立发布、Workspace 事件、完整 diff 和 Manifest 同步基础上，0.6 加入 operation lifecycle、持久 change-set / `change_resume` 与显式 `workspace_gc`；0.7 再加入默认 64 MiB、显式协商最高 256 MiB 的传输能力和文件源授权状态。

0.7.1 固定源码快照通过 348 项测试、0 失败，NAS、MacBook 与 Mac Studio 均实际运行 0.7.1。发布现场抓到并修复了旧 Agent → 新 Gateway 的暂态持久化错误被误报永久 409 的问题；修复后真实 9,089,298 字节导出发生一次 checkpoint 恢复并最终 SHA 一致。两台 Mac 的原生代码/终端验收通过；当前宿主 schema 尚未暴露 >64 MiB 文件参数，因此该项继续作为明确未验收边界。

目标版本永远是规划，不是完成承诺。已知数据损坏、身份隔离、错误副作用重放或不可恢复发布风险属于上线阻塞项；明确的非阻塞功能缺口可以留在 backlog 中继续迭代。

## 验证后直接发布

用户已明确授权：候选通过完整验证、输入身份与产物一致、满足既有发布门禁时，直接继续构建、发布、核对运行版本并做现场验收，不再逐版等待确认。当前授权目标为 NAS 网关、MacBook-M2-Max 和 Mac Studio；如果某台设备当时有真实业务任务，发布器按目标独立延期，不阻塞其他健康目标，也不强制取消未知业务。

一个迭代按固定输入验证 → 两平台构建与打包 → 先网关后 Agent → 各目标独立 drain / 安装 / 就绪 → 各目标功能验收 → 回写问题清单完成闭环。完成发布后，在当前工作会话内继续下一项高优问题，不要求用户反复发送“继续”。这不是在回复结束后自动运行的后台计划，也不会绕过宿主安全审核。

平台拒绝、任务未排空、验证失败、输入变化或健康检查失败时，记录具体阶段并保留候选和原操作标识；正常可执行的独立工作继续进行。不把再次询问确认当作技术问题的替代，也不以已获常规发布授权为由绕过宿主拦截。

## 稳定构建与发布入口

默认从 `scripts/iteration-code.py` 进入。它根据变更路径自动选择 `docs / release_python / runtime` 三档门禁，记录统一 iteration report；只有 runtime 变化才创建固定源码快照并调用 `release-code.py` 的完整 Rust/Python/workspace + 双平台 release 流水线。验证通过后默认直接 commit + push `main`，再按显式配置调用 `publish-code.py`。

```sh
python3 scripts/iteration-code.py --report target/iteration-runs/<id>/iteration.json --commit-message 'feat(code): ...'
```

获取进度使用同一个 report 的 `--status`，需要时附 `--publish-status-dir`；不要重新启动原 build/publish。底层 `source_snapshot.py`、`release-code.py` 和 `publish-code.py` 仍保留为可独立诊断的分层组件。同一报告不覆盖，同槽忙时返回原持有者，缓存复用不等于跳过测试。

完整复盘和新纪律见 [`ITERATION-EFFICIENCY.md`](ITERATION-EFFICIENCY.md)。

源码必须有可追溯 Git 基线；固定源码归档不是长期替代 Git 的理由。发布配置、密码、运行数据库、缓存和大型临时二进制不应进入提交。功能范围先锁定、最终候选再改版本号，避免每个小补丁都触发无意义的全量重链接。下一轮功能优先级见 `NEXT.md`；实际状态以 `backlog.json` 为准。

## 发布证据

当前版本说明：`docs/releases/0.7.1/RELEASE.md`。
固定输入/构建流水线：`target/iteration-071-a2/pipeline.json`，长期事实以源码提交、manifest 哈希和 `docs/releases/0.7.1/` 证据为准，不依赖 target 目录永久存在。
最终运行状态：`docs/releases/0.7.1/deployment-final.json`；原 publisher 的 partial 记录仍保留为历史证据，不覆盖失败过程。
源码提交：`1bbaf74`（0.7.0 主线）、`7071d9d`（0.7.1 receiver hotfix）、`2b1619c`（验收器动态 lifecycle 修复）。
现场验收只有实际执行成功才可标 passed；宿主在执行前拦截、run-id 参数验证失败等情况必须保持未执行/失败状态。

工具平台拒绝某个操作时，记录为“未执行/外部阻塞”，不把它记为测试失败，也不通过改名或包装等价操作绕过。其余独立、正常授权的工作仍可继续。

## 沟通

过程反馈说明已经完成的变化、当前运行的检查和真实阻塞；不让用户靠连续追问才能判断状态。远端任务存在不等于模型会在回复结束后继续工作。每次收尾明确本地源码、构建包、线上运行及未完成项各自的状态。
