# 迭代效率复盘与新工作流

本文记录 0.3.x → 0.7.1 连续迭代中实际暴露的低效模式，以及从现在开始采用的改进。目标不是少做验证，而是**减少重复验证、重复观察、重复发布和人工状态拼接**。

## 过去最不高效的地方

### 1. 一个小修复也容易重新跑完整 Rust 发布流水线

典型情况是：运行时二进制已经验证通过，后面只修了 Python 验收器、发布脚本或文档，却因为没有明确的改动分层，又重新进入 Cargo/双平台 release 路径。版本号变化还会触发大量重新链接。

**改善：三档门禁。**

- `docs`：只做 JSON / backlog / whitespace 等文档门禁。
- `release_python`：发布/验收 Python 变更跑完整 Python 回归和语法检查，不重新编译 Rust。
- `runtime`：Rust、Cargo、migration 等运行时输入变化才进入固定源码快照、完整 Rust/Python/workspace、macOS/Linux release 和打包。

最终生产 runtime 候选仍必须跑完整门禁；分层只消除与改动无关的重复成本。

### 2. 状态分散，导致大量手工轮询

过去需要分别读取 snapshot、pipeline、deployment、每设备 updater、acceptance log 和 devices inventory。一个长编译阶段往往产生很多只为回答“还在跑吗”的观察调用。

**改善：单一 iteration report。** `scripts/iteration-code.py` 记录本轮文件集合、输入指纹、验证 profile、Git push、pipeline 与 publication 状态。`--status` 是只读入口，并可同时带 `--publish-status-dir` 汇总发布状态。

原则：状态查询只观察原 report，不重新启动 build/publish；CPU 活跃时降低轮询频率，只在阶段变化、失败或用户询问时增加观察。

### 3. 验证通过后没有立刻 push，导致“已验证源码”和 Git 主线短暂分叉

这会让后续 hotfix、文档和部署证据很难快速判断到底对应哪份源码。

**改善：验证通过默认直接 commit + push `main`。** 新 runner 的默认行为就是 push；只有显式 `--no-push` 才停在本地验证。push 前会 fetch `origin/main`、拒绝远端领先、只 stage 本轮已验证文件，并再次 `git diff --cached --check`。

### 4. 发布中断后，工作树变干净反而丢失本轮上下文

过去一旦源码已经 commit/push，而发布或验收在后面失败，重新进入流程时 `git diff` 已经为空，需要人工从历史命令、路径和回执拼回发布身份。

**改善：report 持久保存原始文件集合和内容指纹。** 已记录 `git_push=passed` 后，即使工作树已经干净，也能从同一个 report 接着 publication；输入变化则 fail closed，不盲目重跑。

### 5. acceptance / publisher 的测试缺口常在部署后才暴露

0.5.0 的 run-id、0.6/0.7 的动态 lifecycle 比较都属于“服务其实正常，但验收脚本自身过期”。这会把发布问题和产品问题混在一起。

**改善：验收器属于 release-Python 门禁的一部分。** 每次 acceptance/publisher 修改必须先跑完整 `scripts/tests`；动态观察字段和 durable receipt 必须分层比较。部署后失败先判断是 harness 还是 runtime，不重复安装健康目标。

### 6. 设备 busy 与 stale 状态需要太多人工诊断

Studio 0.7.1 被一个网关已完成、Agent 本地仍为 running 的 terminal 阻塞；之后又遇到短时全 lane poll 不可用触发 readiness 回滚。

**改善：流程层先做明确诊断，运行时能力继续在 backlog 推进。** 发布器只延期真正 busy 的目标；stale terminal 对账和 readiness 网络容错分别由 backlog 跟踪。未知活动任务绝不自动 kill。

### 7. 文档经常落后运行版本一到两代

README 一度仍写 0.5.0，而三端已经运行 0.7.1，导致后续规划先花时间纠正文档事实。

**改善：发布证据分成历史原始回执与最终小型事实文件。** README / docs index / product README 只引用最终长期证据；大型 Agent 包、临时 lock、带 bearer URL 的原始工作目录不进入 Git。

## 新的默认执行路径

```text
实现
  ↓
scripts/iteration-code.py 自动识别改动范围
  ↓
最小安全门禁
  ├─ docs           → 文档门禁
  ├─ release_python → Python 全回归
  └─ runtime        → 固定快照 + 完整发布流水线
  ↓
输入指纹再次核对
  ↓
commit + push main
  ↓
仅 runtime：publish-code 三端发布
  ↓
每目标验收 / 失败保留原回执
  ↓
小型 release evidence + docs-only 收尾提交
```

常用入口：

```sh
python3 scripts/iteration-code.py \
  --report target/iteration-runs/<id>/iteration.json \
  --commit-message 'feat(code): ...'
```

runtime 发布再附：

```sh
  --publish-config <deploy-config.json> \
  --publish-report-dir docs/releases/<version>/publish
```

只观察，不产生副作用：

```sh
python3 scripts/iteration-code.py \
  --report target/iteration-runs/<id>/iteration.json \
  --status \
  --publish-status-dir docs/releases/<version>/publish
```

## 从现在开始的纪律

1. **功能范围先锁定，再改最终版本号。** 避免版本号变化过早触发整套 Cargo 重新链接。
2. **同一固定候选只保留一条完整流水线。** 长构建只观察原任务，不因“没输出”重启。
3. **运行时输入没变，不重复 Rust 双平台构建。** 验收脚本/文档修复用对应轻量门禁。
4. **验证通过立即 push。** 不让“验证过但还只在本地”的状态长时间存在。
5. **发布失败按阶段恢复。** 已安装健康目标不重复安装；busy、readiness、acceptance 分开处理。
6. **观察频率跟活动状态走。** CPU/日志持续推进时减少轮询；重启窗口和失败阶段才缩短间隔。
7. **最终报告只说事实。** `started`、候选通过、版本字符串、服务重启都不单独等于发布成功。
8. **每轮最多保留两类长期证据。** 人类可读 `RELEASE.md` + 小型机器可读 final JSON；原始大包/锁/临时 URL 不提交。

## 仍需继续优化

流程收敛不能替代运行时问题。下一轮继续优先解决：宿主 schema 刷新与 >64 MiB 原生闭环、stale terminal 自动对账、readiness 网络抖动容错、发布器“仅重跑验收不重装”的原生恢复入口，以及接收端 DB/IO 故障注入矩阵。
