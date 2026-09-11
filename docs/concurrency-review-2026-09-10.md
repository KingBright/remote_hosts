# Remote Hosts 并发改进与验证交接

> 后续进展：以下正文保留当时的交接状态。2026-09-10 后续迭代已补验原并发候选，并完成 0.3.0 候选源码的 135 项功能测试及格式、严格 Clippy、工作区检查。新实现还加入独立控制通道、传输进度及入站在线 Range 恢复。详见 `docs/iteration-0.3.0-2026-09-10.md` 和对应验证回执；这不表示 0.3.0 已部署。

日期：2026-09-10。工作设备：MacBook-M2-Max。
项目根目录：`/Users/jinliang/Workspace/remote_hosts`。

## 当前交付状态

本轮实际修改了本机源码，不只是下载快照或提出方案。开始时两台 Mac 的在线设备结果均报告 0.2.0，已有文件传输部署由用户的 Codex 完成。本轮保留这些部署和本机已有改动，没有提交、推送、替换线上二进制或重启服务。

**第一阶段实现通过了 19 项单元测试与 2 项并发回归；后续加入的 Gateway 传输并发及扩展测试尚未完成验证。最终工作树不是已验证发布版本。**

最终格式/Clippy/全量测试/工作区检查的组合终端调用，被工具平台返回“因 OpenAI 无法确定请求的安全状态，已拦截此工具调用。”该命令未执行，不是测试运行后失败。没有通过改换入口重试被拦截的验证。后续做了代码读取、静态检查、针对临时文件清理竞态和测试条件的源码修正，以及交接记录；没有继续执行被拦截的验证。

## 已写入的实现

| 项目 | 代码行为 |
|---|---|
| Agent 分通道调度 | read=8、write=2、transfer=2、terminal=4。每类有自己的有界任务集合和长轮询，不在单个 cycle 内等待大文件结束。 |
| 入队前类别筛选 | Gateway 在领取队列记录时按 lanes 筛选；不是先把大量传输拿到本机，再让它们占满同一个等待队列。 |
| 同一操作去重 | 按 operation_id 协调并发执行，保留 fingerprint、持久化执行标记和完成结果。重复请求不应被并发当成两次动作。 |
| 写入资源隔离 | 写工具按真实规范化工作区根目录协调；同根或父子根串行，兄弟目录可以并行。不仅按 workspace_id 加锁，避免同一目录被开成两个工作区后绕过协调。 |
| 终端资源隔离 | terminal_read/input/cancel 按 terminal_id 协调；其它终端不共用同一把全局锁。已有终端进程数量上限保留。 |
| 活跃任务续租 | 独立心跳携带有界 active_operations，续租本设备已派发的任务；避免任务执行超过原 30 秒派发窗口后被不停再次派发。 |
| 会话约束 | 领取任务的 SQL 再次核对当前设备会话，旧会话已经挂起的 poll 不能只凭入口检查领取新会话的任务。 |
| Gateway 文件接收并发 | 用全局 4、每设备 2 的许可代替整条传输持有的全局 Mutex。相同 operation_id 另有互斥，超容量返回 429 和 Retry-After。 |
| 并发下的容量保护 | 短临界区核对磁盘配额、创建临时文件并预留完整逻辑大小；流式数据处理不持有配额锁。最终发布短暂协调元数据，避免清理与重命名相撞。失败临时文件消失按 NotFound 处理。 |

相关文件均相对 `crates/remote-hosts-code/`：

- `src/scheduler.rs`：新增通道容量、按键锁、重叠根目录写入协调、活跃任务和 Gateway 传输许可。
- `src/agent.rs`：去除全局执行锁，分通道 poll/execute/report，并独立续租。
- `src/gateway.rs`：可选 lanes/active_operations 协议、队列筛选、续租与会话检查。
- `src/transfers.rs`：多传输许可、配额预留和短发布协调。
- `src/lib.rs`：注册内部 scheduler 模块。
- `tests/concurrency.rs`：新增并发回归和链路测试。

没有新增公开 MCP 工具，目录仍是 15 项；没有新增依赖。没有修改已有签名 URL 来源限制、凭据、OAuth、文件哈希校验、原子发布或结果不确定时不盲目重放的保护。

## 已实际验证的部分

先只添加两项测试，在原实现上运行：

1. `stalled_transfer_does_not_block_reads_or_terminal_start`：失败。传输阻塞在本地测试源接口，code_read 在 1 秒测试窗口内无法返回。终端启动也在该测试中并行探测，但第一条失败断言先结束用例，不把它作为独立失败测试计数。
2. `concurrent_duplicate_mutation_returns_one_durable_result`：通过。

第一阶段并发修改后再次运行 `cargo test -p remote-hosts-code --lib --test concurrency`：19 项单元测试通过、上述两项回归通过。测试确认挂起传输期间，直接 Agent.execute 的代码读取和终端启动能完成，且并发重复写入保留同一个持久结果。

这一阶段含 4 项新增调度单元测试：通道容量隔离、重叠根目录协调、同资源互斥/弱引用清理、活跃任务去重/释放。编译提示一个测试未使用许可返回值，后续已修改源码，但严格 Clippy 还未执行。

证据在项目内：

```text
target/concurrency-021/baseline.log
target/concurrency-021/lanes-first.log
```

这些是本机临时 fixture、合成设备和凭据的结果，不是公网性能测试，也不是已上线二进制的并发验收。

## 尚未运行完的新增验证

最终工作树新增到 5 项调度单元测试和 5 项 `concurrency` 集成测试。其中后补内容包括：真实 Agent.run 的四通道轮询链路；Gateway 跳过传输积压及心跳设备隔离；一台设备的请求体故意停顿时另一设备仍完成接收；全局与每设备传输上限；配额预留。

**上述后补测试仅已写入，没有可引用的运行成功结果。** 不得把原版 114 项测试或早一阶段的 21 项通过记录套用到最终工作树。

还需要完成格式化、严格 Clippy、完整回归与工作区检查，并检查高压下同类队列公平性、旧会话长轮询切换及运行中重启。Gateway 的配额检查仍有同步本地磁盘元数据访问；没有完成 NAS/Linux 实机并发压力测试。

## 协议与部署边界

新 Gateway 保留旧 Agent 不传 lanes/active_operations 的串行协议。新 Agent 启动时要求 `/healthz` 中 `dispatch_protocol=1`，避免旧 Gateway 忽略类别筛选后造成错误调度。正式上线应先升级 Gateway，再升级 Agent，并保留回滚与活动任务检查。

本轮没有更改包版本号，源码仍处于 0.2.0 工作树上的未发布改动；发布时需要统一更新版本/构建标识。不要因为文件名中含 concurrency-021 就宣称已经发布 0.2.1。

写入协调覆盖本 Agent 的文件工具，不是对任意外部编辑器或 shell 进程提供全局文件系统事务。同类通道仍有容量上限，密集同根写请求可能占据该类等待位置；本轮没有承诺严格 FIFO 或所有负载下无饥饿。

## 用户要求的其它 P0/P1 尚未解决

- 断点续传、分块检查点和连接超时/idle timeout：本轮尚未实现，文件传输的 180 秒总超时仍在，不能宣传为已修复慢链路大文件失败。
- `bytes_done / total_bytes / elapsed / instantaneous_bps / average_bps / retry_count`：尚未实现；active_operations 是内部租约信息，不等于用户可见的传输进度。
- 内容寻址缓存、秒传、chunk hash、提高 64 MiB 上限：尚未实施。

下一实施单元应在并发完整验证后做可恢复传输与进度；应覆盖 Range 被忽略、ETag/源内容变化、错误 Content-Range、链路中断、checkpoint 与最终原子发布之间的崩溃边界。不得靠删除 SHA-256 或 sync_all 来宣称优化。

Tokio 生命周期与许可行为核对参考：`https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html`、`https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html`。仓库仍使用现有锁定依赖，没有为此升级依赖。
