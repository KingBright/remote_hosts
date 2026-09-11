# Remote Hosts 0.3.2：准确验收、能力识别与批量观察

日期：2026-09-10。主仓库为 MacBook-M2-Max 上的 `/Users/jinliang/Workspace/remote_hosts`。

## 当前发布结果

**NAS 网关和 MacBook 已实际运行0.3.2，运行哈希与冻结发布包匹配。MacBook稳定就绪及新接口只读现场验收通过；标准写入/文件往返验收被平台拦截，未执行。Studio因无关工作区仍有活动终端，本轮不强制升级，最后在线版本仍0.3.0。整版全部设备验收没有完成。**

| 证据 | 实际结果 |
|---|---|
| `verification-final.json` | 默认门禁194项通过、0失败，其中153 Rust、41 Python；格式、严格Clippy、工作区检查通过。默认跳过1项需主动授权的launchd测试和2项性能用例。 |
| `live-oneshot.json` | 上述launchd测试随后单独启用并通过：临时任务运行1次、退出0、12秒后无重入，测试任务已清理。统计为194默认通过加1项独立探针，不修改原回执。 |
| `nas-updater.json` | NAS升级至0.3.2，PID20154，运行二进制哈希核对通过，旧包和数据库备份保留。 |
| `macbook-updater.json` | MacBook PID88373，9次就绪采样、稳定23秒，五条调度通道均继续前进。 |
| `runtime-and-integrity-final.json` | 复查MacBook仍PID88373、runs648；更新器runs1、退出0；冻结包完整，但发现3项源码后来变化。 |
| `features-macbook.json` | 正常授权的直连MCP验证批量顺序/原结果、不变游标、重复ID拒绝、能力hash匹配/不匹配和Agent功能报告；临时OAuth撤销。不是完整文件写入验收。 |
| `studio-original-outcome.json` | 先前0.3.1更新在空闲门禁退出，二进制未替换。当前其他任务保留，不再称为升级结果未知。 |
| `incidents.json` | 标准MacBook现场验收的原调用被宿主拦截，未执行、未改包装重放；其他独立检查单独记账。 |

发布包：`dist/remote-hosts-code-0.3.2/`，manifest SHA-256 为 `e5465cb75319e856c67b41f1f5ba1789b3ef491cd8a95b514cad2ec2bc5dcc3c`。完整发布状态见 `deployment.json`，不能用此前0.3.1的完整文件验收替代本次缺失门禁。

## 当前迭代边界

本轮接手时工作树已包含0.3.2版本和此前对就绪、更新器、输入指纹检查的后续修改。保留并继续使用这些改动，不还原成冻结的0.3.1。新增内容需要独立完整验证，不能沿用原0.3.1的166项结果。

此前 `verification.json` 的输入集合已发生变化，该次检查状态正确标为stale。它保留为历史证据；本轮新检查在 `target/iteration-032-f/verification.json`，完成回执另存 `verification-final.json`，不覆盖旧回执。

**本次验证/打包以后，又检测到 `src/observations.rs`、`tests/observations_032.rs` 和 `scripts/check-code-source.py` 三项源码输入变化。** 它们已保留，没有回退或覆盖；冻结发布包所有产物哈希不变。194项通过只覆盖被冻结的输入，不覆盖后续工作树。差异见 `post-verification-source-change.json`，其中观察精确预算和部分失败隔离已新增RH-045，计划后续独立验证与发布。

## 使用问题的实际处理

### RH-028：验收范围和文字结论一致

标准现场验收的摘要从最终JSON中的实际设备列表生成，包含设备数量、名称和版本，不再写死“两台通过”。报告同时绑定origin、版本、run_id和明确选中的device_id集合；重复、未选择、错误版本、尚未完成或未撤销临时授权时，不能输出成功摘要。

旧报告缺少选择范围信息时拒绝自动复用。部分完成的同一范围可按原幂等标识恢复，但不能换范围后把历史设备算作本次通过。新增纯Python测试不访问网络或真实设备。

### RH-035：识别工具能力差异，不伪装宿主已刷新

`devices_list` 保留原设备字段，新增gateway块：运行版本、完整工具目录的SHA-256、协议标识和可选输入能力。客户端传入此前记录的 `known_tools_sha256` 时返回match/mismatch以及refresh_required。

新Agent在经过设备鉴权的poll中上报有界runtime_features；旧Agent未上报则明确not_reported，不根据版本号猜功能。网关能力和每个设备的已报告能力保持分离。能力信息是设备声明，不等于当前设备在线或每项功能的现场验收。

宿主决定工具声明的刷新。本改动能展示和比较目录版本，不能从服务端强制刷新当前ChatGPT会话。目录SHA用于识别目录变更，不是二进制构建ID或授权凭据。

### RH-016：一次观察多项工作，少做无变化查询

`operation_get` 支持operation_id或operation_ids二选一，批量最多20个，wait_ms最多5000。可返回最新状态指纹cursor；有状态、阶段、实际字节、重试次数或进度新鲜度变化时唤醒，单纯经过了更多时间不视为业务进展。

观察过程不入队、不续租、不重放原操作，不会因为等待结束而停止原任务。回执和Agent进度通过通知唤醒，保留500ms数据库恢复检查以处理通知遗漏或网关接收端的进度更新。通知先订阅再读取持久状态。

此cursor是最新状态指纹，不是可回放的完整事件日志，不保证展示两次观察间每一个瞬时变化。批量结果按操作分别读取，不宣称跨操作事务快照。批量返回operations和pending_count，不能把启动终端操作的回执完成当成终端命令已退出。

默认单ID调用保留原结果形状和原查询路径，不额外增加状态查询。批量每个ID都检查原所有者、原工具权限和设备权限；无权访问时拒绝整批输出。超过响应预算的结果保留ID并明确result_omitted，要求单独读取，而不是静默截断内容或重新执行操作。

## Studio 0.3.1 的阻塞已经查明

本轮实际读取了原更新任务回执，state=failed，error=`agent still has active work; upgrade not applied`。它不是安装失败或就绪回滚，而是空闲门禁在替换二进制前退出。

后续只读检查确认Studio仍运行0.3.0，另一个工作区有正在运行的终端，unfinished_operation_ids为空。本轮没有取消或重启该任务，也没有让更新器绕过空闲门禁。精确操作与终端标识见 `studio-original-outcome.json`。

## 验证和现场检查

新增网关回归覆盖批量顺序、原单ID兼容、完成回执唤醒、等待不修改租约、混合权限隔离、响应预算、无意义进度过滤、参数拒绝、工具目录比较以及鉴权后的Agent能力上报。

完整Rust/Python检查、两平台构建、NAS与MacBook部署及只读新参数验收均已实际完成，证据见本页顶部。标准变更验收被拦截，仍待完成。本轮现场新参数验证使用正常授权的MCP接口，并记录所选设备；原生devices_list已展示新返回字段，但不将宿主仍缓存的输入schema说成已经刷新。未传known_tools_sha256时的refresh_required=false，仅表示未做客户端比较，不是匹配证明。

工具数量仍为15，没有新增第三方依赖。权限、设备绑定、文件SHA-256、原子发布和幂等保护不改变。完整workspace_context、事件回放、独立文件任务取消、双向跨进程续传和同类队列公平性仍在产品清单中，不被本轮批量观察提前关闭。
