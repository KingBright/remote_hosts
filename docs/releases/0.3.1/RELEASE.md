# Remote Hosts 0.3.1：发布可靠性与日常交互效率

日期：2026-09-10。主仓库：MacBook-M2-Max 的 `/Users/jinliang/Workspace/remote_hosts`。本轮保留现有用户改动和冻结的 0.3.0 发布包，不创建分支，不自动提交或推送。

## 范围与当前门禁

**冻结的 0.3.1 发布包已完成验证与部分上线：166 项测试通过，其中 Rust 144 项、Python 22 项；格式、严格 Clippy、全工作区检查通过。NAS 网关和 MacBook 已实际运行 0.3.1，MacBook 现场功能验收通过。Mac Studio 候选已送达，一次性升级已提交，但最后仍在线报告 0.3.0，结果诊断和指定设备验收被工具平台拦截，不能标记全设备发布完成。** MCP 工具仍为 15 项，没有新增 Rust 或 Python 第三方依赖。

发布状态见 `deployment.json`，实际设备范围以各设备 JSON 回执为准。MacBook 标准验收脚本的末行仍误打印“both live devices”，而 `acceptance-macbook.json` 正确只列一个设备；此摘要缺陷继续跟踪 RH-028。

**验证完成后又检测到工作树中的 7 个源码输入发生变化，本轮未回退或覆盖这些改动。** 冻结的 `dist/remote-hosts-code-0.3.1/` 中所有产物和 manifest 哈希均未变化。166 项通过只适用于冻结候选输入，不能当作后来工作树变更的验证结果；具体差异见 `post-verification-source-change.json`，后续实现需重新验证。

主要对应产品问题 RH-016、019、020、025、026、029、042、043、044。RH-019 与 RH-020 已附测试和 MacBook 现场证据关闭；其余按实际范围标记部分完成或候选验证。清单仍为 44 项，不因发布而删除未完成项。

### 已取得的运行与使用证据

| 门禁 | 结果与证据 |
|---|---|
| 源码验证 | `verification.json`：166 通过、0 失败，两个既有性能用例按设计 ignored。 |
| NAS | `nas-updater.json`：0.3.1、PID3338，运行二进制哈希匹配，旧版备份保留。 |
| MacBook 控制面就绪 | `macbook-updater.json`：PID43322，7次采样、稳定16.6秒，五条轮询通道确认。 |
| MacBook 一次性更新 | `macbook-runtime-observation.json`：更新任务runs=1、退出0，正式Agent保持PID43322。 |
| 原有功能 | `acceptance-macbook.json`：读写、搜索、符号、终端及2 MiB二进制上传/下载、Range、重试、拒绝覆盖和错误哈希全部通过；临时OAuth撤销。 |
| 新交互功能 | `features-macbook.json`：77,005字节中文/emoji文本分3页完整还原，20个同文件范围物理读取1次，部分批次结果保留，短命令一次返回且重试不重执行。 |
| 原生文件参数 | `native-macbook.json`：51,200字节原生上传、网关导出及独立本机SHA-256通过；测试源文件已核对后清理。沙箱重新下载未执行，不冒充通过。 |
| Mac Studio | `studio-stage.json`：8,401,720字节候选归档经授权网关完整传入，SHA-256一致；升级启动确认不等于升级成功。 |

Studio 原更新任务为 `com.remote-hosts.code-upgrade.ed42e2678742ce3d50775dbb`，结果位置为 `/Users/jinliang/.local/share/remote-hosts-code/releases/upgrade-jobs/ed42e2678742ce3d50775dbb/result.json`。结果未取得前不能换新键再安装，也不能推断它已回滚或安装失败。被拦截的两个动作和网络路径观察见 `incidents.json`。

本对话的宿主工具声明仍未暴露 `wait_ms` 等新增入参，重新发现工具也未刷新；新参数通过正常授权的直连 MCP 验收，不等同于 ChatGPT 原生参数声明已经更新。RH-035 已提升到下一轮优先处理。

## 实际改造

### 升级不再依靠一次瞬间的 PID

Gateway 新增设备鉴权的 `GET /device/readiness`。它只读取当前设备的注册会话，不领取任务、不续租、不更新心跳、不允许读取其他设备。健康接口报告 `readiness_protocol=1`，调度协议仍为 2。

Agent 在当前进程和会话下，记录五条通道成功完成 HTTP 轮询、解析响应后的时间。启动阶段遇到短暂网关不可用或协议不匹配时，在同一进程内退避重试，而不是立刻退出让 launchd 重启。

升级器先验证网关支持就绪观察，再切换文件；之后要求同一 PID/会话连续稳定至少 15 秒，本机版本标记、五条实际轮询确认和网关设备注册信息一致且新鲜。超时按已保存哈希恢复旧二进制，并再次检查旧版本回连。对 0.3.0 等旧版本回退，不虚构旧版本并不存在的五通道就绪记录。

上述是稳定的控制面就绪，不代替读写、终端、文件往返等独立功能验收。外部编辑器/新任务与升级空闲检查之间还没有完整 drain/fencing 协议，不能称为所有工作负载下无损升级。

### 明确的一次性启动入口

`scripts/launch-code-upgrade.py` 将更新器及辅助模块快照到独立任务目录，使用固定的绝对解释器路径。plist 显式配置 `RunAtLoad=true`、`KeepAlive=false`，没有定时、目录监视或登录目录注册；不使用 `launchctl submit`。

任务由候选路径、哈希及版本生成稳定标识。独占的启动意图记录阻止重复 bootstrap；启动结果不确定时停止自动重放，保留人工诊断依据。同一已安装哈希直接返回 `no_change`，不重启 Agent，也不覆盖原有升级回执。新任务没有旧回执时，仍保存本次 no-change 结果。

已在真实 MacBook launchd 上完成不更换二进制的 smoke：任务 runs=1、退出码0；Agent PID1927、runs644 保持不变。测试任务随后卸载。证据为 `target/iteration-031/oneshot-observation.json`。这个测试验证启动器行为，不是一次真实升级失败回滚测试。

### 同一批读取复用快照，超长行可以推进

同一批 `code_read` 按路径复用文本与哈希，设置32 MiB批次快照上限。测试中的20个同文件范围对应1次物理读取、19次缓存命中；不是宣称公网速度提升20倍。

单行超过输出预算时返回保持 UTF-8 边界的片段、`next_line` 和 `next_line_byte_offset`。继续读取使用 `line_byte_offset`，非零偏移必须提供原 `expected_version`，防止跨版本拼接。测试逐段重建含中文、emoji的约77KB单行，内容完整一致。

`allow_partial=true` 时，单个文件缺失或版本冲突不丢弃其它成功范围；逐范围返回错误码及可用的当前版本。默认保持遇错失败，兼容现有调用方。

### 短命令一次返回初始输出

`terminal_exec` 增加可选 `wait_ms=0..2000`，默认0保持原行为。指定短等待后可在同一次调用中返回输出、稳定游标、退出码及捕获状态；等待到期不杀进程，也不重新执行。观察失败仍保留 terminal_id，引导查询原任务。

测试覆盖非零退出码、同幂等键重放不再次写入、等待到期后继续读取原终端且不重复字节。它没有取代所有 operation_get/terminal_read 多层观察协议；RH-016 仍只是部分解决。

### 统一验证回执与打包门禁

`scripts/check-code-source.py` 执行固定的格式、严格 Clippy、Rust回归、Python回归及工作区检查；记录源码/依赖输入指纹、各命令退出码、用时、日志路径和SHA-256。输入发生变化时标为stale，不报告passed。

打包器拒绝未完成、失败、stale或缺失必要门禁的回执。发布包包含新的升级器辅助模块和一次性启动入口，不能仅复制 upgrade-code-agent.py 而漏掉它的同目录依赖。

## 本轮已观察的问题

1. 原生file_download导出140,017字节源码快照完成，但沙箱下载遭URL来源限制。未绕过限制，继续使用已授权代码读取。属于RH-030宿主路径边界，不宣称文件导出失败。
2. 第一次检查器准备阶段使用no-deps metadata未同步锁文件，locked门禁准确拒绝。第二次全依赖metadata触及未缓存atomic-polyfill。已将准备限定为目标包的本机offline check，并为准备失败生成独立机器回执。早期日志不删除。
3. 真正一次性任务的no_change分支原先只输出stdout，新的任务没有result.json。已补为“没有旧回执时写新回执”，并添加专门测试；已有历史回执继续保持不变。

## 验证入口

```sh
/opt/homebrew/bin/python3 scripts/check-code-source.py --report target/iteration-031/verification-3.json
```

输出报告使用不覆盖创建。重复验证请指定新文件名。最终源码变化后必须重新验证；不要将本轮早期38项通过套用到后续改动。

## 保留限制

双向跨进程续传、授权刷新、独立传输取消、严格同类队列公平性、完整workspace_context、结构化输出去重、1000条历史列表恢复及后台GC等仍按产品清单规划。64 MiB文件上限保留。

本轮不改变设备身份、凭据、根目录授权、SHA-256校验或文件原子发布保护。新字段进入运行端后，宿主若缓存旧工具schema，需要刷新能力；直接MCP验收与原生ChatGPT能力暴露必须分开记录。
