# remote-hosts-code 0.7.0 发布记录

0.7.0 是文件通道能力的大步版本：默认单文件限制仍为 64 MiB，capable Agent 可显式协商最高 256 MiB，并新增本地磁盘余量保护和文件源授权状态。

固定源码候选通过 **346 项测试、0 失败**，macOS/Linux release 与打包均成功；源码提交为 `1bbaf74 feat(code): release 0.7.0 negotiated large transfers`。

NAS Gateway 随后成功升级到 0.7.0，但在继续升级两台 0.6 Agent 时，真实公共 Agent 包导出暴露出跨版本接收端问题：最后一个 chunk 的字节已正确写入 staging，但 durable offset/进度持久化阶段失败被 Gateway 统一映射为永久 HTTP 409。旧 Agent 因此不会重试，发布器在 Agent 安装前停止。

这次失败没有被掩盖或重复安装；0.7.0 Gateway 保持健康，两台 Mac 仍在 0.6.0。问题随即进入 0.7.1 热修复：真正的 identity/offset/checksum 冲突继续返回 409，数据库/IO/状态提交等内部暂态错误改为可重试 5xx，并补充端到端重试回归。

0.7.1 的最终修复、现场 checkpoint 恢复和三端部署证据见 [`../0.7.1/RELEASE.md`](../0.7.1/RELEASE.md)。大型发布包、临时锁和工作目录不进入 Git；本记录只保留长期可复现的结论与提交身份。
