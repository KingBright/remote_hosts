//! Server-rendered task cards. No JavaScript, execution controls or duplicated state store.
use serde_json::Value;
fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
fn text(value: &Value) -> String {
    if value.is_null() {
        "unknown".into()
    } else if let Some(s) = value.as_str() {
        escape(s)
    } else {
        escape(&value.to_string())
    }
}
pub(crate) fn render(snapshot: &Value) -> String {
    let mut html = String::from(
        "<!doctype html><html lang=zh-CN><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><meta http-equiv=refresh content=5><title>Remote Hosts Status</title><style>body{font-family:system-ui;margin:0;background:#11151c;color:#e9edf5}main{max-width:1200px;margin:auto;padding:24px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,350px),1fr));gap:16px}.card{border:1px solid #354153;border-radius:12px;padding:16px;background:#19222e;overflow-wrap:anywhere}h1,h2,h3{line-height:1.3}.muted{color:#adb8c7}.state{font-weight:700}dl{display:grid;grid-template-columns:112px 1fr;gap:6px;margin:12px 0}dt{color:#adb8c7}dd{margin:0}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px}button{padding:8px 16px;font:inherit}code{font-size:12px}</style></head><body><main><h1>Remote Hosts Status</h1><p>Authoritative Gateway state. Heartbeat is not counted as business progress.</p><p class=muted>每 5 秒更新。进程结果、日志完整性和业务验收分别显示；unknown 不代表失败，也不允许重放。</p>",
    );
    html.push_str(&format!("<section class=card><h2>{}</h2><p>观察时间：{} · 活跃操作：{} · 待核对：{} · 当前页完整：{}</p></section>",
        text(&snapshot["summary"]["state"]),text(&snapshot["observed_at"]),text(&snapshot["summary"]["active_operations"]),
        text(&snapshot["summary"]["uncertain_operations"]),text(&snapshot["summary"]["scope_complete"])));
    html.push_str("<h2>设备</h2><div class=grid>");
    if let Some(devices) = snapshot["fleet"]["devices"].as_array() {
        for d in devices {
            html.push_str(&format!(
                "<section class=card><h3>{}</h3><p>版本 {} · 在线 {} · 版本一致 {}</p></section>",
                text(&d["name"]),
                text(&d["version"]),
                text(&d["online"]),
                text(&d["converged"])
            ));
        }
    }
    html.push_str("</div><h2>任务与真实操作</h2><div class=grid>");
    if let Some(items) = snapshot["operations"].as_array() {
        for item in items {
            html.push_str(&format!(
                "<section class=card><h3>{} · {}</h3><p class=state>{}</p><dl>",
                text(&item["device_name"]),
                text(&item["tool"]),
                text(&item["state"])
            ));
            for (label, value) in [
                ("操作", &item["operation_id"]),
                ("工作区", &item["workspace_id"]),
                ("目录", &item["working_directory"]),
                ("通信阶段", &item["transport_state"]),
                ("进程 PID", &item["process"]["pid"]),
                ("进程状态", &item["process"]["state"]),
                ("退出码", &item["process"]["exit_code"]),
                ("最后确认", &item["last_confirmed_event"]),
                ("进度变化时间", &item["last_progress_at"]),
                ("设备心跳", &item["heartbeat_at"]),
                ("状态过期", &item["stale"]),
                ("阻塞原因", &item["blocked_reason"]),
                ("所需操作", &item["receipt"]["user_action"]),
                ("下一步", &item["receipt"]["next_action"]),
                ("业务验收", &item["receipt"]["business_state"]),
                ("证据完整", &item["receipt"]["evidence_complete"]),
            ] {
                html.push_str(&format!("<dt>{label}</dt><dd>{}</dd>", text(value)));
            }
            html.push_str(&format!(
                "</dl><details><summary>查看脱敏回执</summary><pre>{}</pre></details></section>",
                escape(&serde_json::to_string_pretty(&item["receipt"]).unwrap_or_default())
            ));
        }
    }
    html.push_str("</div><h2>尚无远端操作的请求</h2><div class=grid>");
    if let Some(items) = snapshot["requests_without_operation"].as_array() {
        for item in items {
            html.push_str(&format!("<section class=card><h3>{}</h3><p>{}</p><code>{}</code><details><summary>脱敏回执</summary><pre>{}</pre></details></section>",text(&item["tool"]),text(&item["state"]),text(&item["request_id"]),escape(&serde_json::to_string_pretty(item).unwrap_or_default())));
        }
    }
    html.push_str("</div><p class=muted>只展示已授权、仍在保留期内的记录。历史分页或缺失的证据不会被标为整体完成。</p><form method=post action=/status/logout><button type=submit>退出</button></form></main></body></html>");
    html
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn task_cards_escape_data_and_show_unknown_instead_of_fake_progress() {
        let html = render(
            &json!({"summary":{"state":"no_active_work","active_operations":0,"uncertain_operations":0,"scope_complete":true},"operations":[{"device_name":"<script>bad()</script>","tool":"terminal_exec","state":"stale_or_unknown","receipt":{"business_state":"not_evaluated","evidence_complete":false}}]}),
        );
        assert!(!html.contains("<script>"));
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("unknown"));
        assert!(html.contains("not_evaluated"));
        // Responsive CSS may legitimately use 100% width. Reject progress UI,
        // not layout percentages, when the snapshot has no measured denominator.
        assert!(!html.contains("<progress"));
        assert!(!html.contains("aria-valuenow"));
        assert!(!html.contains(">100%<"));
        assert!(html.contains("脱敏回执"));
    }
}
