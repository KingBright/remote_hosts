//! Read-only permission presentation; never grants, binds or resumes work.
use crate::{DeviceRegistration, auth::Principal};
use serde_json::{Value, json};

pub(crate) fn snapshot(
    fleet: &Value,
    registrations: &[DeviceRegistration],
    p: &Principal,
) -> Value {
    let devices = fleet["devices"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|d| {
            let id = d["device_id"].as_str()?;
            let registration = registrations.iter().find(|r| r.id == id)?;
            let caps = &d["capabilities"];
            let mut view = json!({
                "device_id":id,"name":d["name"],"online":d["online"],"version":d["version"],
                "scopes":registration.scopes,"roots":caps["roots"],"platform":caps["platform"],
                "allow_write":caps["allow_write"],"allow_exec":caps["allow_exec"],
                "capability_reported":caps.is_object(),
                "privileged_executor":caps["maintenance_tasks"]["privileged_executor"]
            });
            view["capabilities"] = capabilities(&view, Some(&p.scopes));
            Some(view)
        })
        .collect::<Vec<_>>();
    json!({"protocol":1,"mode":"existing_authority","account_scope_source":"authenticated_mcp",
        "account_scopes":p.scopes,"ordinary_task_grant_required":false,
        "audit":"automatic_request_and_operation_receipts",
        "existing_task_bindings":"version_expiry_and_revocation_still_enforced","devices":devices})
}
/// The owner browser session is not evidence of a particular MCP client's scopes.
pub(crate) fn owner_status(view: &mut Value) {
    view["account_scope_source"] = json!("owner_status_session_not_mcp_client");
    view["account_scopes"] = Value::Null;
    if let Some(devices) = view["devices"].as_array_mut() {
        for device in devices {
            device["capabilities"] = capabilities(device, None);
        }
    }
}
fn capabilities(device: &Value, account: Option<&Vec<String>>) -> Value {
    let has = |scope: &str| {
        device["scopes"]
            .as_array()
            .is_some_and(|v| v.iter().any(|s| s == scope))
    };
    let values = [
        (
            "code:read",
            "读取代码",
            device["roots"].as_array().map(|r| !r.is_empty()),
        ),
        ("code:write", "修改代码", device["allow_write"].as_bool()),
        (
            "terminal:exec",
            "本机用户执行",
            device["allow_exec"].as_bool(),
        ),
    ]
    .map(|(scope, label, local)| {
        let account_allowed = account.map(|a| a.iter().any(|s| s == scope));
        let registered = has(scope);
        let state = if account_allowed == Some(false) {
            "account_scope_denied"
        } else if !registered {
            "device_scope_denied"
        } else if device["online"] != true {
            "device_offline"
        } else if local == Some(false) {
            match scope {
                "code:write" => "local_write_disabled",
                "terminal:exec" => "local_exec_disabled",
                _ => "local_roots_unavailable",
            }
        } else if local.is_none() {
            "capability_unknown"
        } else if account_allowed.is_none() {
            "registered_connection_unknown"
        } else {
            "available"
        };
        json!({"scope":scope,"label":label,"state":state,
            "account_scope":account_allowed,"device_scope":registered,"local_allowed":local})
    });
    json!(values)
}
pub(crate) fn reason(code: &str) -> Option<&'static str> {
    Some(match code {
        "available" => "当前连接、设备注册及 Agent 条件已允许；操作仍受本机用户权限约束。",
        "registered_connection_unknown" => {
            "设备注册及 Agent 已允许；本页不是 MCP 客户端会话，连接 scope 需由该连接核验。"
        }
        "account_scope_denied" => {
            "当前 MCP 连接未获此 scope。需核对连接授权；刷新工具目录不能补足权限。"
        }
        "device_scope_denied" => {
            "此设备注册未允许该 scope。需核对设备授权；刷新工具目录不能补足权限。"
        }
        "local_write_disabled" => "设备 Agent 的文件写入开关已关闭；需在该设备审查本机设置。",
        "local_exec_disabled" => "设备 Agent 的执行开关已关闭；需在该设备审查本机设置。",
        "local_roots_unavailable" => "Agent 未提供可用代码根目录；需核对该设备的路径设置。",
        "capability_unknown" => "Agent 未报告此能力，当前状态不可确认。",
        "device_offline" => "设备离线；目录和开关可能是上次报告，不能据此确认当前可执行。",
        "platform_authorization_required" => {
            "需要本人在原系统或平台提示中完成认证；MCP 授权不能代替这一步。"
        }
        "access_denied" => {
            "原回执报告路径或系统权限拒绝。MCP scope 不赋予管理员权限；请核对原设备及路径。"
        }
        "task_authorization_version_required" | "task_authorization_version_changed" => {
            "原任务已使用限时限制，须保持原任务并核对实际授权版本，不能换任务 ID 绕过。"
        }
        "task_authorization_expired" | "task_authorization_expiry_required" => {
            "原任务的限时授权已到期或缺少到期信息；原限制仍生效，不能通过新任务 ID 绕过。"
        }
        "task_authorization_revoked" => "原任务授权已撤销；停止该任务并保留原回执。",
        "task_authorization_missing" | "task_scope_denied" | "task_device_denied" => {
            "原任务的设备或 scope 限制未满足；核对原任务限制，不能自动扩大权限。"
        }
        "source_address_policy_rejected" => {
            "原传输被来源地址策略拒绝；限时授权或工具刷新不能修复该边界。"
        }
        _ => return None,
    })
}
pub(crate) fn rejection(receipt: &Value) -> Option<&'static str> {
    receipt["error_code"].as_str().and_then(reason)
}
fn escape(s: &str) -> String {
    crate::gateway::status_html_escape(s)
}
fn display(v: &Value) -> String {
    v.as_str().map(escape).unwrap_or_else(|| {
        if v.is_null() {
            "未报告".into()
        } else {
            escape(&v.to_string())
        }
    })
}
pub(crate) fn render(view: &Value) -> String {
    let mut html = String::from(
        "<section aria-labelledby='device-access'><h2 id='device-access'>连接与访问</h2><p>日常操作复用已连接客户端和设备的现有权限。</p><div class=grid>",
    );
    if let Some(devices) = view["devices"].as_array() {
        for d in devices {
            let connected = match d["online"].as_bool() {
                Some(true) => "已连接",
                Some(false) => "已断开",
                None => "连接待确认",
            };
            let caps = d["capabilities"].as_array();
            let full =
                caps.is_some_and(|v| v.len() == 3 && v.iter().all(|c| c["state"] == "available"));
            let configured = caps.is_some_and(|v| {
                v.len() == 3
                    && v.iter()
                        .all(|c| c["state"] == "registered_connection_unknown")
            });
            let profile = if full {
                "本机用户完全访问"
            } else if configured {
                "本机用户完全访问（设备已允许）"
            } else {
                "访问受限或待确认"
            };
            html.push_str(&format!("<section class=card data-device='{}'><h3>{}</h3><p class=state>{connected} · {profile}</p>",
                display(&d["device_id"]),display(&d["name"])));
            if let Some(caps) = caps {
                if let Some(c) = caps.iter().find(|c| {
                    c["state"] != "available" && c["state"] != "registered_connection_unknown"
                }) {
                    if let Some(reason) = reason(c["state"].as_str().unwrap_or("")) {
                        html.push_str(&format!("<p>{reason}</p>"));
                    }
                }
            }
            html.push_str("<p class=muted>以本机用户权限执行，不授予 OS 管理员权限；需要系统认证的操作仍由本人完成。</p></section>");
        }
    }
    html.push_str("</div><details id='access-disconnect'><summary>撤销或断开</summary><p>请在发起 MCP 连接的客户端中断开或撤销 Remote Hosts。退出此管理页只结束网页会话，不撤销 MCP 连接；本页不会代你撤销权限。</p></details><details id='advanced-permissions'><summary>高级设置</summary>");
    if view["account_scope_source"] == "authenticated_mcp" {
        html.push_str(&format!(
            "<p>当前 MCP 连接 scopes：{}</p>",
            display(&view["account_scopes"])
        ));
    } else {
        html.push_str("<p class=muted>这里显示设备注册和 Agent 报告；所有者浏览器会话不代表某个 MCP 连接的 scopes。</p>");
    }
    html.push_str("<p>代码工具按已授权目录读写。终端使用本机用户权限，能访问代码目录之外。普通 MCP 任务无需额外限时授权或手填任务 ID；每次调用自动关联请求与操作回执。</p>");
    if let Some(devices) = view["devices"].as_array() {
        for d in devices {
            html.push_str(&format!("<section class=card><h3>{}</h3><dl><dt>版本</dt><dd>{}</dd><dt>代码目录</dt><dd>{}</dd><dt>文件写入开关</dt><dd>{}</dd><dt>执行开关</dt><dd>{}</dd></dl>",
                display(&d["name"]),display(&d["version"]),display(&d["roots"]),display(&d["allow_write"]),display(&d["allow_exec"])));
            if let Some(caps) = d["capabilities"].as_array() {
                for c in caps {
                    html.push_str(&format!(
                        "<p><strong>{}</strong> · <code>{}</code><br>{}</p>",
                        display(&c["label"]),
                        display(&c["scope"]),
                        reason(c["state"].as_str().unwrap_or(""))
                            .unwrap_or("当前权限状态不可确认。")
                    ));
                }
            }
            html.push_str("<p class=muted>管理员操作需在该设备完成本机认证。");
            if d["privileged_executor"] == false {
                html.push_str("当前 Agent 未提供管理员执行器。");
            } else {
                html.push_str("本页未确认可用的管理员执行授权。");
            }
            if d["platform"] == "linux" {
                html.push_str("Linux NoNewPrivs=1 会阻止进程提权，不能通过 MCP scope 绕过；本页未上报该运行标志。");
            }
            html.push_str("</p></section>");
        }
    }
    html.push_str("<section id='advanced-authorization'><h3>可选的限时任务限制</h3><p>仅在你主动增加任务限制时使用。已有绑定继续按版本、到期和撤销状态检查；不要换任务 ID 绕过原限制。</p><a href='/status/task-authorization'>设置限时任务限制</a></section></details></section>");
    html
}
#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Value, Vec<DeviceRegistration>, Principal) {
        (
            json!({"devices":[{"device_id":"d","name":"<script>device</script>","online":true,"version":"test","capabilities":{"roots":["/project"],"platform":"linux","allow_write":true,"allow_exec":true,"maintenance_tasks":{"privileged_executor":false},"session":"SECRET_SESSION"}}]}),
            vec![DeviceRegistration {
                id: "d".into(),
                name: "test".into(),
                token_hash: "SECRET_HASH".into(),
                scopes: crate::SCOPES
                    .split_whitespace()
                    .map(str::to_owned)
                    .collect(),
            }],
            Principal {
                owner: "owner".into(),
                scopes: crate::SCOPES
                    .split_whitespace()
                    .map(str::to_owned)
                    .collect(),
            },
        )
    }
    #[test]
    fn permission_view_existing_authority_is_metadata_only_and_redacted() {
        let (f, r, p) = fixture();
        let v = snapshot(&f, &r, &p);
        assert_eq!(v["ordinary_task_grant_required"], false);
        assert_eq!(v["devices"][0]["capabilities"][2]["state"], "available");
        let html = render(&v);
        assert!(html.contains("NoNewPrivs=1"));
        assert!(html.contains("不授予 OS 管理员权限"));
        assert!(html.contains("&lt;script&gt;"));
        assert!(!html.contains("<script>"));
        assert!(!v.to_string().contains("SECRET"));
        assert!(!html.contains("SECRET"));
        assert!(!html.contains("<form"));
        assert!(!html.contains("name=task_id"));
    }
    #[test]
    fn permission_view_owner_browser_cannot_claim_mcp_client_scope() {
        let (f, r, p) = fixture();
        let mut v = snapshot(&f, &r, &p);
        owner_status(&mut v);
        assert!(v["account_scopes"].is_null());
        assert_eq!(
            v["devices"][0]["capabilities"][0]["state"],
            "registered_connection_unknown"
        );
        assert!(render(&v).contains("不代表某个 MCP 连接"));
    }
    #[test]
    fn permission_view_account_device_and_local_denials_are_distinct() {
        let (mut f, mut r, mut p) = fixture();
        p.scopes = vec!["code:read".into()];
        assert_eq!(
            snapshot(&f, &r, &p)["devices"][0]["capabilities"][2]["state"],
            "account_scope_denied"
        );
        p.scopes = crate::SCOPES
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        r[0].scopes = vec!["code:read".into()];
        assert_eq!(
            snapshot(&f, &r, &p)["devices"][0]["capabilities"][2]["state"],
            "device_scope_denied"
        );
        r[0].scopes = p.scopes.clone();
        f["devices"][0]["capabilities"]["allow_write"] = json!(false);
        f["devices"][0]["capabilities"]["allow_exec"] = json!(false);
        let v = snapshot(&f, &r, &p);
        assert_eq!(
            v["devices"][0]["capabilities"][1]["state"],
            "local_write_disabled"
        );
        assert_eq!(
            v["devices"][0]["capabilities"][2]["state"],
            "local_exec_disabled"
        );
        f["devices"][0]["online"] = json!(false);
        assert_eq!(
            snapshot(&f, &r, &p)["devices"][0]["capabilities"][2]["state"],
            "device_offline"
        );
    }
    #[test]
    fn permission_view_unknown_local_capability_does_not_mean_enabled() {
        let (mut f, r, p) = fixture();
        f["devices"][0]["capabilities"]["allow_exec"] = Value::Null;
        assert_eq!(
            snapshot(&f, &r, &p)["devices"][0]["capabilities"][2]["state"],
            "capability_unknown"
        );
    }
    #[test]
    fn permission_view_rejection_guidance_preserves_original_boundaries() {
        for code in ["account_scope_denied", "device_scope_denied"] {
            assert!(reason(code).unwrap().contains("刷新工具目录不能补足权限"));
        }
        assert!(
            reason("task_authorization_expired")
                .unwrap()
                .contains("不能通过新任务 ID")
        );
        assert!(
            reason("platform_authorization_required")
                .unwrap()
                .contains("本人")
        );
        assert!(
            reason("local_exec_disabled")
                .unwrap()
                .contains("开关已关闭")
        );
        assert!(
            reason("local_write_disabled")
                .unwrap()
                .contains("开关已关闭")
        );
    }

    #[test]
    fn permission_view_personal_default_hides_scope_and_task_forms() {
        let (f, r, p) = fixture();
        let v = snapshot(&f, &r, &p);
        let html = render(&v);
        let default = html
            .split("<details id='advanced-permissions'>")
            .next()
            .unwrap();
        assert!(default.contains("已连接 · 本机用户完全访问"));
        assert!(default.contains("撤销或断开"));
        assert!(!default.contains("code:read"));
        assert!(!default.contains("/project"));
        assert!(!default.contains("限时"));
        assert!(!default.contains("Task ID"));
        assert!(html.contains("<details id='advanced-permissions'><summary>高级设置</summary>"));
        assert!(!html.contains("<details id='advanced-permissions' open"));
        assert!(html.contains("不撤销 MCP 连接"));
    }
}
