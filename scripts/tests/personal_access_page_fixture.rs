#![allow(dead_code)]
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
pub fn hash(value: impl AsRef<[u8]>) -> String {
    Sha256::digest(value.as_ref())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub struct DeviceRegistration {
    pub id: String,
    pub name: String,
    pub token_hash: String,
    pub scopes: Vec<String>,
}
pub mod auth {
    pub struct Principal {
        pub owner: String,
        pub scopes: Vec<String>,
    }
}
pub mod gateway {
    pub fn status_html_escape(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#39;")
    }
}
#[path = "../../crates/remote-hosts-code/src/permission_view.rs"]
mod permission_view;
#[path = "../../crates/remote-hosts-code/src/status_view.rs"]
mod status_view;
fn main() {
    let output = std::path::PathBuf::from(std::env::args().nth(1).unwrap());
    std::fs::create_dir_all(&output).unwrap();
    for case in [
        "full",
        "owner",
        "unknown",
        "offline",
        "exec-disabled",
        "account-readonly",
        "device-readonly",
    ] {
        let all = vec![
            "code:read".to_owned(),
            "code:write".to_owned(),
            "terminal:exec".to_owned(),
        ];
        let principal = auth::Principal {
            owner: "fixture".into(),
            scopes: if case == "account-readonly" {
                vec!["code:read".into()]
            } else {
                all.clone()
            },
        };
        let registrations = vec![DeviceRegistration {
            id: "fixture".into(),
            name: "Synthetic device".into(),
            token_hash: "UNUSED_SYNTHETIC_HASH".into(),
            scopes: if case == "device-readonly" {
                vec!["code:read".into()]
            } else {
                all.clone()
            },
        }];
        let mut fleet = json!({"devices":[{"device_id":"fixture","name":"Synthetic device","online":case!="offline","version":"candidate","capabilities":{"roots":["/fixtures/project"],"platform":"linux","allow_write":true,"allow_exec":case!="exec-disabled","maintenance_tasks":{"privileged_executor":false}}}]});
        if case == "unknown" {
            fleet["devices"][0]["capabilities"]["allow_exec"] = Value::Null
        }
        let mut permission = permission_view::snapshot(&fleet, &registrations, &principal);
        if case == "owner" {
            permission_view::owner_status(&mut permission)
        }
        let snapshot = json!({"permissions":permission,"summary":{"state":"no_active_work","active_operations":0,"uncertain_operations":0,"scope_complete":true},"observed_at":1,"operations":[],"requests_without_operation":[]});
        std::fs::write(
            output.join(format!("{case}.html")),
            status_view::render(&snapshot),
        )
        .unwrap();
        std::fs::write(
            output.join(format!("{case}.json")),
            serde_json::to_vec_pretty(&snapshot).unwrap(),
        )
        .unwrap();
    }
}
