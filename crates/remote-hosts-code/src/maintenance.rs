//! Device-authenticated bounded drain leases. Controls/readers and result delivery continue.
use crate::{gateway::Gateway, now};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    action: String,
    lease_id: String,
    #[serde(default = "ttl")]
    ttl_seconds: i64,
    #[serde(default)]
    receipt: Option<Value>,
}
fn ttl() -> i64 {
    300
}
pub(crate) fn routes(g: Gateway) -> Router {
    Router::new()
        .route("/device/maintenance", post(change))
        .with_state(g)
}
pub(crate) async fn view(g: &Gateway, device: &str) -> Result<Value> {
    Ok(g.store
        .get::<Value>("device_drain", device)
        .await?
        .unwrap_or_else(|| json!({"state":"open","protocol":1})))
}
async fn change(State(g): State<Gateway>, headers: HeaderMap, Json(r): Json<Request>) -> Response {
    let Ok(device) = g.device(&headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    match apply(&g, &device, &r).await {
        Ok(v) => Json(v).into_response(),
        Err(_) => (
            StatusCode::CONFLICT,
            Json(json!({"error":"maintenance_lease_conflict","service_changed":false})),
        )
            .into_response(),
    }
}
async fn apply(g: &Gateway, device: &str, r: &Request) -> Result<Value> {
    ensure!(
        crate::transfers::valid_hash(&r.lease_id),
        "invalid maintenance lease"
    );
    ensure!((30..=900).contains(&r.ttl_seconds), "invalid lease ttl");
    ensure!(
        matches!(
            r.action.as_str(),
            "acquire" | "release" | "status" | "report"
        ),
        "invalid maintenance action"
    );
    if r.action == "status" {
        return view(g, device).await;
    }
    let mut tx = g.store.pool.begin_with("BEGIN IMMEDIATE").await?;
    let previous: Option<(String, i64)> =
        sqlx::query_as("SELECT value,expires FROM kv WHERE kind='device_drain' AND key=?")
            .bind(device)
            .fetch_optional(&mut *tx)
            .await?;
    let mut owned = false;
    if let Some((value, expires)) = previous {
        let value: Value = serde_json::from_str(&value)?;
        owned = expires > now() && value["lease_id"] == r.lease_id;
        ensure!(
            expires <= now() || value["lease_id"] == r.lease_id,
            "maintenance lease belongs to another updater"
        );
    }
    let v = if r.action == "report" {
        ensure!(owned, "receipt requires a live owned drain lease");
        let input = r.receipt.as_ref().context("missing update receipt")?;
        ensure!(
            input.is_object() && serde_json::to_vec(input)?.len() <= 8192,
            "invalid update receipt"
        );
        let mut receipt = json!({});
        for k in [
            "version",
            "candidate_sha256",
            "installed_sha256",
            "state",
            "phase",
            "service_changed",
            "pid",
            "started_at",
            "session",
            "stable_seconds",
            "samples",
            "gateway_verified",
            "all_lanes_verified",
            "active_terminals",
            "active_operations",
        ] {
            if let Some(value) = input.get(k) {
                receipt[k] = value.clone();
            }
        }
        if input["state"] == "failed" {
            receipt["error_code"] = json!(if input["error"]
                .as_str()
                .is_some_and(|s| s.contains("active work"))
            {
                "device_busy"
            } else {
                "update_failed"
            });
        }
        let v = json!({"receipt":receipt,"lease_id":r.lease_id,"reported_at":now(),"protocol":1});
        sqlx::query("INSERT INTO kv VALUES('device_update',?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,expires=excluded.expires").bind(device).bind(v.to_string()).bind(now()+86400).execute(&mut *tx).await?;
        json!({"accepted":true,"protocol":1})
    } else if r.action == "release" {
        sqlx::query("DELETE FROM kv WHERE kind='device_drain' AND key=?")
            .bind(device)
            .execute(&mut *tx)
            .await?;
        json!({"state":"open","protocol":1})
    } else {
        let v = json!({"state":"draining","lease_id":r.lease_id,"expires_at":now()+r.ttl_seconds,"protocol":1,"accepting_new_execution":false});
        sqlx::query("INSERT INTO kv VALUES('device_drain',?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,expires=excluded.expires").bind(device).bind(v.to_string()).bind(now()+r.ttl_seconds).execute(&mut *tx).await?;
        v
    };
    tx.commit().await?;
    g.wake_device(device);
    Ok(v)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn drains_are_owned_expiring_and_do_not_change_services() {
        let d = tempfile::tempdir().unwrap();
        let g = Gateway::new(crate::GatewayConfig {
            public_url: "https://example.test".into(),
            bind: "127.0.0.1:0".into(),
            state_dir: d.path().to_path_buf(),
            owner: "o".into(),
            password_hash: "unused".into(),
            devices: vec![],
            redirect_uris: vec![],
        })
        .await
        .unwrap();
        let r = Request {
            action: "acquire".into(),
            lease_id: crate::hash("a"),
            ttl_seconds: 300,
            receipt: None,
        };
        assert_eq!(apply(&g, "a", &r).await.unwrap()["state"], "draining");
        let mut other = r.clone();
        other.lease_id = crate::hash("b");
        assert!(apply(&g, "a", &other).await.is_err());
        assert_eq!(view(&g, "b").await.unwrap()["state"], "open");
        let mut release = r;
        release.action = "release".into();
        apply(&g, "a", &release).await.unwrap();
        assert_eq!(view(&g, "a").await.unwrap()["state"], "open");
    }
}
