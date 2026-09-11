//! Bounded workspace handoff. Live keyset pages, never a global event snapshot.
use crate::{
    files::{self, Workspace},
    hash, now,
    store::Store,
    transfer_journal::Journal,
};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalCursor {
    workspace: String,
    active_only: bool,
    busy: i64,
    created: i64,
    key: String,
}
impl TerminalCursor {
    fn decode(text: &str, ws: &Workspace, active_only: bool) -> Result<Self> {
        ensure!(text.len() <= 2048, "invalid terminal cursor");
        let cursor: Self = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(text)
                .context("invalid terminal cursor")?,
        )
        .context("invalid terminal cursor")?;
        ensure!(
            cursor.workspace == ws.id
                && cursor.active_only == active_only
                && (0..=1).contains(&cursor.busy)
                && cursor.created >= 0
                && !cursor.key.is_empty()
                && cursor.key.len() <= 128,
            "terminal cursor belongs to different workspace/filter or is invalid"
        );
        Ok(cursor)
    }
    fn encode(&self) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?))
    }
}

pub(crate) async fn read(store: &Store, ws: &Workspace, args: &Value) -> Result<Value> {
    let limit = files::number(args, "limit", 20, 1, 50)?;
    let active_only = args
        .get("active_only")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let after = args
        .get("transfer_after")
        .and_then(Value::as_str)
        .unwrap_or("");
    ensure!(after.len() <= 128, "invalid transfer cursor");
    let cursor = args
        .get("terminal_cursor")
        .and_then(Value::as_str)
        .map(|text| TerminalCursor::decode(text, ws, active_only))
        .transpose()?;
    let paged = cursor.is_some();
    let cursor = cursor.unwrap_or_else(|| TerminalCursor {
        workspace: ws.id.clone(),
        active_only,
        busy: 1,
        created: i64::MAX,
        key: String::new(),
    });
    let transfer_rows: Vec<(String,)> = sqlx::query_as(
        "SELECT value FROM kv WHERE kind='transfer_local' AND json_extract(value,'$.workspace_id')=? AND key>? AND (?=0 OR json_extract(value,'$.phase') NOT IN ('completed','cancelled','failed','expired')) ORDER BY key LIMIT ?")
        .bind(&ws.id).bind(after).bind(active_only).bind((limit+1) as i64).fetch_all(&store.pool).await?;
    let transfers_truncated = transfer_rows.len() > limit;
    let transfers: Vec<Value> = transfer_rows
        .into_iter()
        .take(limit)
        .map(|(text,)| serde_json::from_str::<Journal>(&text).map(|j| j.view()))
        .collect::<std::result::Result<_, _>>()?;
    // Order active terminals first, then newest creation, then unique key. Cursor
    // encodes all ordering components and the workspace/filter, not an offset.
    let mut rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "WITH scoped AS (SELECT value,key,CASE WHEN json_extract(value,'$.state') IN ('running','starting') THEN 1 ELSE 0 END AS busy,CAST(COALESCE(json_extract(value,'$.created_at'),0) AS INTEGER) AS created FROM kv WHERE kind='terminal' AND json_extract(value,'$.workspace_id')=?) SELECT value,key,busy,created FROM scoped WHERE (?=0 OR busy=1) AND (?=0 OR busy<? OR (busy=? AND (created<? OR (created=? AND key>?)))) ORDER BY busy DESC,created DESC,key LIMIT ?")
        .bind(&ws.id).bind(active_only).bind(paged).bind(cursor.busy).bind(cursor.busy)
        .bind(cursor.created).bind(cursor.created).bind(&cursor.key).bind((limit+1) as i64)
        .fetch_all(&store.pool).await?;
    let terminals_truncated = rows.len() > limit;
    rows.truncate(limit);
    let next_terminal = if terminals_truncated {
        rows.last()
            .map(|(_, key, busy, created)| {
                TerminalCursor {
                    workspace: ws.id.clone(),
                    active_only,
                    key: key.clone(),
                    busy: *busy,
                    created: *created,
                }
                .encode()
            })
            .transpose()?
    } else {
        None
    };
    let terminals: Vec<Value> = rows
        .into_iter()
        .map(|(text, _, _, _)| serde_json::from_str(&text))
        .collect::<std::result::Result<_, _>>()?;
    let (terminal_count, active_terminals): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*),COALESCE(SUM(json_extract(value,'$.state') IN ('running','starting')),0) FROM kv WHERE kind='terminal' AND json_extract(value,'$.workspace_id')=?")
        .bind(&ws.id).fetch_one(&store.pool).await?;
    let (transfer_count, retained, paused): (i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*),COALESCE(SUM(json_extract(value,'$.phase') NOT IN ('completed','cancelled','failed','expired')),0),COALESCE(SUM(json_extract(value,'$.phase') IN ('paused','awaiting_source')),0) FROM kv WHERE kind='transfer_local' AND json_extract(value,'$.workspace_id')=?")
        .bind(&ws.id).fetch_one(&store.pool).await?;
    let build = store.get::<Value>("runtime", "build").await?;
    let delivery = store.get::<Value>("runtime", "receipt_delivery").await?;
    let delivery_stale = delivery
        .as_ref()
        .map(|v| !(0..=45).contains(&(now() - v["reported_at"].as_i64().unwrap_or(0))));
    let mut result = json!({"workspace_id":ws.id,"device_id":ws.device_id,"root":ws.root,
        "runtime":build,"receipt_delivery":delivery,"receipt_delivery_stale":delivery_stale,
        "receipt_delivery_scope":"device-wide counts only; no other workspace records",
        "transfers":transfers,"terminals":terminals,"active_only":active_only,
        "summary":{"terminals":terminal_count,"active_terminals":active_terminals,
            "transfers":transfer_count,"retained_transfers":retained,"paused_transfers":paused},
        "transfers_truncated":transfers_truncated,
        "next_transfer_id":if transfers_truncated {transfers.last().map(|v|v["operation_id"].clone())}else{None},
        "terminal_limit":limit,"terminals_truncated":terminals_truncated,"next_terminal_cursor":next_terminal,
        "scope":"workspace records only; historical exits are not build-verification evidence",
        "consistency":"live pages; state changes can move records, restart pagination for a fresh view",
        "protocol":1,"pagination_protocol":1});
    result["events"] = crate::work_events::read(
        store,
        ws,
        args.get("after_event").and_then(Value::as_str),
        limit,
    )
    .await?;
    let mut identity = result.clone();
    identity["request_page"] = json!({"transfer_after":after,"terminal_cursor":args.get("terminal_cursor"),"after_event":args.get("after_event")});
    if let Some(stats) = identity["receipt_delivery"].as_object_mut() {
        stats.remove("reported_at");
    }
    let fingerprint = hash(serde_json::to_vec(&identity)?);
    if args.get("cursor").and_then(Value::as_str) == Some(&fingerprint) {
        return Ok(
            json!({"workspace_id":ws.id,"changed":false,"cursor":fingerprint,
            "receipt_delivery_stale":delivery_stale,"observed_at":now()}),
        );
    }
    result["changed"] = json!(true);
    result["cursor"] = json!(fingerprint);
    result["observed_at"] = json!(now());
    ensure!(
        serde_json::to_vec(&result)?.len() <= 128 * 1024,
        "workspace_context_budget: reduce limit"
    );
    Ok(result)
}
