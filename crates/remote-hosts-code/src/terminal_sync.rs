//! Bounded state replication: observe terminal completion without launching a read job.
use crate::{gateway::Gateway, now, store::Store, terminal::Status};
use anyhow::Result;
use serde_json::{Value, json};
pub(crate) fn valid(s: &Status) -> bool {
    uuid::Uuid::parse_str(&s.id).is_ok()
        && s.workspace_id.len() <= 160
        && s.state.len() <= 32
        && s.output_error.as_ref().is_none_or(|s| s.len() <= 100)
}
pub(crate) async fn collect(s: &Store) -> Result<Vec<Status>> {
    let rows:Vec<(String,)>=sqlx::query_as("SELECT k.value FROM kv k LEFT JOIN (SELECT entity,MAX(seq) AS last FROM work_events WHERE kind='terminal' GROUP BY entity) e ON e.entity=k.key WHERE k.kind='terminal' ORDER BY (json_extract(k.value,'$.state') IN ('running','starting')) DESC,COALESCE(e.last,0) DESC,k.key LIMIT 24").fetch_all(&s.pool).await?;
    rows.into_iter()
        .map(|(v,)| Ok(serde_json::from_str(&v)?))
        .collect()
}
pub(crate) async fn save(
    g: &Gateway,
    device: &str,
    session: &str,
    statuses: &[Status],
) -> Result<()> {
    if statuses.is_empty() {
        return Ok(());
    }
    sqlx::query("INSERT INTO kv(kind,key,value,expires) SELECT 'terminal_observation',j.id,json_object('terminal',json(s.value),'reported_at',?,'session',?),? FROM json_each(?) s JOIN jobs j ON j.id=json_extract(s.value,'$.id') WHERE j.device=? AND json_extract(j.request,'$.tool')='terminal_exec' AND json_extract(j.request,'$.arguments.workspace_id')=json_extract(s.value,'$.workspace_id') AND EXISTS(SELECT 1 FROM kv WHERE kind='online' AND key=? AND json_extract(value,'$.hello.session')=?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,expires=excluded.expires")
        .bind(now()).bind(session).bind(now()+86400).bind(serde_json::to_string(statuses)?).bind(device).bind(device).bind(session).execute(&g.store.pool).await?;
    g.observation_changed.notify_waiters();
    Ok(())
}
pub(crate) async fn observed(g: &Gateway, id: &str) -> Result<Option<Value>> {
    Ok(g.store.get::<Value>("terminal_observation",id).await?.map(|v|json!({"terminal":v["terminal"],"reported_at":v["reported_at"],"stale":!(0..=45).contains(&(now()-v["reported_at"].as_i64().unwrap_or(0))),"output_next_action":"terminal_read; use its independent byte cursor","protocol":1})))
}
