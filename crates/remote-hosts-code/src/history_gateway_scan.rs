//! Restartable bounded maintenance. Checkpoints contain IDs/counters, not bodies.
use super::*;
use serde::{Deserialize, Serialize};
use std::time::Instant;

pub(super) const READ_BUDGET: usize = 256;
pub(super) const WRITE_BUDGET: usize = 8;
const WALL_BUDGET: Duration = Duration::from_secs(1);
const ONE: &str = "SELECT j.id,j.device,j.request,j.result,j.updated,o.value FROM jobs j LEFT JOIN kv o ON o.kind='terminal_observation' AND o.key=j.id WHERE j.id=? AND j.state='done' AND j.result IS NOT NULL AND NOT EXISTS(SELECT 1 FROM history_result_digests h WHERE h.id=j.id) AND NOT EXISTS(SELECT 1 FROM semantic_guards s WHERE s.operation_id=j.id)";

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct Scan {
    pub at: i64,
    pub after: String,
    pub retained: usize,
    pub bytes: u64,
    pub protected: u64,
    pub item_errors: u64,
    pub oldest: Vec<Oldest>,
    pub scan_finished: bool,
    pub complete: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Oldest {
    id: String,
    at: i64,
    bytes: u64,
}
#[derive(Default, Serialize)]
pub(super) struct Tick {
    pub scanned: usize,
    pub writes: usize,
    pub expired_bodies: u64,
    pub legacy_clocks_initialized: u64,
    pub budget_yield: bool,
}
// A slow initial read must still permit one row of progress; otherwise every
// retry can consume its wall budget fetching the same first page forever.
fn wall_exhausted(started: Instant, tick: &Tick) -> bool {
    (tick.scanned > 0 || tick.writes > 0) && started.elapsed() >= WALL_BUDGET
}

impl Scan {
    pub fn restore(value: &Value, at: i64) -> Self {
        let Ok(saved) = serde_json::from_value::<Self>(value.clone()) else {
            return Self::default();
        };
        let valid_id = |id: &str| uuid::Uuid::parse_str(id).is_ok();
        if saved.at <= 0
            || saved.at > at
            || (!saved.after.is_empty() && !valid_id(&saved.after))
            || saved.oldest.len() > WRITE_BUDGET
            || saved.oldest.iter().any(|v| !valid_id(&v.id))
        {
            return Self::default();
        }
        saved
    }
    pub fn pressure(&self, policy: &Policy) -> bool {
        self.retained > policy.max_items || self.bytes > policy.max_bytes
    }
    pub fn report(&self, tick: &Tick, policy: &Policy) -> Value {
        let mut report = serde_json::to_value(tick).expect("bounded counters serialize");
        report["retained_bodies"] = json!(self.retained);
        report["retained_body_bytes"] = json!(self.bytes);
        report["protected_bodies"] = json!(self.protected);
        report["item_errors"] = json!(self.item_errors);
        report["scan_complete"] = json!(self.complete);
        report["has_more"] = json!(!self.complete);
        report["pressure_remaining"] = json!(self.pressure(policy));
        report["inventory_scope"] = json!(if self.complete {
            "completed live scan estimate"
        } else {
            "partial live scan estimate"
        });
        report["idempotency_records_preserved"] = json!(true);
        report
    }
    fn retain(&mut self, row: &Row, at: i64, policy: &Policy) {
        let bytes = (row.2.len() + row.3.len()) as u64;
        self.retained = self.retained.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        if self.at.saturating_sub(at) >= policy.minimum_seconds {
            self.oldest.push(Oldest {
                id: row.0.clone(),
                at,
                bytes,
            });
            self.oldest
                .sort_unstable_by(|a, b| (a.at, &a.id).cmp(&(b.at, &b.id)));
            self.oldest.truncate(WRITE_BUDGET);
        }
    }
}

/// A tick admits at most eight body/legacy-clock mutations. It never cancels
/// an admitted commit to enforce its cooperative wall budget. On an error the
/// cursor stays before the failed row, while earlier committed progress survives.
pub(super) async fn step(
    store: &Store,
    policy: &Policy,
    at: i64,
    scan: &mut Scan,
    tick: &mut Tick,
) -> Result<()> {
    let started = Instant::now();
    if scan.complete || scan.at == 0 {
        *scan = Scan {
            at,
            ..Scan::default()
        };
    }
    while !scan.scan_finished {
        if tick.scanned >= READ_BUDGET
            || tick.writes >= WRITE_BUDGET
            || wall_exhausted(started, tick)
        {
            tick.budget_yield = true;
            return Ok(());
        }
        let rows: Vec<Row> = sqlx::query_as(SCAN)
            .bind(&scan.after)
            .fetch_all(&store.pool)
            .await?;
        if rows.is_empty() {
            scan.scan_finished = true;
            break;
        }
        for mut row in rows {
            if tick.scanned >= READ_BUDGET
                || tick.writes >= WRITE_BUDGET
                || wall_exhausted(started, tick)
            {
                tick.budget_yield = true;
                return Ok(());
            }
            tick.scanned += 1;
            let (brief, failed, finished_at) = match summary(&row, scan.at) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    scan.protected += 1;
                    scan.after = row.0;
                    continue;
                }
                Err(_) => {
                    scan.protected += 1;
                    scan.item_errors += 1;
                    scan.after = row.0;
                    continue;
                }
            };
            row.4 = if finished_at == 0 {
                let Some((confirmed, initialized)) = legacy_clock(store, &row, scan.at).await?
                else {
                    scan.protected += 1;
                    scan.after = row.0;
                    continue;
                };
                if initialized {
                    tick.writes += 1;
                    tick.legacy_clocks_initialized += 1;
                }
                confirmed
            } else {
                finished_at
            };
            let ttl = if failed {
                policy.failed_seconds
            } else {
                policy.successful_seconds
            };
            if scan.at.saturating_sub(row.4) >= ttl {
                // Count attempted writes as well: racing lifecycle changes must
                // not circumvent the admission budget through no-op transactions.
                tick.writes += 1;
                if retire(store, &row, &brief).await? {
                    tick.expired_bodies += 1;
                    scan.after = row.0;
                    continue;
                }
            }
            scan.retain(&row, row.4, policy);
            scan.after = row.0;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    while scan.pressure(policy) && !scan.oldest.is_empty() {
        if tick.writes >= WRITE_BUDGET || wall_exhausted(started, tick) {
            tick.budget_yield = true;
            return Ok(());
        }
        let candidate = scan.oldest[0].clone();
        let selected: Option<Row> = sqlx::query_as(ONE)
            .bind(&candidate.id)
            .fetch_optional(&store.pool)
            .await?;
        if let Some(mut row) = selected
            && let Ok(Some((brief, _, finished_at))) = summary(&row, scan.at)
        {
            row.4 = if finished_at == 0 {
                store
                    .get::<Value>("history_retention_clock", &row.0)
                    .await?
                    .and_then(|v| v["confirmed_at"].as_i64())
                    .unwrap_or(0)
            } else {
                finished_at
            };
            // A more recent completion invalidates a stale capacity candidate.
            if row.4 > 0
                && row.4 <= candidate.at
                && scan.at.saturating_sub(row.4) >= policy.minimum_seconds
            {
                tick.writes += 1;
                if retire(store, &row, &brief).await? {
                    tick.expired_bodies += 1;
                    scan.retained = scan.retained.saturating_sub(1);
                    scan.bytes = scan.bytes.saturating_sub(candidate.bytes);
                }
            }
        }
        scan.oldest.remove(0);
    }
    scan.complete = true;
    scan.oldest.clear();
    Ok(())
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    #[test]
    fn slow_first_read_cannot_starve_all_progress() {
        let started = Instant::now() - Duration::from_secs(2);
        assert!(!wall_exhausted(started, &Tick::default()));
        assert!(wall_exhausted(
            started,
            &Tick {
                scanned: 1,
                ..Tick::default()
            }
        ));
    }
}
