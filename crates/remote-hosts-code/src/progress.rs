//! Bounded operation telemetry. Durable results remain authoritative.
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Instant,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Snapshot {
    pub operation_id: String,
    pub phase: String,
    pub bytes_done: u64,
    #[serde(default)]
    pub confirmed_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub resumed_bytes: u64,
    pub retry_count: u32,
    pub elapsed_ms: u64,
    pub average_bps: f64,
    pub instantaneous_bps: f64,
    pub updated_at: i64,
}
impl Snapshot {
    pub fn valid(&self) -> bool {
        uuid::Uuid::parse_str(&self.operation_id).is_ok()
            && matches!(
                self.phase.as_str(),
                "queued"
                    | "waiting_resource"
                    | "running"
                    | "snapshot"
                    | "connecting"
                    | "transferring"
                    | "waiting_retry"
                    | "verifying"
                    | "publishing"
                    | "completed"
                    | "failed"
                    | "paused"
                    | "awaiting_source"
                    | "cancelling"
                    | "cancelled"
            )
            && self.bytes_done <= crate::transfers::MAX_BYTES as u64
            && self
                .total_bytes
                .is_none_or(|n| n <= crate::transfers::MAX_BYTES as u64 && n >= self.bytes_done)
            && self.confirmed_bytes.is_none_or(|n| n <= self.bytes_done)
            && self.resumed_bytes <= self.bytes_done
            && self.retry_count <= 1000
            && self.elapsed_ms <= 31 * 86400 * 1000
            && self.average_bps.is_finite()
            && self.average_bps >= 0.0
            && self.instantaneous_bps.is_finite()
            && self.instantaneous_bps >= 0.0
    }
}
struct State {
    id: String,
    phase: &'static str,
    bytes: u64,
    confirmed: Option<u64>,
    newly_written: u64,
    total: Option<u64>,
    resumed: u64,
    retries: u32,
    started: Instant,
    changed: Instant,
    sampled: Instant,
    sampled_bytes: u64,
    recent_bps: f64,
    updated_at: i64,
    phase_started: Instant,
    phase_ms: std::collections::BTreeMap<String, u64>,
}
#[derive(Clone)]
pub(crate) struct Progress(Arc<Mutex<State>>);
impl Progress {
    pub fn new(id: &str) -> Self {
        let now = Instant::now();
        Self(Arc::new(Mutex::new(State {
            id: id.into(),
            phase: "queued",
            bytes: 0,
            confirmed: None,
            newly_written: 0,
            total: None,
            resumed: 0,
            retries: 0,
            started: now,
            changed: now,
            sampled: now,
            sampled_bytes: 0,
            recent_bps: 0.0,
            updated_at: crate::now(),
            phase_started: now,
            phase_ms: Default::default(),
        })))
    }
    pub fn phase(&self, phase: &'static str) {
        if let Ok(mut s) = self.0.lock()
            && s.phase != phase
        {
            let elapsed = s.phase_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
            let old = s.phase.to_owned();
            *s.phase_ms.entry(old).or_default() += elapsed;
            s.phase_started = Instant::now();
            s.phase = phase;
            s.changed = Instant::now();
            s.updated_at = crate::now();
        }
    }
    pub fn advance(&self, bytes: u64, total: Option<u64>) {
        if let Ok(mut s) = self.0.lock()
            && (bytes != s.bytes || total != s.total)
        {
            let elapsed = s.sampled.elapsed().as_secs_f64();
            if elapsed >= 0.25 || bytes < s.sampled_bytes {
                s.recent_bps = bytes.saturating_sub(s.sampled_bytes) as f64 / elapsed.max(0.001);
                s.sampled = Instant::now();
                s.sampled_bytes = bytes;
            }
            s.newly_written = s
                .newly_written
                .saturating_add(bytes.saturating_sub(s.bytes));
            s.confirmed = s.confirmed.map(|n| n.min(bytes));
            s.bytes = bytes;
            s.total = total;
            s.resumed = s.resumed.min(bytes);
            s.changed = Instant::now();
            s.updated_at = crate::now();
        }
    }
    pub fn restore(&self, bytes: u64, total: Option<u64>, confirmed: u64) {
        if let Ok(mut s) = self.0.lock() {
            s.bytes = bytes;
            s.total = total;
            s.confirmed = Some(confirmed.min(bytes));
            s.resumed = bytes;
            s.sampled_bytes = bytes;
            s.sampled = Instant::now();
            s.recent_bps = 0.0;
            s.updated_at = crate::now();
        }
    }
    pub fn confirm(&self, bytes: u64) {
        if let Ok(mut s) = self.0.lock() {
            s.confirmed = Some(bytes.min(s.bytes));
            s.updated_at = crate::now();
        }
    }
    pub fn retry(&self, resumed: u64) {
        if let Ok(mut s) = self.0.lock() {
            s.retries = s.retries.saturating_add(1);
            s.resumed = resumed.min(s.bytes);
            s.phase = "waiting_retry";
            s.changed = Instant::now();
            s.updated_at = crate::now();
        }
    }
    pub fn timings(&self) -> serde_json::Value {
        let Ok(s) = self.0.lock() else {
            return serde_json::json!({"available":false});
        };
        let mut phases = s.phase_ms.clone();
        *phases.entry(s.phase.into()).or_default() +=
            s.phase_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        serde_json::json!({"clock":"agent_monotonic","total_ms":s.started.elapsed().as_millis(),"phase_ms":phases,"scope":"this execution attempt; not cross-machine wall-clock subtraction"})
    }
    pub fn snapshot(&self) -> Option<Snapshot> {
        let s = self.0.lock().ok()?;
        let elapsed = s.started.elapsed();
        Some(Snapshot {
            operation_id: s.id.clone(),
            phase: s.phase.into(),
            bytes_done: s.bytes,
            confirmed_bytes: s.confirmed,
            total_bytes: s.total,
            resumed_bytes: s.resumed,
            retry_count: s.retries,
            elapsed_ms: elapsed.as_millis().min(u64::MAX as u128) as u64,
            average_bps: s.newly_written as f64 / elapsed.as_secs_f64().max(0.001),
            instantaneous_bps: if s.phase != "transferring" || s.changed.elapsed().as_secs() >= 3 {
                0.0
            } else {
                s.recent_bps
            },
            updated_at: s.updated_at,
        })
    }
}
#[derive(Default)]
pub(crate) struct Registry(Mutex<HashMap<String, Weak<Mutex<State>>>>);
impl Registry {
    pub fn start(&self, id: &str) -> Progress {
        let p = Progress::new(id);
        if let Ok(mut entries) = self.0.lock() {
            entries.retain(|_, value| value.strong_count() > 0);
            entries.insert(id.into(), Arc::downgrade(&p.0));
        }
        p
    }
    pub fn snapshots(&self) -> Vec<Snapshot> {
        let Ok(mut entries) = self.0.lock() else {
            return vec![];
        };
        entries.retain(|_, value| value.strong_count() > 0);
        entries
            .values()
            .filter_map(Weak::upgrade)
            .filter_map(|state| Progress(state).snapshot())
            .take(32)
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tracks_retries_and_drops_completed_handles() {
        let registry = Registry::default();
        let p = registry.start(&uuid::Uuid::new_v4().to_string());
        p.phase("transferring");
        p.advance(4096, Some(8192));
        p.retry(4096);
        let s = registry.snapshots().pop().unwrap();
        assert!(s.valid());
        assert_eq!(s.bytes_done, 4096);
        assert_eq!(s.resumed_bytes, 4096);
        assert_eq!(s.retry_count, 1);
        assert_eq!(s.phase, "waiting_retry");
        drop(p);
        assert!(registry.snapshots().is_empty());
    }
    #[test]
    fn rejects_untrusted_progress_values() {
        let p = Progress::new(&uuid::Uuid::new_v4().to_string());
        let mut s = p.snapshot().unwrap();
        s.average_bps = f64::NAN;
        assert!(!s.valid());
        s.average_bps = 0.0;
        s.phase = "arbitrary text".into();
        assert!(!s.valid());
        s.phase = "running".into();
        s.bytes_done = 10;
        s.total_bytes = Some(9);
        assert!(!s.valid());
    }
}
