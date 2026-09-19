//! Typed, secret-free poll diagnostics. Poll recovery never replays a command.
use serde::Serialize;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Failure {
    pub category: &'static str,
    pub stage: &'static str,
    pub http_status: Option<u16>,
    pub retry_after_seconds: Option<u64>,
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "device_poll:{}:{}", self.stage, self.category)
    }
}
impl std::error::Error for Failure {}
impl Failure {
    pub fn at(category: &'static str, stage: &'static str) -> Self {
        Self {
            category,
            stage,
            http_status: None,
            retry_after_seconds: None,
        }
    }
    pub fn transport(stage: &'static str, error: &reqwest::Error) -> Self {
        let category = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connection_failed"
        } else if error.is_decode() {
            "invalid_response"
        } else if error.is_body() {
            "response_body_failed"
        } else {
            "transport_failed"
        };
        Self::at(category, stage)
    }
    pub fn http(status: u16, retry_after: Option<&str>) -> Self {
        let category = match status {
            401 | 403 => "authentication_or_policy_rejected",
            408 | 504 => "gateway_timeout",
            409 => "session_or_protocol_conflict",
            429 => "rate_limited",
            500..=599 => "gateway_unavailable",
            _ => "http_rejected",
        };
        Self {
            category,
            stage: "poll_http",
            http_status: Some(status),
            retry_after_seconds: retry_after
                .and_then(|v| v.parse::<u64>().ok())
                .map(|v| v.clamp(1, 300)),
        }
    }
    pub fn classify(error: &anyhow::Error) -> Self {
        if let Some(f) = error.downcast_ref::<Self>() {
            return f.clone();
        }
        if let Some(e) = error.downcast_ref::<reqwest::Error>() {
            return Self::transport("poll_transport", e);
        }
        if error.downcast_ref::<sqlx::Error>().is_some() {
            return Self::at("local_storage_failed", "poll_prepare");
        }
        Self::at("local_poll_failure", "poll_prepare")
    }
}
#[derive(Default)]
pub struct Health {
    attempts: u32,
    total_failures: u64,
    since: Option<Instant>,
    since_epoch: Option<i64>,
    last_report: Option<Instant>,
    last_failure: Option<Failure>,
}
impl Health {
    pub fn failed(
        &mut self,
        failure: Failure,
        instant: Instant,
        epoch: i64,
    ) -> (Duration, Option<Value>) {
        self.attempts = self.attempts.saturating_add(1);
        self.total_failures = self.total_failures.saturating_add(1);
        if self.since.is_none() {
            self.since = Some(instant);
            self.since_epoch = Some(epoch);
        }
        let retry = failure.retry_after_seconds.unwrap_or_else(|| {
            if matches!(failure.http_status, Some(401 | 403 | 409)) {
                60
            } else {
                2u64.pow(self.attempts.min(5))
            }
        });
        let report = self
            .last_report
            .is_none_or(|at| instant.saturating_duration_since(at) >= Duration::from_secs(60))
            || self.last_failure.as_ref() != Some(&failure);
        self.last_failure = Some(failure.clone());
        let value=report.then(||{
            self.last_report=Some(instant);
            json!({"protocol":1,"state":"retrying","failure":failure,"consecutive_failures":self.attempts,
                "total_failures":self.total_failures,"unavailable_since":self.since_epoch,"observed_at":epoch,
                "retry_after_seconds":retry,"next_action":"retry_device_poll_only","command_replayed":false,
                "evidence_boundary":"poll availability does not determine already running command outcome"})
        });
        (Duration::from_secs(retry), value)
    }
    pub fn succeeded(&mut self, instant: Instant, epoch: i64) -> Option<Value> {
        if self.attempts == 0 {
            return None;
        }
        let attempts = self.attempts;
        let elapsed = self.since.map(|at| {
            instant
                .saturating_duration_since(at)
                .as_millis()
                .min(u64::MAX as u128) as u64
        });
        self.attempts = 0;
        self.last_report = None;
        self.since = None;
        Some(
            json!({"protocol":1,"state":"recovered","recovered_at":epoch,"observed_at":epoch,
            "unavailable_since":self.since_epoch.take(),"outage_duration_ms":elapsed,"failed_attempts":attempts,
            "total_failures":self.total_failures,"last_failure":self.last_failure,"next_action":null,"command_replayed":false}),
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_failures_are_rate_limited_and_recovery_is_explicit() {
        let mut health = Health::default();
        let now = Instant::now();
        let f = Failure::http(503, None);
        let (delay, first) = health.failed(f.clone(), now, 100);
        assert_eq!(delay, Duration::from_secs(2));
        assert_eq!(first.unwrap()["state"], "retrying");
        assert!(
            health
                .failed(f.clone(), now + Duration::from_secs(2), 102)
                .1
                .is_none()
        );
        assert!(
            health
                .failed(f, now + Duration::from_secs(62), 162)
                .1
                .is_some()
        );
        let recovered = health
            .succeeded(now + Duration::from_secs(64), 164)
            .unwrap();
        assert_eq!(recovered["failed_attempts"], 3);
        assert_eq!(recovered["outage_duration_ms"], 64000);
        assert_eq!(recovered["command_replayed"], false);
        assert!(health.succeeded(now, 165).is_none());
        assert_eq!(
            health.failed(Failure::http(503, None), now, 166).0,
            Duration::from_secs(2)
        );
    }
    #[test]
    fn errors_keep_http_class_without_urls_credentials_or_response_body() {
        for (status, category) in [
            (403, "authentication_or_policy_rejected"),
            (429, "rate_limited"),
            (503, "gateway_unavailable"),
            (504, "gateway_timeout"),
        ] {
            let f = Failure::http(status, Some("https://secret.invalid/?token=do-not-log"));
            assert_eq!(f.category, category);
            assert_eq!(f.retry_after_seconds, None);
            assert!(!serde_json::to_string(&f).unwrap().contains("secret"));
        }
        assert_eq!(
            Failure::http(429, Some("999999")).retry_after_seconds,
            Some(300)
        );
    }
    #[test]
    fn category_change_is_not_hidden_and_duration_ignores_wall_clock_jump() {
        let mut h = Health::default();
        let now = Instant::now();
        h.failed(Failure::http(503, None), now, 100);
        let (delay, event) = h.failed(Failure::http(403, None), now + Duration::from_secs(1), 10);
        assert_eq!(delay, Duration::from_secs(60));
        assert_eq!(event.unwrap()["failure"]["http_status"], 403);
        assert_eq!(
            h.succeeded(now + Duration::from_secs(2), 11).unwrap()["outage_duration_ms"],
            2000
        );
    }
}
