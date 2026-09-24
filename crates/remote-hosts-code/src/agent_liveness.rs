//! Network-only lease watchdog. Local telemetry cannot block authenticated liveness.
//! It observes the same device/session/origin and never dispatches or replays work.
use super::{Agent, DeviceHello};
use crate::poll_health::{Failure, Health};
use anyhow::Result;
use serde_json::json;
use std::{sync::Mutex, time::Duration};
use tokio::time::Instant;

const CONTACT_INTERVAL: Duration = Duration::from_secs(10);
const REQUEST_BUDGET: Duration = Duration::from_secs(8);

#[derive(Default)]
pub(super) struct Liveness {
    acknowledged_at: Mutex<Option<Instant>>,
    response_at: Mutex<Option<Instant>>,
}
impl Liveness {
    pub fn observed(&self, request_started: Instant) {
        // A long-poll reply can arrive long after Gateway renewed last_seen.
        // Its send time is a conservative lower bound, not a fresh renewal now.
        let mut last = self
            .acknowledged_at
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let previous = *last;
        *last = Some(previous.map_or(request_started, |old| old.max(request_started)));
        drop(last);
        *self.response_at.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }
    fn observed_after(&self, at: Instant) -> bool {
        self.response_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some_and(|ack| ack > at)
    }
    fn due_at(&self, at: Instant) -> bool {
        self.acknowledged_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none_or(|ack| at.saturating_duration_since(ack) >= CONTACT_INTERVAL)
    }
}

pub(super) fn network_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(15))
        .http2_keep_alive_interval(Duration::from_secs(15))
        .http2_keep_alive_timeout(Duration::from_secs(5))
        .build()?)
}

impl Agent {
    pub(super) async fn keepalive_loop(&self, hello: &DeviceHello) -> Result<()> {
        // A separate pool keeps long polls/receipt uploads from occupying the
        // watchdog's connection. No alternate origin or implicit device failover.
        self.keepalive_with_client(network_client()?, hello).await
    }

    async fn keepalive_with_client(
        &self,
        mut client: reqwest::Client,
        hello: &DeviceHello,
    ) -> Result<()> {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut retry_at = Instant::now();
        let mut session_conflict_at = None;
        let mut health = Health::default();
        loop {
            tick.tick().await;
            let at = Instant::now();
            // Startup heartbeat may precede the first session-claiming poll.
            // Only a later authenticated round-trip can clear that conflict;
            // an explicit Retry-After or authentication denial is never bypassed.
            if session_conflict_at.is_some_and(|failed_at| self.liveness.observed_after(failed_at))
            {
                retry_at = at;
                session_conflict_at = None;
            }
            if at < retry_at || !self.liveness.due_at(at) {
                continue;
            }
            match self.keepalive_once(&client, hello).await {
                Ok(()) => {
                    if let Some(event) = health.succeeded(std::time::Instant::now(), crate::now()) {
                        tracing::info!(details=%event, "device keepalive recovered");
                    }
                }
                Err(error) => {
                    let failure = Failure::classify(&error);
                    let transport_failed = failure.stage == "keepalive_send";
                    session_conflict_at = (failure.http_status == Some(409)
                        && failure.retry_after_seconds.is_none())
                    .then(Instant::now);
                    let (delay, event) =
                        health.failed(failure, std::time::Instant::now(), crate::now());
                    if let Some(event) = event {
                        // Log only typed fields. Never wait for SQLite here.
                        tracing::warn!(details=%event, "device keepalive unavailable; observation retry only");
                    }
                    // Keep transient transport recovery responsive; preserve all
                    // HTTP Retry-After/auth/conflict backoffs without bypassing policy.
                    let delay = if transport_failed {
                        delay.min(Duration::from_secs(10))
                    } else {
                        delay
                    };
                    retry_at = Instant::now() + delay;
                    if transport_failed && let Ok(fresh) = network_client() {
                        client = fresh;
                    }
                }
            }
        }
    }

    async fn keepalive_once(&self, client: &reqwest::Client, hello: &DeviceHello) -> Result<()> {
        // Exclusively bounded process-local facts. No SQLite, log-file reads,
        // reconciliation, durable writes or manufactured poll-lane timestamps.
        let mut request = serde_json::to_value(hello)?;
        request["active_operations"] = json!(self.active.list()?);
        request["progress"] = json!(self.progress.snapshots());
        let contact_started = Instant::now();
        let response = client
            .post(format!("{}/device/heartbeat", self.config.gateway_url))
            .bearer_auth(&self.config.device_token)
            .json(&request)
            .timeout(REQUEST_BUDGET)
            .send()
            .await
            .map_err(|e| Failure::transport("keepalive_send", &e))?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            let mut failure = Failure::http(
                response.status().as_u16(),
                response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|h| h.to_str().ok()),
            );
            failure.stage = "keepalive_http";
            return Err(failure.into());
        }
        self.liveness.observed(contact_started);
        Ok(())
    }
}

#[cfg(test)]
#[path = "agent_liveness_tests.rs"]
mod tests;
