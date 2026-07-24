//! Connector heartbeat and agent-visible state snapshots.

use remote_hosts_domain::{
    AgentHint, ConnectorId, EntityState, StateEvent, StateEventId, StateReasonCode, StateSnapshot,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Result of applying a connector heartbeat.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorHeartbeatOutcome {
    /// Current agent-visible snapshot.
    pub snapshot: StateSnapshot,
    /// State transition event when the state changed.
    pub event: Option<StateEvent>,
}

/// Converts connector heartbeat observations into state snapshots and transition events.
pub struct ConnectorStateTracker;

impl ConnectorStateTracker {
    /// Records a heartbeat state transition.
    pub fn record_heartbeat(
        connector_id: ConnectorId,
        old_state: EntityState,
        new_state: EntityState,
        observed_at: OffsetDateTime,
    ) -> ConnectorHeartbeatOutcome {
        let reason_code = reason_for_state(&new_state, false);
        let event = (old_state != new_state).then(|| StateEvent {
            id: StateEventId::new(),
            entity_type: "connector".to_owned(),
            entity_id: connector_id.to_string(),
            old_state,
            new_state: new_state.clone(),
            reason_code: reason_code.clone(),
            observed_at,
        });

        ConnectorHeartbeatOutcome {
            snapshot: Self::snapshot(new_state, observed_at, observed_at, 60),
            event,
        }
    }

    /// Builds an agent-visible connector snapshot.
    pub fn snapshot(
        state: EntityState,
        observed_at: OffsetDateTime,
        now: OffsetDateTime,
        stale_after_seconds: u64,
    ) -> StateSnapshot {
        let state_age_seconds = (now - observed_at).whole_seconds().max(0).cast_unsigned();
        let heartbeat_stale = state_age_seconds > stale_after_seconds;
        let visible_state = if heartbeat_stale {
            EntityState::ConnectorOffline
        } else {
            state
        };

        StateSnapshot {
            reason_code: reason_for_state(&visible_state, heartbeat_stale),
            human_message: message_for_state(&visible_state, heartbeat_stale),
            agent_hint: hint_for_state(&visible_state, heartbeat_stale),
            retry_after_seconds: retry_after_for_state(&visible_state, heartbeat_stale),
            confidence: confidence_for_state(heartbeat_stale),
            state: visible_state,
            observed_at,
            state_age_seconds,
        }
    }
}

fn reason_for_state(state: &EntityState, heartbeat_stale: bool) -> StateReasonCode {
    if heartbeat_stale {
        return StateReasonCode::ConnectorHeartbeatStale;
    }

    match state {
        EntityState::ConnectorOffline => StateReasonCode::ConnectorHeartbeatStale,
        EntityState::AuthFailed => StateReasonCode::SshAuthFailed,
        EntityState::SshHandshakeFailed => StateReasonCode::SshHandshakeFailed,
        EntityState::Throttled | EntityState::RateLimited => StateReasonCode::TargetSshdRateLimited,
        EntityState::TargetOverloaded => StateReasonCode::TargetOverloaded,
        EntityState::CircuitOpen => StateReasonCode::CircuitOpen,
        _ => StateReasonCode::None,
    }
}

fn hint_for_state(state: &EntityState, heartbeat_stale: bool) -> Option<AgentHint> {
    if heartbeat_stale {
        return Some(AgentHint::ConnectorOfflineTryPublicPath);
    }

    match state {
        EntityState::ConnectorOffline => Some(AgentHint::ConnectorOfflineTryPublicPath),
        EntityState::AuthFailed => Some(AgentHint::AuthFailedDoNotRetry),
        EntityState::Throttled | EntityState::RateLimited | EntityState::TargetOverloaded => {
            Some(AgentHint::UseCachedStateOrWait)
        }
        EntityState::CircuitOpen => Some(AgentHint::WaitBeforeRetry),
        _ => None,
    }
}

fn retry_after_for_state(state: &EntityState, heartbeat_stale: bool) -> Option<u64> {
    if heartbeat_stale {
        return Some(30);
    }

    match state {
        EntityState::ConnectorOffline => Some(30),
        EntityState::Throttled | EntityState::RateLimited => Some(10),
        EntityState::TargetOverloaded | EntityState::CircuitOpen => Some(60),
        _ => None,
    }
}

fn message_for_state(state: &EntityState, heartbeat_stale: bool) -> String {
    if heartbeat_stale {
        return "connector heartbeat is stale".to_owned();
    }

    match state {
        EntityState::Healthy | EntityState::Connected => "connector is online".to_owned(),
        EntityState::ConnectorOffline => "connector is offline".to_owned(),
        EntityState::Throttled | EntityState::RateLimited => "connector is rate limited".to_owned(),
        EntityState::TargetOverloaded => "target appears overloaded".to_owned(),
        EntityState::CircuitOpen => "connector circuit breaker is open".to_owned(),
        EntityState::AuthFailed => "connector reported authentication failure".to_owned(),
        _ => "connector state is available".to_owned(),
    }
}

fn confidence_for_state(heartbeat_stale: bool) -> f32 {
    if heartbeat_stale { 0.2 } else { 1.0 }
}

#[cfg(test)]
mod tests {
    use remote_hosts_domain::{AgentHint, ConnectorId, EntityState, StateReasonCode, now_utc};

    use super::ConnectorStateTracker;

    #[test]
    fn heartbeat_transition_emits_state_event() -> Result<(), Box<dyn std::error::Error>> {
        let now = now_utc();
        let outcome = ConnectorStateTracker::record_heartbeat(
            ConnectorId::new(),
            EntityState::ConnectorOffline,
            EntityState::Healthy,
            now,
        );

        let event = outcome
            .event
            .ok_or("state change should produce an event")?;
        assert_eq!(event.old_state, EntityState::ConnectorOffline);
        assert_eq!(event.new_state, EntityState::Healthy);
        assert_eq!(outcome.snapshot.state, EntityState::Healthy);
        assert_eq!(outcome.snapshot.reason_code, StateReasonCode::None);
        Ok(())
    }

    #[test]
    fn stale_heartbeat_tells_agent_connector_is_offline() {
        let observed_at = now_utc() - time::Duration::seconds(120);
        let snapshot =
            ConnectorStateTracker::snapshot(EntityState::Healthy, observed_at, now_utc(), 30);

        assert_eq!(snapshot.state, EntityState::ConnectorOffline);
        assert_eq!(
            snapshot.reason_code,
            StateReasonCode::ConnectorHeartbeatStale
        );
        assert_eq!(
            snapshot.agent_hint,
            Some(AgentHint::ConnectorOfflineTryPublicPath)
        );
    }
}
