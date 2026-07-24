//! Host state aggregation.

use remote_hosts_domain::{
    AccessPathHealth, AgentHint, ConnectionSession, EntityState, HostFact, StateReasonCode,
    StateSnapshot,
};
use serde::{Deserialize, Serialize};

/// Input data used to summarize host state for an agent.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HostStateInput {
    /// Connector state.
    pub connector_state: Option<StateSnapshot>,
    /// Access path health snapshots.
    pub access_paths: Vec<AccessPathHealth>,
    /// Connection sessions.
    pub sessions: Vec<ConnectionSession>,
    /// Host facts.
    pub facts: Vec<HostFact>,
}

/// Agent-facing host state aggregate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostStateAggregate {
    /// Overall state.
    pub overall: EntityState,
    /// Primary reason code.
    pub reason_code: StateReasonCode,
    /// Suggested agent hint.
    pub agent_hint: Option<AgentHint>,
    /// Human-facing message.
    pub human_message: String,
    /// Age of newest fact in seconds, if facts are present.
    pub newest_fact_age_seconds: Option<u64>,
    /// Active connection session count.
    pub active_session_count: usize,
}

/// Host state aggregator.
pub struct HostStateAggregator;

impl HostStateAggregator {
    /// Builds a state aggregate from current snapshots.
    pub fn aggregate(input: &HostStateInput) -> HostStateAggregate {
        if let Some(connector) = &input.connector_state
            && connector.state == EntityState::ConnectorOffline
        {
            return HostStateAggregate {
                overall: EntityState::ConnectorOffline,
                reason_code: connector.reason_code.clone(),
                agent_hint: connector.agent_hint.clone(),
                human_message: "connector is offline".to_owned(),
                newest_fact_age_seconds: newest_fact_age(&input.facts),
                active_session_count: active_sessions(&input.sessions),
            };
        }

        let throttled_paths = input
            .access_paths
            .iter()
            .filter(|health| {
                matches!(
                    health.state,
                    EntityState::Throttled
                        | EntityState::TargetOverloaded
                        | EntityState::RateLimited
                )
            })
            .collect::<Vec<_>>();
        if !throttled_paths.is_empty() {
            let local_handshake_budget = throttled_paths.iter().all(|health| {
                health.last_error_code == Some(StateReasonCode::LocalHandshakeBudgetExhausted)
            });
            return HostStateAggregate {
                overall: EntityState::Throttled,
                reason_code: if local_handshake_budget {
                    StateReasonCode::LocalHandshakeBudgetExhausted
                } else {
                    throttled_paths
                        .iter()
                        .find_map(|health| health.last_error_code.clone())
                        .unwrap_or(StateReasonCode::TargetSshdRateLimited)
                },
                agent_hint: Some(AgentHint::UseCachedStateOrWait),
                human_message: if local_handshake_budget {
                    "connector local SSH handshake budget is temporarily exhausted".to_owned()
                } else {
                    "target is throttled or overloaded".to_owned()
                },
                newest_fact_age_seconds: newest_fact_age(&input.facts),
                active_session_count: active_sessions(&input.sessions),
            };
        }

        if input
            .access_paths
            .iter()
            .any(|health| matches!(health.state, EntityState::Connected | EntityState::Healthy))
        {
            return HostStateAggregate {
                overall: EntityState::Healthy,
                reason_code: StateReasonCode::None,
                agent_hint: None,
                human_message: "host has at least one healthy access path".to_owned(),
                newest_fact_age_seconds: newest_fact_age(&input.facts),
                active_session_count: active_sessions(&input.sessions),
            };
        }

        HostStateAggregate {
            overall: EntityState::Unknown,
            reason_code: StateReasonCode::None,
            agent_hint: Some(AgentHint::RefreshFactsBeforeExecution),
            human_message: "host state is unknown or stale".to_owned(),
            newest_fact_age_seconds: newest_fact_age(&input.facts),
            active_session_count: active_sessions(&input.sessions),
        }
    }
}

fn newest_fact_age(facts: &[HostFact]) -> Option<u64> {
    let newest = facts.iter().map(|fact| fact.observed_at).max()?;
    Some(
        (remote_hosts_domain::now_utc() - newest)
            .whole_seconds()
            .max(0)
            .cast_unsigned(),
    )
}

fn active_sessions(sessions: &[ConnectionSession]) -> usize {
    sessions
        .iter()
        .filter(|session| matches!(session.state, EntityState::Connected | EntityState::Healthy))
        .count()
}

#[cfg(test)]
mod tests {
    use remote_hosts_domain::{AccessPathHealth, AccessPathId, EntityState, StateReasonCode};

    use super::{HostStateAggregator, HostStateInput};

    #[test]
    fn throttled_path_dominates_summary() {
        let aggregate = HostStateAggregator::aggregate(&HostStateInput {
            access_paths: vec![AccessPathHealth {
                access_path_id: AccessPathId::new(),
                state: EntityState::TargetOverloaded,
                last_checked_at: Some(remote_hosts_domain::now_utc()),
                latency_ms: None,
                failure_count: 3,
                last_error_code: None,
                next_retry_at: None,
            }],
            ..HostStateInput::default()
        });

        assert_eq!(aggregate.overall, EntityState::Throttled);
    }

    #[test]
    fn healthy_path_makes_host_healthy() {
        let aggregate = HostStateAggregator::aggregate(&HostStateInput {
            access_paths: vec![AccessPathHealth {
                access_path_id: AccessPathId::new(),
                state: EntityState::Connected,
                last_checked_at: Some(remote_hosts_domain::now_utc()),
                latency_ms: Some(12),
                failure_count: 0,
                last_error_code: None,
                next_retry_at: None,
            }],
            ..HostStateInput::default()
        });

        assert_eq!(aggregate.overall, EntityState::Healthy);
    }

    #[test]
    fn local_handshake_budget_does_not_blame_target_sshd() {
        let aggregate = HostStateAggregator::aggregate(&HostStateInput {
            access_paths: vec![AccessPathHealth {
                access_path_id: AccessPathId::new(),
                state: EntityState::Throttled,
                last_checked_at: Some(remote_hosts_domain::now_utc()),
                latency_ms: None,
                failure_count: 0,
                last_error_code: Some(StateReasonCode::LocalHandshakeBudgetExhausted),
                next_retry_at: Some(remote_hosts_domain::now_utc() + time::Duration::seconds(164)),
            }],
            ..HostStateInput::default()
        });

        assert_eq!(aggregate.overall, EntityState::Throttled);
        assert_eq!(
            aggregate.reason_code,
            StateReasonCode::LocalHandshakeBudgetExhausted
        );
        assert!(
            aggregate
                .human_message
                .contains("connector local SSH handshake budget")
        );
    }
}
