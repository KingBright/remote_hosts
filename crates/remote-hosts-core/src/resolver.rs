//! Access path resolution.

use remote_hosts_domain::{
    AccessPath, AccessPathHealth, AgentHint, Connector, EntityState, StateReasonCode,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Candidate access path plus optional live state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessCandidate {
    /// Access path.
    pub access_path: AccessPath,
    /// Optional connector that can use this path.
    pub connector: Option<Connector>,
    /// Optional health snapshot.
    pub health: Option<AccessPathHealth>,
}

/// Successful access resolution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessResolution {
    /// Selected candidate.
    pub selected: AccessCandidate,
    /// Human-facing reason.
    pub reason: String,
    /// Whether cached state was used.
    pub used_cached_state: bool,
}

/// Access resolution failure.
#[derive(Clone, Debug, Error, Serialize, Deserialize, Eq, PartialEq)]
#[error("{human_message}")]
pub struct AccessResolutionError {
    /// Resulting state.
    pub state: EntityState,
    /// Reason code.
    pub reason_code: StateReasonCode,
    /// Agent hint.
    pub agent_hint: AgentHint,
    /// Human-facing message.
    pub human_message: String,
    /// Retry delay.
    pub retry_after_seconds: Option<u64>,
}

/// Access path resolver.
pub struct AccessResolver;

impl AccessResolver {
    /// Resolves the best access path from a candidate list.
    ///
    /// # Errors
    ///
    /// Returns an actionable error if no candidate is usable.
    pub fn resolve(
        candidates: &[AccessCandidate],
    ) -> Result<AccessResolution, AccessResolutionError> {
        if candidates.is_empty() {
            return Err(AccessResolutionError {
                state: EntityState::NotConfigured,
                reason_code: StateReasonCode::PolicyRejected,
                agent_hint: AgentHint::UseAlternateAccessPath,
                human_message: "no access paths are configured".to_owned(),
                retry_after_seconds: None,
            });
        }

        let mut scored = candidates
            .iter()
            .filter(|candidate| candidate.access_path.enabled)
            .filter_map(score_candidate)
            .collect::<Vec<_>>();

        scored.sort_by_key(|(score, candidate)| (*score, candidate.access_path.priority));

        if let Some((_score, selected)) = scored.into_iter().next() {
            let used_cached_state = selected.health.is_some();
            return Ok(AccessResolution {
                selected: selected.clone(),
                reason: "selected lowest-risk reachable access path".to_owned(),
                used_cached_state,
            });
        }

        Err(best_failure(candidates))
    }
}

fn score_candidate(candidate: &AccessCandidate) -> Option<(u32, &AccessCandidate)> {
    if matches!(
        candidate
            .connector
            .as_ref()
            .map(|connector| &connector.state),
        Some(EntityState::ConnectorOffline | EntityState::CircuitOpen)
    ) {
        return None;
    }

    let health_state = candidate
        .health
        .as_ref()
        .map_or(EntityState::Unknown, |health| health.state.clone());

    match health_state {
        EntityState::Connected | EntityState::Healthy => Some((0, candidate)),
        EntityState::Degraded => Some((10, candidate)),
        EntityState::Unknown | EntityState::Resolving | EntityState::NotConfigured => {
            Some((20, candidate))
        }
        EntityState::AuthFailed
        | EntityState::HostKeyChanged
        | EntityState::CircuitOpen
        | EntityState::Throttled
        | EntityState::TargetOverloaded
        | EntityState::RateLimited
        | EntityState::TcpUnreachable
        | EntityState::SshHandshakeFailed
        | EntityState::ConnectorOffline
        | EntityState::Maintenance => None,
    }
}

fn best_failure(candidates: &[AccessCandidate]) -> AccessResolutionError {
    if candidates.iter().any(|candidate| {
        matches!(
            candidate.health.as_ref().map(|h| &h.state),
            Some(EntityState::AuthFailed)
        )
    }) {
        return AccessResolutionError {
            state: EntityState::AuthFailed,
            reason_code: StateReasonCode::SshAuthFailed,
            agent_hint: AgentHint::AuthFailedDoNotRetry,
            human_message: "all usable access paths are blocked by authentication failure"
                .to_owned(),
            retry_after_seconds: None,
        };
    }

    if let Some((retry_after_seconds, reason_code)) = candidates
        .iter()
        .filter_map(|candidate| candidate.health.as_ref())
        .filter(|health| {
            matches!(
                health.state,
                EntityState::Throttled | EntityState::TargetOverloaded | EntityState::RateLimited
            )
        })
        .filter_map(|health| {
            health.next_retry_at.map(|next_retry| {
                let now = remote_hosts_domain::now_utc();
                (
                    (next_retry - now).whole_seconds().max(0).cast_unsigned(),
                    health
                        .last_error_code
                        .clone()
                        .unwrap_or(StateReasonCode::TargetSshdRateLimited),
                )
            })
        })
        .min_by_key(|(retry_after_seconds, _)| *retry_after_seconds)
    {
        let local_budget = reason_code == StateReasonCode::LocalHandshakeBudgetExhausted;
        return AccessResolutionError {
            state: EntityState::Throttled,
            reason_code,
            agent_hint: AgentHint::UseCachedStateOrWait,
            human_message: if local_budget {
                "the connector local SSH handshake budget is temporarily exhausted".to_owned()
            } else {
                "target access paths are throttled or overloaded".to_owned()
            },
            retry_after_seconds: Some(retry_after_seconds),
        };
    }

    if candidates.iter().any(|candidate| {
        matches!(
            candidate
                .connector
                .as_ref()
                .map(|connector| &connector.state),
            Some(EntityState::ConnectorOffline)
        )
    }) {
        return AccessResolutionError {
            state: EntityState::ConnectorOffline,
            reason_code: StateReasonCode::ConnectorHeartbeatStale,
            agent_hint: AgentHint::ConnectorOfflineTryPublicPath,
            human_message: "required connector is offline".to_owned(),
            retry_after_seconds: Some(30),
        };
    }

    AccessResolutionError {
        state: EntityState::TcpUnreachable,
        reason_code: StateReasonCode::TcpProbeFailed,
        agent_hint: AgentHint::UseAlternateAccessPath,
        human_message: "no enabled access path is currently reachable".to_owned(),
        retry_after_seconds: Some(60),
    }
}

#[cfg(test)]
mod tests {
    use remote_hosts_domain::{
        AccessPath, AccessPathHealth, AccessPathId, ConnectionMode, CredentialId, EntityState,
        EnvironmentId, HostId, Protocol, RouteType, StateReasonCode,
    };

    use super::{AccessCandidate, AccessResolutionError, AccessResolver};

    fn path(priority: i32) -> AccessPath {
        AccessPath {
            id: AccessPathId::new(),
            host_id: HostId::new(),
            environment_id: EnvironmentId::new(),
            connector_id: None,
            protocol: Protocol::Ssh,
            address: format!("10.0.0.{priority}"),
            port: 22,
            username: "ops".to_owned(),
            credential_id: CredentialId::new(),
            route_type: RouteType::Lan,
            proxy_chain: Vec::new(),
            priority,
            enabled: true,
            connection_mode: ConnectionMode::Pooled,
            idle_ttl_seconds: 600,
            keepalive_seconds: 30,
            max_concurrent_channels: 1,
            max_new_connections_per_minute: 1,
            requires_tty: false,
            notes: None,
        }
    }

    fn health(access_path: &AccessPath, state: EntityState) -> AccessPathHealth {
        AccessPathHealth {
            access_path_id: access_path.id,
            state,
            last_checked_at: Some(remote_hosts_domain::now_utc()),
            latency_ms: Some(10),
            failure_count: 0,
            last_error_code: None,
            next_retry_at: None,
        }
    }

    #[test]
    fn chooses_lowest_priority_healthy_path() -> Result<(), AccessResolutionError> {
        let slow = path(20);
        let fast = path(10);
        let resolution = AccessResolver::resolve(&[
            AccessCandidate {
                access_path: slow.clone(),
                connector: None,
                health: Some(health(&slow, EntityState::Healthy)),
            },
            AccessCandidate {
                access_path: fast.clone(),
                connector: None,
                health: Some(health(&fast, EntityState::Healthy)),
            },
        ])?;

        assert_eq!(resolution.selected.access_path.id, fast.id);
        Ok(())
    }

    #[test]
    fn auth_failure_tells_agent_not_to_retry() -> Result<(), Box<dyn std::error::Error>> {
        let p = path(10);
        let result = AccessResolver::resolve(&[AccessCandidate {
            access_path: p.clone(),
            connector: None,
            health: Some(health(&p, EntityState::AuthFailed)),
        }]);

        let error = result
            .err()
            .ok_or_else(|| std::io::Error::other("auth failed path should not resolve"))?;
        assert_eq!(error.reason_code, StateReasonCode::SshAuthFailed);
        Ok(())
    }

    #[test]
    fn local_handshake_budget_is_not_reported_as_target_rate_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let p = path(10);
        let mut local_budget = health(&p, EntityState::Throttled);
        local_budget.last_error_code = Some(StateReasonCode::LocalHandshakeBudgetExhausted);
        local_budget.next_retry_at =
            Some(remote_hosts_domain::now_utc() + time::Duration::seconds(164));

        let error = AccessResolver::resolve(&[AccessCandidate {
            access_path: p,
            connector: None,
            health: Some(local_budget),
        }])
        .err()
        .ok_or_else(|| std::io::Error::other("local budget should defer resolution"))?;

        assert_eq!(
            error.reason_code,
            StateReasonCode::LocalHandshakeBudgetExhausted
        );
        assert!(error.human_message.contains("connector local"));
        assert!(error.retry_after_seconds.is_some_and(|value| value <= 164));
        Ok(())
    }
}
