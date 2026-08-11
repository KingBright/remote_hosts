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

        let now = remote_hosts_domain::now_utc();
        let mut scored = candidates
            .iter()
            .filter(|candidate| candidate.access_path.enabled)
            .filter_map(|candidate| score_candidate(candidate, now))
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

        Err(best_failure(candidates, now))
    }
}

fn score_candidate(
    candidate: &AccessCandidate,
    now: time::OffsetDateTime,
) -> Option<(u32, &AccessCandidate)> {
    if matches!(
        candidate
            .connector
            .as_ref()
            .map(|connector| &connector.state),
        Some(EntityState::ConnectorOffline | EntityState::CircuitOpen)
    ) {
        return None;
    }

    if candidate
        .health
        .as_ref()
        .is_some_and(|health| pooled_transport_recovery_is_cooling_down(health, now))
    {
        return None;
    }

    let health_state = candidate
        .health
        .as_ref()
        .map_or(EntityState::Unknown, |health| {
            if access_health_retry_is_ready(health, now) {
                EntityState::Unknown
            } else {
                health.state.clone()
            }
        });

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

fn best_failure(
    candidates: &[AccessCandidate],
    now: time::OffsetDateTime,
) -> AccessResolutionError {
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
            health
                .next_retry_at
                .filter(|next_retry| *next_retry > now)
                .map(|next_retry| {
                    (
                        (next_retry - now).whole_seconds().max(1).cast_unsigned(),
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

    if let Some(error) = retryable_connectivity_failure(candidates, now) {
        return error;
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

fn retryable_connectivity_failure(
    candidates: &[AccessCandidate],
    now: time::OffsetDateTime,
) -> Option<AccessResolutionError> {
    let (state, reason_code, retry_after_seconds) = candidates
        .iter()
        .filter_map(|candidate| candidate.health.as_ref())
        .filter(|health| {
            matches!(
                health.state,
                EntityState::TcpUnreachable | EntityState::SshHandshakeFailed
            ) || pooled_transport_recovery_is_cooling_down(health, now)
        })
        .map(|health| {
            let retry_after_seconds = health
                .next_retry_at
                .filter(|retry_at| *retry_at > now)
                .map_or(30, |retry_at| {
                    (retry_at - now).whole_seconds().max(1).cast_unsigned()
                });
            let fallback_reason = match health.state {
                EntityState::TcpUnreachable => StateReasonCode::TcpProbeFailed,
                EntityState::Degraded => StateReasonCode::PooledTransportInvalidated,
                _ => StateReasonCode::SshHandshakeFailed,
            };
            (
                health.state.clone(),
                health.last_error_code.clone().unwrap_or(fallback_reason),
                retry_after_seconds,
            )
        })
        .min_by_key(|(_, _, retry_after_seconds)| *retry_after_seconds)?;
    let human_message = match reason_code {
        StateReasonCode::PooledTransportInvalidated => {
            "the previous pooled SSH channel was discarded; wait for its short cooldown, then retry one fresh connection".to_owned()
        }
        StateReasonCode::SshHandshakeFailed => {
            "the most recent SSH handshake or channel setup failed; wait before retrying this access path".to_owned()
        }
        StateReasonCode::TcpProbeFailed => {
            "the most recent TCP probe failed; wait before retrying this access path".to_owned()
        }
        _ => "the selected access path is temporarily unavailable; wait before retrying"
            .to_owned(),
    };
    Some(AccessResolutionError {
        state,
        reason_code,
        agent_hint: AgentHint::WaitBeforeRetry,
        human_message,
        retry_after_seconds: Some(retry_after_seconds),
    })
}

fn access_health_retry_is_ready(health: &AccessPathHealth, now: time::OffsetDateTime) -> bool {
    let retry_is_ready = health
        .next_retry_at
        .is_some_and(|next_retry_at| next_retry_at <= now);
    retry_is_ready
        && (matches!(
            health.state,
            EntityState::Throttled
                | EntityState::TargetOverloaded
                | EntityState::RateLimited
                | EntityState::TcpUnreachable
                | EntityState::SshHandshakeFailed
        ) || pooled_transport_recovery(health))
}

fn pooled_transport_recovery_is_cooling_down(
    health: &AccessPathHealth,
    now: time::OffsetDateTime,
) -> bool {
    pooled_transport_recovery(health)
        && health
            .next_retry_at
            .is_some_and(|next_retry_at| next_retry_at > now)
}

fn pooled_transport_recovery(health: &AccessPathHealth) -> bool {
    health.state == EntityState::Degraded
        && health.last_error_code == Some(StateReasonCode::PooledTransportInvalidated)
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

    #[test]
    fn expired_local_handshake_budget_allows_one_retry() -> Result<(), Box<dyn std::error::Error>> {
        let p = path(10);
        let mut local_budget = health(&p, EntityState::Throttled);
        local_budget.last_error_code = Some(StateReasonCode::LocalHandshakeBudgetExhausted);
        local_budget.next_retry_at =
            Some(remote_hosts_domain::now_utc() - time::Duration::seconds(1));

        let resolution = AccessResolver::resolve(&[AccessCandidate {
            access_path: p.clone(),
            connector: None,
            health: Some(local_budget),
        }])?;

        assert_eq!(resolution.selected.access_path.id, p.id);
        assert!(resolution.used_cached_state);
        Ok(())
    }

    #[test]
    fn expired_ssh_failure_allows_one_fresh_connection_attempt()
    -> Result<(), Box<dyn std::error::Error>> {
        let p = path(10);
        let mut failed = health(&p, EntityState::SshHandshakeFailed);
        failed.last_error_code = Some(StateReasonCode::SshHandshakeFailed);
        failed.next_retry_at = Some(remote_hosts_domain::now_utc() - time::Duration::seconds(1));

        let resolution = AccessResolver::resolve(&[AccessCandidate {
            access_path: p.clone(),
            connector: None,
            health: Some(failed),
        }])?;

        assert_eq!(resolution.selected.access_path.id, p.id);
        assert!(resolution.used_cached_state);
        Ok(())
    }

    #[test]
    fn active_ssh_failure_is_not_misreported_as_tcp_unreachable()
    -> Result<(), Box<dyn std::error::Error>> {
        let p = path(10);
        let mut failed = health(&p, EntityState::SshHandshakeFailed);
        failed.last_error_code = Some(StateReasonCode::SshHandshakeFailed);
        failed.next_retry_at = Some(remote_hosts_domain::now_utc() + time::Duration::seconds(30));

        let error = AccessResolver::resolve(&[AccessCandidate {
            access_path: p,
            connector: None,
            health: Some(failed),
        }])
        .err()
        .ok_or_else(|| std::io::Error::other("SSH failure should wait before retrying"))?;

        assert_eq!(error.state, EntityState::SshHandshakeFailed);
        assert_eq!(error.reason_code, StateReasonCode::SshHandshakeFailed);
        assert!(
            error
                .retry_after_seconds
                .is_some_and(|seconds| seconds <= 30)
        );
        Ok(())
    }

    #[test]
    fn pooled_transport_recovery_respects_cooldown_then_allows_one_attempt()
    -> Result<(), Box<dyn std::error::Error>> {
        let p = path(10);
        let mut cooling_down = health(&p, EntityState::Degraded);
        cooling_down.last_error_code = Some(StateReasonCode::PooledTransportInvalidated);
        cooling_down.next_retry_at =
            Some(remote_hosts_domain::now_utc() + time::Duration::seconds(10));

        let error = AccessResolver::resolve(&[AccessCandidate {
            access_path: p.clone(),
            connector: None,
            health: Some(cooling_down.clone()),
        }])
        .err()
        .ok_or_else(|| std::io::Error::other("pooled transport cooldown should defer retry"))?;
        assert_eq!(error.state, EntityState::Degraded);
        assert_eq!(
            error.reason_code,
            StateReasonCode::PooledTransportInvalidated
        );

        cooling_down.next_retry_at =
            Some(remote_hosts_domain::now_utc() - time::Duration::seconds(1));
        let resolution = AccessResolver::resolve(&[AccessCandidate {
            access_path: p.clone(),
            connector: None,
            health: Some(cooling_down),
        }])?;
        assert_eq!(resolution.selected.access_path.id, p.id);
        Ok(())
    }
}
