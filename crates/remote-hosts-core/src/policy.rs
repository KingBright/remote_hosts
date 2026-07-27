//! Server protection policy for agent-driven remote execution.

use remote_hosts_domain::{AgentHint, EntityState, StateReasonCode};
use serde::{Deserialize, Serialize};

/// Static protection policy attached to a host or access path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServerProtectionPolicy {
    /// Maximum new SSH handshakes per 10 minutes.
    pub max_new_ssh_handshakes_per_10_min: u32,
    /// Maximum parallel exec channels per host.
    pub max_parallel_exec_channels_per_host: u32,
    /// Maximum parallel probe jobs per host.
    pub max_parallel_probe_jobs_per_host: u32,
    /// Maximum persistent PTYs per host.
    pub max_persistent_ptys_per_host: u32,
    /// Maximum queued operations per host.
    pub max_operation_queue_depth_per_host: u32,
    /// Default command timeout in seconds.
    pub default_exec_timeout_seconds: u64,
    /// Default output limit in bytes.
    pub default_output_limit_bytes: usize,
    /// Maximum one-shot input payload for persistent PTY sessions.
    pub max_pty_input_bytes: usize,
    /// Cooldown when overload is detected.
    pub overload_cooldown_seconds: u64,
}

impl ServerProtectionPolicy {
    /// Production baseline designed for agent safety before throughput.
    pub fn production_default() -> Self {
        Self {
            max_new_ssh_handshakes_per_10_min: 10,
            max_parallel_exec_channels_per_host: 1,
            max_parallel_probe_jobs_per_host: 1,
            max_persistent_ptys_per_host: 1,
            max_operation_queue_depth_per_host: 20,
            default_exec_timeout_seconds: 30,
            default_output_limit_bytes: 256 * 1024,
            max_pty_input_bytes: 16 * 1024,
            overload_cooldown_seconds: 300,
        }
    }

    /// Returns a policy decision for current per-host pressure.
    pub fn decide(
        &self,
        queued_operations: u32,
        active_exec_channels: u32,
        active_probe_jobs: u32,
        active_ptys: u32,
        overload_cooldown_active: bool,
    ) -> ProtectionDecision {
        if overload_cooldown_active {
            return ProtectionDecision::deny(
                EntityState::Throttled,
                StateReasonCode::TargetOverloaded,
                AgentHint::UseCachedStateOrWait,
                Some(self.overload_cooldown_seconds),
                "target is in overload cooldown",
            );
        }

        if queued_operations >= self.max_operation_queue_depth_per_host {
            return ProtectionDecision::deny(
                EntityState::Throttled,
                StateReasonCode::PolicyRejected,
                AgentHint::WaitBeforeRetry,
                Some(30),
                "operation queue is full",
            );
        }

        if active_exec_channels >= self.max_parallel_exec_channels_per_host {
            return ProtectionDecision::deny(
                EntityState::RateLimited,
                StateReasonCode::PolicyRejected,
                AgentHint::UseExistingWorkspace,
                Some(5),
                "parallel exec channel limit reached",
            );
        }

        if active_probe_jobs >= self.max_parallel_probe_jobs_per_host {
            return ProtectionDecision::deny(
                EntityState::RateLimited,
                StateReasonCode::PolicyRejected,
                AgentHint::ReduceProbeFrequency,
                Some(10),
                "parallel probe limit reached",
            );
        }

        if active_ptys >= self.max_persistent_ptys_per_host {
            return ProtectionDecision::deny(
                EntityState::RateLimited,
                StateReasonCode::PolicyRejected,
                AgentHint::UseExistingWorkspace,
                Some(10),
                "persistent PTY limit reached",
            );
        }

        ProtectionDecision {
            allowed: true,
            state: EntityState::Healthy,
            reason_code: StateReasonCode::None,
            agent_hint: None,
            retry_after_seconds: None,
            human_message: "request allowed".to_owned(),
        }
    }
}

impl Default for ServerProtectionPolicy {
    fn default() -> Self {
        Self::production_default()
    }
}

/// Result of a protection policy check.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtectionDecision {
    /// Whether execution is allowed.
    pub allowed: bool,
    /// Resulting state.
    pub state: EntityState,
    /// Reason code.
    pub reason_code: StateReasonCode,
    /// Agent hint.
    pub agent_hint: Option<AgentHint>,
    /// Retry delay in seconds.
    pub retry_after_seconds: Option<u64>,
    /// Human-readable message.
    pub human_message: String,
}

impl ProtectionDecision {
    fn deny(
        state: EntityState,
        reason_code: StateReasonCode,
        agent_hint: AgentHint,
        retry_after_seconds: Option<u64>,
        human_message: &str,
    ) -> Self {
        Self {
            allowed: false,
            state,
            reason_code,
            agent_hint: Some(agent_hint),
            retry_after_seconds,
            human_message: human_message.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use remote_hosts_domain::{AgentHint, EntityState};

    use super::ServerProtectionPolicy;

    #[test]
    fn default_policy_denies_when_overload_cooldown_is_active() {
        let decision = ServerProtectionPolicy::default().decide(0, 0, 0, 0, true);
        assert!(!decision.allowed);
        assert_eq!(decision.state, EntityState::Throttled);
        assert_eq!(decision.agent_hint, Some(AgentHint::UseCachedStateOrWait));
    }

    #[test]
    fn default_policy_allows_quiet_host() {
        let decision = ServerProtectionPolicy::default().decide(0, 0, 0, 0, false);
        assert!(decision.allowed);
    }
}
