//! Closed requests, host-bound plans and durable receipts. No shell/argv/path request fields.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const PROTOCOL: u32 = 1;
pub const PLAN_TTL: u64 = 300;
pub const MAX_FRAME: usize = 65536;
pub const ACTION: &str = "remove_legacy_remoteplay";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub protocol: u32,
    pub device_id: String,
    pub grant_id: String,
    pub allowed_uid: u32,
    pub allowed_gid: u32,
    pub home: String,
    pub platform: String,
    pub enabled: bool,
}
impl Policy {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.protocol == PROTOCOL, "unsupported_policy_protocol");
        uuid::Uuid::parse_str(&self.device_id)?;
        uuid::Uuid::parse_str(&self.grant_id)?;
        ensure!(self.allowed_uid != 0, "root_caller_grant_not_allowed");
        ensure!(
            matches!(self.platform.as_str(), "macos" | "linux"),
            "unsupported_platform"
        );
        ensure!(
            self.home.starts_with('/') && self.home.len() < 256,
            "invalid_home"
        );
        ensure!(
            self.home
                .split('/')
                .skip(1)
                .all(|s| !s.is_empty() && s != "." && s != ".."),
            "invalid_home_components"
        );
        ensure!(
            !self.home.chars().any(char::is_control),
            "invalid_home_control_character"
        );
        Ok(())
    }
    pub fn authorize(&self, uid: u32) -> Result<()> {
        self.validate()?;
        ensure!(uid == self.allowed_uid, "caller_uid_not_authorized");
        Ok(())
    }
    pub fn targets(&self) -> Vec<String> {
        if self.platform == "linux" {
            LINUX_UNITS
                .iter()
                .map(|u| format!("/etc/systemd/system/{u}"))
                .collect()
        } else {
            vec![
                format!(
                    "{}/remote_play_test/RemotePlay Unified.app/Contents/MacOS/remote_play",
                    self.home
                ),
                format!(
                    "{}/remote_play_test/RemotePlay Unified.app/Contents/Resources/bin/easytier-core",
                    self.home
                ),
                format!("{}/workspace/vpn/bin/macos/easytier-core", self.home),
            ]
        }
    }
    pub fn current_mac_binary(&self) -> String {
        format!(
            "{}/Applications/RemotePlay.app/Contents/MacOS/remote_play",
            self.home
        )
    }
}
pub const LINUX_UNITS: [&str; 2] = ["remote-play.service", "easytier-remoteplay.service"];
pub const CURRENT_UNIT: &str = "remote-play-current.service";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
    Plan {
        request_id: String,
    },
    Apply {
        request_id: String,
        plan_sha256: String,
    },
    Receipt {
        request_id: String,
    },
    Revoke,
}
impl Request {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Plan { request_id } | Self::Receipt { request_id } => valid_id(request_id),
            Self::Apply {
                request_id,
                plan_sha256,
            } => {
                valid_id(request_id)?;
                ensure!(
                    plan_sha256.len() == 64
                        && plan_sha256
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                    "invalid_plan_digest"
                );
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
pub fn valid_id(id: &str) -> Result<()> {
    let parsed = uuid::Uuid::parse_str(id)?;
    ensure!(
        parsed.to_string() == id,
        "request_id_must_be_canonical_uuid"
    );
    Ok(())
}
pub fn digest<T: Serialize>(value: &T) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FileState {
    pub path: String,
    pub sha256: String,
    pub size: u64,
    pub device: u64,
    pub inode: u64,
    pub uid: u32,
    pub mode: u32,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProcessState {
    pub pid: i32,
    pub uid: u32,
    pub started: String,
    pub executable: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UnitState {
    pub name: String,
    pub load: String,
    pub active: String,
    pub enabled: String,
    pub fragment: String,
    pub drop_ins: String,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Snapshot {
    pub files: Vec<Option<FileState>>,
    pub processes: Vec<ProcessState>,
    pub units: Vec<UnitState>,
    pub current_service_active: bool,
    pub uu_processes: Vec<ProcessState>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Plan {
    pub protocol: u32,
    pub action: String,
    pub request_id: String,
    pub device_id: String,
    pub caller_uid: u32,
    pub policy_sha256: String,
    pub created_at: u64,
    pub expires_at: u64,
    pub snapshot: Snapshot,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Prepared,
    Running,
    Succeeded,
    Failed,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Record {
    pub plan: Plan,
    pub plan_sha256: String,
    pub state: State,
    pub steps: Vec<String>,
    pub error: Option<String>,
    pub after: Option<Snapshot>,
    pub updated_at: u64,
}
