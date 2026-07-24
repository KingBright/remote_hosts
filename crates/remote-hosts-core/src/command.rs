//! Command profiles and validation.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ServerProtectionPolicy;

/// Command risk class.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandClass {
    /// Reads state without mutation.
    ReadOnly,
    /// Performs bounded build or local compute.
    Build,
    /// Sensitive or mutating command.
    Sensitive,
}

/// A structured command profile that can be exposed safely to agents.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandProfile {
    /// Profile name.
    pub name: String,
    /// Program name.
    pub program: String,
    /// Fixed or templated argument list.
    pub args: Vec<String>,
    /// Command class.
    pub class: CommandClass,
    /// Timeout in seconds.
    pub timeout_seconds: u64,
    /// Maximum captured output bytes.
    pub output_limit_bytes: usize,
    /// Whether a TTY is required.
    pub requires_tty: bool,
}

/// Command validation error.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum CommandValidationError {
    /// Program is empty.
    #[error("program must not be empty")]
    EmptyProgram,
    /// Program is not on the allowlist.
    #[error("program `{0}` is not allowed for this command class")]
    ProgramNotAllowed(String),
    /// Shell metacharacter was detected.
    #[error("argument `{0}` contains shell metacharacters; use structured arguments")]
    ShellMetacharacter(String),
    /// A shell script profile has an invalid executable or argument shape.
    #[error("invalid shell script profile: {0}")]
    InvalidShellProfile(String),
    /// Timeout is invalid.
    #[error("timeout must be between 1 and {max} seconds, got {actual}")]
    InvalidTimeout {
        /// Maximum timeout.
        max: u64,
        /// Actual timeout.
        actual: u64,
    },
    /// Output limit is invalid.
    #[error("output limit must be between 1 and {max} bytes, got {actual}")]
    InvalidOutputLimit {
        /// Maximum output bytes.
        max: usize,
        /// Actual output bytes.
        actual: usize,
    },
}

/// Public description of a built-in command profile.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandProfileInfo {
    /// Stable profile name accepted by agent-facing tools.
    pub name: &'static str,
    /// Short description of the profile.
    pub description: &'static str,
    /// Command class.
    pub class: CommandClass,
    /// Example argument lists.
    pub examples: Vec<Vec<&'static str>>,
}

/// Error returned when a built-in command profile cannot be resolved.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum CommandProfileResolutionError {
    /// Profile name is unknown.
    #[error("unknown command profile `{0}`")]
    UnknownProfile(String),
    /// Profile does not accept the supplied argument count.
    #[error("profile `{profile}` accepts at most {max} arguments, got {actual}")]
    TooManyArguments {
        /// Profile name.
        profile: String,
        /// Maximum accepted argument count.
        max: usize,
        /// Actual argument count.
        actual: usize,
    },
    /// Argument is not allowed for this profile.
    #[error("argument `{argument}` is not allowed for profile `{profile}`")]
    ArgumentNotAllowed {
        /// Profile name.
        profile: String,
        /// Rejected argument.
        argument: String,
    },
    /// Resolved command failed built-in validation.
    #[error(transparent)]
    Validation(#[from] CommandValidationError),
}

/// Built-in command profile catalog exposed to agents.
#[derive(Clone, Debug, Default)]
pub struct CommandProfileCatalog;

impl CommandProfileCatalog {
    /// Lists built-in profiles.
    pub fn list_builtin() -> Vec<CommandProfileInfo> {
        vec![
            CommandProfileInfo {
                name: "host.identity",
                description: "Print the remote hostname.",
                class: CommandClass::ReadOnly,
                examples: vec![vec![]],
            },
            CommandProfileInfo {
                name: "host.uname",
                description: "Print kernel and architecture information.",
                class: CommandClass::ReadOnly,
                examples: vec![vec!["-a"], vec!["-m"]],
            },
            CommandProfileInfo {
                name: "host.uptime",
                description: "Print uptime and load average.",
                class: CommandClass::ReadOnly,
                examples: vec![vec![]],
            },
            CommandProfileInfo {
                name: "host.whoami",
                description: "Print the effective remote user.",
                class: CommandClass::ReadOnly,
                examples: vec![vec![]],
            },
            CommandProfileInfo {
                name: "disk.usage",
                description: "Print filesystem usage with df.",
                class: CommandClass::ReadOnly,
                examples: vec![vec!["-h"], vec!["-h", "/"]],
            },
            CommandProfileInfo {
                name: "memory.free",
                description: "Print memory usage with free.",
                class: CommandClass::ReadOnly,
                examples: vec![vec!["-h"]],
            },
            CommandProfileInfo {
                name: "process.snapshot",
                description: "Print a process snapshot with ps aux.",
                class: CommandClass::ReadOnly,
                examples: vec![vec![]],
            },
            CommandProfileInfo {
                name: "gpu.nvidia_smi",
                description: "Print bounded NVIDIA GPU status.",
                class: CommandClass::ReadOnly,
                examples: vec![
                    vec!["-L"],
                    vec!["--query-gpu=name,utilization.gpu", "--format=csv"],
                ],
            },
            CommandProfileInfo {
                name: "service.status",
                description: "Print systemd service status for one unit.",
                class: CommandClass::ReadOnly,
                examples: vec![vec!["sshd.service"]],
            },
            CommandProfileInfo {
                name: "shell.posix",
                description: "Run a POSIX shell script through the existing pooled workspace connection.",
                class: CommandClass::Sensitive,
                examples: vec![vec!["set -e\nhostname\nuptime"]],
            },
            CommandProfileInfo {
                name: "shell.powershell",
                description: "Run a PowerShell script through the existing pooled workspace connection.",
                class: CommandClass::Sensitive,
                examples: vec![vec!["$ErrorActionPreference = 'Stop'\nGet-ComputerInfo"]],
            },
        ]
    }

    /// Resolves a built-in command profile and agent-supplied structured arguments.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown profile names, invalid arguments, or failed command validation.
    pub fn resolve_builtin(
        name: &str,
        args: Vec<String>,
        policy: &ServerProtectionPolicy,
    ) -> Result<CommandProfile, CommandProfileResolutionError> {
        if matches!(name, "shell.posix" | "shell.powershell") {
            return resolve_shell_builtin(name, args, policy);
        }
        let profile = match name {
            "host.identity" => {
                ensure_no_args(name, &args)?;
                profile(name, "hostname", args, policy)
            }
            "host.uname" => {
                let args = if args.is_empty() {
                    vec!["-a".to_owned()]
                } else {
                    ensure_allowed_values(name, &args, &["-a", "-s", "-r", "-m", "-n"])?;
                    args
                };
                profile(name, "uname", args, policy)
            }
            "host.uptime" => {
                ensure_no_args(name, &args)?;
                profile(name, "uptime", args, policy)
            }
            "host.whoami" => {
                ensure_no_args(name, &args)?;
                profile(name, "whoami", args, policy)
            }
            "disk.usage" => {
                ensure_max_args(name, &args, 4)?;
                for arg in &args {
                    if !is_safe_df_arg(arg) {
                        return Err(CommandProfileResolutionError::ArgumentNotAllowed {
                            profile: name.to_owned(),
                            argument: arg.clone(),
                        });
                    }
                }
                profile(name, "df", args, policy)
            }
            "memory.free" => {
                let args = if args.is_empty() {
                    vec!["-h".to_owned()]
                } else {
                    ensure_allowed_values(name, &args, &["-h", "-m", "-g"])?;
                    args
                };
                profile(name, "free", args, policy)
            }
            "process.snapshot" => {
                ensure_no_args(name, &args)?;
                profile(name, "ps", vec!["aux".to_owned()], policy)
            }
            "gpu.nvidia_smi" => {
                let args = if args.is_empty() {
                    vec!["-L".to_owned()]
                } else {
                    ensure_max_args(name, &args, 8)?;
                    for arg in &args {
                        if !is_safe_nvidia_smi_arg(arg) {
                            return Err(CommandProfileResolutionError::ArgumentNotAllowed {
                                profile: name.to_owned(),
                                argument: arg.clone(),
                            });
                        }
                    }
                    args
                };
                profile(name, "nvidia-smi", args, policy)
            }
            "service.status" => {
                ensure_max_args(name, &args, 1)?;
                let Some(service) = args.first() else {
                    return Err(CommandProfileResolutionError::ArgumentNotAllowed {
                        profile: name.to_owned(),
                        argument: "<missing-service>".to_owned(),
                    });
                };
                if !is_safe_systemd_unit(service) {
                    return Err(CommandProfileResolutionError::ArgumentNotAllowed {
                        profile: name.to_owned(),
                        argument: service.clone(),
                    });
                }
                profile(
                    name,
                    "systemctl",
                    vec!["status".to_owned(), service.clone()],
                    policy,
                )
            }
            other => {
                return Err(CommandProfileResolutionError::UnknownProfile(
                    other.to_owned(),
                ));
            }
        };

        profile.validate()?;
        Ok(profile)
    }
}

fn resolve_shell_builtin(
    name: &str,
    args: Vec<String>,
    policy: &ServerProtectionPolicy,
) -> Result<CommandProfile, CommandProfileResolutionError> {
    let script = one_shell_script(name, args)?;
    let profile = match name {
        "shell.posix" => shell_profile(name, "sh", vec!["-lc".to_owned(), script], policy),
        "shell.powershell" => shell_profile(
            name,
            "powershell.exe",
            vec![
                "-NoLogo".to_owned(),
                "-NoProfile".to_owned(),
                "-NonInteractive".to_owned(),
                "-Command".to_owned(),
                script,
            ],
            policy,
        ),
        other => {
            return Err(CommandProfileResolutionError::UnknownProfile(
                other.to_owned(),
            ));
        }
    };
    profile.validate()?;
    Ok(profile)
}

impl CommandProfile {
    /// Validates a profile against the built-in production safety baseline.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is empty, not allowlisted for its class, contains shell
    /// metacharacters in arguments, or exceeds timeout/output limits.
    pub fn validate(&self) -> Result<(), CommandValidationError> {
        const MAX_TIMEOUT_SECONDS: u64 = 900;
        const MAX_OUTPUT_LIMIT_BYTES: usize = 8 * 1024 * 1024;

        if self.program.trim().is_empty() {
            return Err(CommandValidationError::EmptyProgram);
        }

        if !allowed_programs(&self.class).contains(self.program.as_str()) {
            return Err(CommandValidationError::ProgramNotAllowed(
                self.program.clone(),
            ));
        }

        if matches!(self.class, CommandClass::Sensitive) {
            validate_shell_profile(self)?;
        } else {
            for arg in &self.args {
                if contains_shell_metacharacter(arg) {
                    return Err(CommandValidationError::ShellMetacharacter(arg.clone()));
                }
            }
        }

        let max_timeout_seconds = if matches!(self.class, CommandClass::Sensitive) {
            7_200
        } else {
            MAX_TIMEOUT_SECONDS
        };
        if self.timeout_seconds == 0 || self.timeout_seconds > max_timeout_seconds {
            return Err(CommandValidationError::InvalidTimeout {
                max: max_timeout_seconds,
                actual: self.timeout_seconds,
            });
        }

        if self.output_limit_bytes == 0 || self.output_limit_bytes > MAX_OUTPUT_LIMIT_BYTES {
            return Err(CommandValidationError::InvalidOutputLimit {
                max: MAX_OUTPUT_LIMIT_BYTES,
                actual: self.output_limit_bytes,
            });
        }

        Ok(())
    }
}

fn allowed_programs(class: &CommandClass) -> BTreeSet<&'static str> {
    match class {
        CommandClass::ReadOnly => [
            "cat",
            "df",
            "docker",
            "free",
            "head",
            "hostname",
            "ip",
            "journalctl",
            "ls",
            "nvidia-smi",
            "ps",
            "pwd",
            "sed",
            "systemctl",
            "tail",
            "uname",
            "uptime",
            "whoami",
        ]
        .into_iter()
        .collect(),
        CommandClass::Build => ["cargo", "go", "node", "npm", "pnpm", "python", "uv", "yarn"]
            .into_iter()
            .collect(),
        CommandClass::Sensitive => ["powershell.exe", "sh"].into_iter().collect(),
    }
}

fn contains_shell_metacharacter(value: &str) -> bool {
    value
        .chars()
        .any(|ch| matches!(ch, ';' | '&' | '|' | '`' | '$' | '<' | '>' | '\n' | '\r'))
}

fn profile(
    name: &str,
    program: &str,
    args: Vec<String>,
    policy: &ServerProtectionPolicy,
) -> CommandProfile {
    CommandProfile {
        name: name.to_owned(),
        program: program.to_owned(),
        args,
        class: CommandClass::ReadOnly,
        timeout_seconds: policy.default_exec_timeout_seconds,
        output_limit_bytes: policy.default_output_limit_bytes,
        requires_tty: false,
    }
}

fn shell_profile(
    name: &str,
    program: &str,
    args: Vec<String>,
    policy: &ServerProtectionPolicy,
) -> CommandProfile {
    CommandProfile {
        name: name.to_owned(),
        program: program.to_owned(),
        args,
        class: CommandClass::Sensitive,
        timeout_seconds: policy.default_exec_timeout_seconds.max(900),
        output_limit_bytes: policy.default_output_limit_bytes.max(1024 * 1024),
        requires_tty: false,
    }
}

fn one_shell_script(
    name: &str,
    args: Vec<String>,
) -> Result<String, CommandProfileResolutionError> {
    ensure_max_args(name, &args, 1)?;
    let Some(script) = args.into_iter().next() else {
        return Err(CommandProfileResolutionError::ArgumentNotAllowed {
            profile: name.to_owned(),
            argument: "<missing-script>".to_owned(),
        });
    };
    if script.trim().is_empty() || script.contains('\0') || script.len() > 64 * 1024 {
        return Err(CommandProfileResolutionError::ArgumentNotAllowed {
            profile: name.to_owned(),
            argument: "<invalid-script>".to_owned(),
        });
    }
    Ok(script)
}

fn validate_shell_profile(profile: &CommandProfile) -> Result<(), CommandValidationError> {
    let script = match profile.name.as_str() {
        "shell.posix"
            if profile.program == "sh"
                && profile.args.len() == 2
                && profile.args.first().is_some_and(|arg| arg == "-lc") =>
        {
            profile.args.get(1)
        }
        "shell.powershell"
            if profile.program == "powershell.exe"
                && profile.args.len() == 5
                && profile.args[..4]
                    == ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command"] =>
        {
            profile.args.get(4)
        }
        _ => {
            return Err(CommandValidationError::InvalidShellProfile(
                "expected shell.posix or shell.powershell with fixed launcher arguments".to_owned(),
            ));
        }
    };
    if script.is_none_or(|script| {
        script.trim().is_empty() || script.contains('\0') || script.len() > 64 * 1024
    }) {
        return Err(CommandValidationError::InvalidShellProfile(
            "script must be non-empty, contain no NUL, and be at most 65536 bytes".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_no_args(name: &str, args: &[String]) -> Result<(), CommandProfileResolutionError> {
    ensure_max_args(name, args, 0)
}

fn ensure_max_args(
    name: &str,
    args: &[String],
    max: usize,
) -> Result<(), CommandProfileResolutionError> {
    if args.len() > max {
        return Err(CommandProfileResolutionError::TooManyArguments {
            profile: name.to_owned(),
            max,
            actual: args.len(),
        });
    }
    Ok(())
}

fn ensure_allowed_values(
    name: &str,
    args: &[String],
    allowed: &[&str],
) -> Result<(), CommandProfileResolutionError> {
    for arg in args {
        if !allowed.contains(&arg.as_str()) {
            return Err(CommandProfileResolutionError::ArgumentNotAllowed {
                profile: name.to_owned(),
                argument: arg.clone(),
            });
        }
    }
    Ok(())
}

fn is_safe_df_arg(arg: &str) -> bool {
    matches!(arg, "-h" | "-T" | "-P")
        || (arg.starts_with('/') && is_path_like(arg))
        || (arg == "." || arg.starts_with("./")) && is_path_like(arg)
}

fn is_safe_nvidia_smi_arg(arg: &str) -> bool {
    arg == "-L"
        || arg == "-q"
        || arg == "-i"
        || arg.starts_with("--query-gpu=")
        || arg.starts_with("--format=")
        || arg.starts_with("--id=")
        || arg.chars().all(|ch| ch.is_ascii_digit() || ch == ',')
}

fn is_safe_systemd_unit(arg: &str) -> bool {
    arg.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '@'))
        && !arg.starts_with('-')
        && arg.len() <= 128
}

fn is_path_like(arg: &str) -> bool {
    arg.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | '+'))
        && !contains_shell_metacharacter(arg)
        && !arg.contains("..")
        && !arg.contains('=')
}

#[cfg(test)]
mod tests {
    use super::{
        CommandClass, CommandProfile, CommandProfileCatalog, CommandProfileResolutionError,
        CommandValidationError,
    };
    use crate::ServerProtectionPolicy;

    fn readonly_profile(program: &str, args: Vec<String>) -> CommandProfile {
        CommandProfile {
            name: "test".to_owned(),
            program: program.to_owned(),
            args,
            class: CommandClass::ReadOnly,
            timeout_seconds: 30,
            output_limit_bytes: 1024,
            requires_tty: false,
        }
    }

    #[test]
    fn accepts_structured_readonly_command() {
        let profile = readonly_profile("nvidia-smi", vec!["--query-gpu=name".to_owned()]);
        assert_eq!(profile.validate(), Ok(()));
    }

    #[test]
    fn denies_unlisted_program() {
        let profile = readonly_profile("rm", vec!["-rf".to_owned(), "/".to_owned()]);
        assert_eq!(
            profile.validate(),
            Err(CommandValidationError::ProgramNotAllowed("rm".to_owned()))
        );
    }

    #[test]
    fn denies_shell_metacharacters_inside_arguments() {
        let profile = readonly_profile("cat", vec!["/tmp/a; rm -rf /".to_owned()]);
        assert_eq!(
            profile.validate(),
            Err(CommandValidationError::ShellMetacharacter(
                "/tmp/a; rm -rf /".to_owned()
            ))
        );
    }

    #[test]
    fn resolves_builtin_profile_with_defaults() -> Result<(), CommandProfileResolutionError> {
        let profile = CommandProfileCatalog::resolve_builtin(
            "host.uname",
            Vec::new(),
            &ServerProtectionPolicy::default(),
        )?;

        assert_eq!(profile.program, "uname");
        assert_eq!(profile.args, vec!["-a"]);
        Ok(())
    }

    #[test]
    fn resolves_posix_shell_script_for_real_workspace_operations()
    -> Result<(), CommandProfileResolutionError> {
        let script = "set -euo pipefail\nkubectl get pods | head -n 20";
        let profile = CommandProfileCatalog::resolve_builtin(
            "shell.posix",
            vec![script.to_owned()],
            &ServerProtectionPolicy::default(),
        )?;

        assert_eq!(profile.program, "sh");
        assert_eq!(profile.args, vec!["-lc", script]);
        assert_eq!(profile.class, CommandClass::Sensitive);
        assert_eq!(profile.timeout_seconds, 900);
        profile.validate()?;
        Ok(())
    }

    #[test]
    fn denies_shell_like_builtin_arguments() {
        let error = CommandProfileCatalog::resolve_builtin(
            "disk.usage",
            vec!["/tmp; cat /etc/shadow".to_owned()],
            &ServerProtectionPolicy::default(),
        );

        assert!(matches!(
            error,
            Err(CommandProfileResolutionError::ArgumentNotAllowed { .. })
        ));
    }
}
