//! Fixed macOS/Linux operations. Real execution is reachable only behind root-owned policy.
use crate::{
    engine::{Backend, unix_time},
    filesystem::{Store, inspect_file, quarantine, trusted_dir},
    protocol::*,
};
use anyhow::{Context, Result, ensure};
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};

pub struct SystemBackend;
// No inherited PATH, loader, shell, proxy or systemd pager environment.
fn command(program: &str, args: &[&str]) -> Command {
    let mut c = Command::new(program);
    c.args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
        .env("LC_ALL", "C")
        .env("SYSTEMD_PAGER", "")
        .env("SYSTEMD_COLORS", "0")
        .current_dir("/")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    c
}
async fn output(mut cmd: Command, limit: u64) -> Result<(bool, String)> {
    cmd.stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child = cmd.spawn().context("system_command_spawn_failed")?;
    let stdout = child.stdout.take().context("missing_command_stdout")?;
    let read = async {
        let mut data = Vec::new();
        stdout.take(limit + 1).read_to_end(&mut data).await?;
        ensure!(data.len() as u64 <= limit, "system_command_output_limit");
        Ok::<_, anyhow::Error>(data)
    };
    let (data, status) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::try_join!(read, async { Ok::<_, anyhow::Error>(child.wait().await?) })
    })
    .await
    .context("system_query_timeout")??;
    Ok((status.success(), String::from_utf8(data)?))
}
async fn mutate(program: &str, args: &[&str]) -> Result<()> {
    let mut cmd = command(program, args);
    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    let mut child = cmd.spawn()?;
    let status = tokio::time::timeout(Duration::from_secs(100), child.wait())
        .await
        .context("system_action_timeout_outcome_requires_inspection")??;
    ensure!(
        status.success(),
        "system_action_failed: {program} exit={:?}",
        status.code()
    );
    Ok(())
}
fn progress(store: &Store, record: &mut Record, step: String) -> Result<()> {
    record.steps.push(step);
    record.updated_at = unix_time();
    store.save(record)
}
/// ps -o comm is used, never args; full paths containing spaces remain intact.
pub fn parse_processes(text: &str) -> Result<Vec<ProcessState>> {
    let mut result = vec![];
    for line in text.lines().filter(|s| !s.trim().is_empty()) {
        let mut rest = line.trim_start();
        let mut fields = vec![];
        for _ in 0..7 {
            let end = rest
                .find(char::is_whitespace)
                .context("malformed_ps_output")?;
            fields.push(&rest[..end]);
            rest = rest[end..].trim_start();
        }
        let pid = fields[0].parse::<i32>()?;
        ensure!(pid > 0, "invalid_ps_pid");
        result.push(ProcessState {
            pid,
            uid: fields[1].parse()?,
            started: fields[2..7].join(" "),
            executable: rest.to_string(),
        });
    }
    Ok(result)
}
async fn processes() -> Result<Vec<ProcessState>> {
    let (ok, text) = output(
        command("/bin/ps", &["-axo", "pid=,uid=,lstart=,comm="]),
        2 * 1024 * 1024,
    )
    .await?;
    ensure!(ok, "process_inventory_failed");
    parse_processes(&text)
}
fn unit_from_text(name: &str, text: &str) -> Result<UnitState> {
    let props: std::collections::BTreeMap<_, _> =
        text.lines().filter_map(|l| l.split_once('=')).collect();
    let get = |k| {
        props
            .get(k)
            .map(|s| (*s).to_owned())
            .ok_or_else(|| anyhow::anyhow!("missing_systemd_property:{k}"))
    };
    let u = UnitState {
        name: name.into(),
        load: get("LoadState")?,
        active: get("ActiveState")?,
        enabled: get("UnitFileState")?,
        fragment: get("FragmentPath")?,
        drop_ins: get("DropInPaths")?,
    };
    ensure!(
        u.load == "not-found" || u.fragment == format!("/etc/systemd/system/{name}"),
        "unexpected_legacy_service_fragment"
    );
    ensure!(
        u.drop_ins.is_empty(),
        "legacy_service_has_unreviewed_dropins"
    );
    Ok(u)
}
async fn unit(name: &str) -> Result<UnitState> {
    let (_, text) = output(
        command(
            "/usr/bin/systemctl",
            &[
                "show",
                "--property=LoadState,ActiveState,UnitFileState,FragmentPath,DropInPaths",
                "--",
                name,
            ],
        ),
        16384,
    )
    .await?;
    unit_from_text(name, &text)
}
async fn current_linux(policy: &Policy) -> Result<bool> {
    let mut c = command(
        "/usr/bin/systemctl",
        &["--user", "is-active", "--", CURRENT_UNIT],
    );
    c.uid(policy.allowed_uid)
        .gid(policy.allowed_gid)
        .env(
            "XDG_RUNTIME_DIR",
            format!("/run/user/{}", policy.allowed_uid),
        )
        .env(
            "DBUS_SESSION_BUS_ADDRESS",
            format!("unix:path=/run/user/{}/bus", policy.allowed_uid),
        );
    let (ok, text) = output(c, 1024).await?;
    Ok(ok && text.trim() == "active")
}
impl Backend for SystemBackend {
    async fn inspect(&self, policy: &Policy) -> Result<Snapshot> {
        ensure!(
            policy.platform == std::env::consts::OS,
            "policy_platform_mismatch"
        );
        let mut s = Snapshot::default();
        for path in policy.targets() {
            s.files.push(inspect_file(&path, policy.allowed_uid)?);
        }
        if policy.platform == "linux" {
            trusted_dir(std::path::Path::new("/etc/systemd/system"), 0)?;
            ensure!(
                s.files
                    .iter()
                    .flatten()
                    .all(|f| f.uid == 0 && f.mode & 0o022 == 0),
                "legacy_unit_not_root_controlled"
            );
            for name in LINUX_UNITS {
                s.units.push(unit(name).await?);
            }
            s.current_service_active = current_linux(policy).await?;
        } else {
            let targets = policy.targets();
            for p in processes().await? {
                if p.executable == policy.current_mac_binary() && p.uid == policy.allowed_uid {
                    s.current_service_active = true;
                }
                if p.executable.starts_with("/Applications/UURemote.app/") {
                    s.uu_processes.push(p.clone());
                }
                if targets.contains(&p.executable) {
                    ensure!(
                        p.uid == 0 || p.uid == policy.allowed_uid,
                        "legacy_process_owned_by_another_user"
                    );
                    s.processes.push(p);
                }
            }
        }
        Ok(s)
    }
    async fn execute(
        &self,
        policy: &Policy,
        store: &Store,
        record: &mut Record,
    ) -> Result<Snapshot> {
        if policy.platform == "linux" {
            for name in LINUX_UNITS {
                let u = unit(name).await?;
                if u.load != "not-found" {
                    progress(store, record, format!("starting_stop_disable:{name}"))?;
                    mutate("/usr/bin/systemctl", &["disable", "--now", "--", name]).await?;
                    let stopped = unit(name).await?;
                    ensure!(
                        matches!(stopped.active.as_str(), "inactive" | "failed"),
                        "legacy_service_did_not_stop"
                    );
                    progress(store, record, format!("stopped_disabled:{name}"))?;
                }
            }
        } else {
            let targets = policy.targets();
            // Old parent first, then its embedded EasyTier and the separate VPN copy.
            for target in targets {
                let selected: Vec<_> = record
                    .plan
                    .snapshot
                    .processes
                    .iter()
                    .filter(|p| p.executable == target)
                    .cloned()
                    .collect();
                for expected in selected {
                    let fresh = processes().await?;
                    let Some(actual) = fresh.iter().find(|p| p.pid == expected.pid) else {
                        continue;
                    };
                    ensure!(
                        actual == &expected,
                        "process_identity_changed_do_not_signal"
                    );
                    progress(
                        store,
                        record,
                        format!("starting_sigterm:{}:{}", expected.pid, expected.executable),
                    )?;
                    match nix::sys::signal::kill(
                        nix::unistd::Pid::from_raw(expected.pid),
                        nix::sys::signal::Signal::SIGTERM,
                    ) {
                        Ok(()) | Err(nix::errno::Errno::ESRCH) => (),
                        Err(e) => return Err(e.into()),
                    }
                    let mut gone = false;
                    for _ in 0..20 {
                        let fresh = processes().await?;
                        if !fresh.iter().any(|p| p == &expected) {
                            gone = true;
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    ensure!(gone, "legacy_process_did_not_exit_no_force_kill_attempted");
                    progress(store, record, format!("process_stopped:{}", expected.pid))?;
                }
            }
            ensure!(
                self.inspect(policy).await?.processes.is_empty(),
                "legacy_process_respawned_before_quarantine"
            );
        }
        let files = record.plan.snapshot.files.clone();
        for (index, file) in files.iter().enumerate() {
            if let Some(file) = file {
                progress(store, record, format!("starting_quarantine:{}", file.path))?;
                let backup = store
                    .dir
                    .join(format!("{}-{index}.bak", record.plan.request_id));
                quarantine(file, policy.allowed_uid, &backup)?;
                progress(store, record, format!("quarantined:{}", file.path))?;
            }
        }
        if policy.platform == "linux" {
            mutate("/usr/bin/systemctl", &["daemon-reload"]).await?;
        }
        let after = self.inspect(policy).await?;
        ensure!(
            after.current_service_active,
            "protected_current_remoteplay_not_active_after_cleanup"
        );
        ensure!(
            after.files.iter().all(Option::is_none) && after.processes.is_empty(),
            "legacy_targets_remain"
        );
        ensure!(
            after
                .units
                .iter()
                .all(|u| matches!(u.active.as_str(), "inactive" | "failed")
                    && !matches!(
                        u.enabled.as_str(),
                        "enabled" | "enabled-runtime" | "linked" | "linked-runtime"
                    )),
            "legacy_services_remain_active_or_enabled"
        );
        ensure!(
            record.plan.snapshot.uu_processes.is_empty() || !after.uu_processes.is_empty(),
            "uu_processes_disappeared_requires_inspection"
        );
        progress(
            store,
            record,
            "targeted_cleanup_verified_not_full_disk_audit".into(),
        )?;
        Ok(after)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ps_paths_with_spaces_are_exact() {
        let p=parse_processes("11493 0 Wed May 20 22:28:48 2026 /Users/test/RemotePlay Unified.app/Contents/MacOS/remote_play\n").unwrap();
        assert_eq!(
            p[0].executable,
            "/Users/test/RemotePlay Unified.app/Contents/MacOS/remote_play"
        );
        assert_eq!(p[0].started, "Wed May 20 22:28:48 2026");
    }
    #[test]
    fn ps_malformed_rejected() {
        assert!(parse_processes("1 root ???").is_err());
    }
    #[test]
    fn systemd_foreign_fragment_rejected() {
        assert!(unit_from_text("remote-play.service","LoadState=loaded\nActiveState=active\nUnitFileState=enabled\nFragmentPath=/usr/lib/systemd/system/remote-play.service\nDropInPaths=\n").is_err());
    }
    #[test]
    fn systemd_dropins_rejected() {
        assert!(unit_from_text("remote-play.service","LoadState=loaded\nActiveState=active\nUnitFileState=enabled\nFragmentPath=/etc/systemd/system/remote-play.service\nDropInPaths=/etc/systemd/system/remote-play.service.d/override.conf\n").is_err());
    }
    #[test]
    fn systemd_missing_properties_rejected() {
        assert!(unit_from_text("remote-play.service", "ActiveState=inactive").is_err());
    }
}
