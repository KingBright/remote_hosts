//! Fixed read-only system queries. No install, restart, reboot, policy or DNS mutation argv.
use crate::{
    executor,
    ho5::{Backend, Deployment, Observation, PACKAGES, Transaction},
    protocol::{digest, valid_id},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Query {
    BootId,
    Security,
    OstreeStatus,
    OwnerUid,
    CanReboot,
    PackagesAuthorization,
    RebootAuthorization,
    VerifiedPackages,
}
#[allow(async_fn_in_trait)]
pub trait ReadBus {
    async fn query(&mut self, query: Query) -> Result<Value>;
}
pub struct ReadOnlyBus<T>(pub T);
fn data(v: &Value) -> Result<&Value> {
    v["data"].get(0).context("invalid_dbus_reply")
}
fn deployment(v: &Value) -> Result<Deployment> {
    Ok(Deployment {
        checksum: v["checksum"]
            .as_str()
            .context("missing_deployment_checksum")?
            .into(),
        packages: v["packages"]
            .as_array()
            .context("missing_packages")?
            .iter()
            .map(|v| v.as_str().map(String::from).context("invalid_package_name"))
            .collect::<Result<_>>()?,
    })
}
impl<T: ReadBus> Backend for ReadOnlyBus<T> {
    async fn inspect(&mut self) -> Result<Observation> {
        let boot_id = self
            .0
            .query(Query::BootId)
            .await?
            .as_str()
            .context("invalid_boot_id")?
            .to_owned();
        valid_id(&boot_id)?;
        let no_new_privs = self
            .0
            .query(Query::Security)
            .await?
            .as_bool()
            .context("invalid_nnp")?;
        let status = self.0.query(Query::OstreeStatus).await?;
        let ds = status["deployments"]
            .as_array()
            .context("missing_deployments")?;
        ensure!(
            ds.iter().filter(|d| d["booted"] == true).count() == 1
                && ds.iter().filter(|d| d["staged"] == true).count() <= 1,
            "ambiguous_deployments"
        );
        let booted = deployment(
            ds.iter()
                .find(|d| d["booted"] == true)
                .context("booted_missing")?,
        )?;
        let staged = ds
            .iter()
            .find(|d| d["staged"] == true)
            .map(deployment)
            .transpose()?;
        let transaction = if status["transaction"].is_null()
            || status["transaction"]
                .as_array()
                .is_some_and(|a| a.is_empty())
        {
            None
        } else {
            let t = status["transaction"]
                .as_array()
                .context("invalid_transaction")?;
            ensure!(
                t.len() == 3 && t.iter().all(Value::is_string),
                "invalid_transaction_tuple"
            );
            Some(Transaction {
                owner: t[1].as_str().context("invalid_transaction_owner")?.into(),
                identity_sha256: digest(t)?,
            })
        };
        let owner_reply = self.0.query(Query::OwnerUid).await?;
        let service_uid = u32::try_from(
            data(&owner_reply)?
                .as_u64()
                .context("invalid_service_uid")?,
        )?;
        let can_reply = self.0.query(Query::CanReboot).await?;
        let can = data(&can_reply)?
            .as_str()
            .context("invalid_reboot_capability")?;
        ensure!(
            matches!(
                can,
                "yes"
                    | "no"
                    | "na"
                    | "challenge"
                    | "inhibited"
                    | "inhibitor-blocked"
                    | "challenge-inhibitor-blocked"
            ),
            "invalid_reboot_capability"
        );
        let packages_authorized = self
            .0
            .query(Query::PackagesAuthorization)
            .await?
            .as_bool()
            .context("invalid_packages_authority")?;
        let reboot_authorized = can == "yes"
            && self
                .0
                .query(Query::RebootAuthorization)
                .await?
                .as_bool()
                .context("invalid_reboot_authority")?;
        let verified = self.0.query(Query::VerifiedPackages).await?;
        let verified_booted_packages = verified
            .as_array()
            .context("invalid_verified_packages")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(String::from)
                    .context("invalid_verified_package")
            })
            .collect::<Result<_>>()?;
        let o = Observation {
            boot_id,
            booted,
            staged,
            transaction,
            completion: None,
            packages_authorized,
            reboot_authorized,
            reboot_inhibited: can.contains("inhibit"),
            service_uid,
            no_new_privs,
            verified_booted_packages,
        };
        o.validate()?;
        Ok(o)
    }
}
/// Host Linux only. Errors never include raw replies, argv/env, configs or credentials.
pub struct SystemReadBus;
fn process_identity() -> Result<String> {
    let raw = std::fs::read_to_string("/proc/self/stat").context("process_metadata_unavailable")?;
    let rest = raw.rsplit_once(')').context("invalid_process_metadata")?.1;
    let started = rest
        .split_whitespace()
        .nth(19)
        .context("invalid_process_start")?;
    ensure!(
        started.bytes().all(|b| b.is_ascii_digit()),
        "invalid_process_start"
    );
    Ok(format!(
        "{},{},{}",
        std::process::id(),
        started,
        nix::unistd::geteuid().as_raw()
    ))
}
pub fn fixed_argv(q: Query, process: &str) -> Result<(&'static str, Vec<String>)> {
    let words: Vec<&str> = match q {
        Query::OstreeStatus => vec!["status", "--json"],
        Query::OwnerUid => vec![
            "--system",
            "--json=short",
            "call",
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            "org.freedesktop.DBus",
            "GetConnectionUnixUser",
            "s",
            "org.projectatomic.rpmostree1",
        ],
        Query::CanReboot => vec![
            "--system",
            "--json=short",
            "call",
            "org.freedesktop.login1",
            "/org/freedesktop/login1",
            "org.freedesktop.login1.Manager",
            "CanReboot",
        ],
        Query::PackagesAuthorization | Query::RebootAuthorization => {
            ensure!(
                !process.is_empty()
                    && process.split(',').count() == 3
                    && process.bytes().all(|b| b.is_ascii_digit() || b == b','),
                "invalid_process_identity"
            );
            vec![
                "--action-id",
                if q == Query::PackagesAuthorization {
                    "org.projectatomic.rpmostree1.install-uninstall-packages"
                } else {
                    "org.freedesktop.login1.reboot"
                },
                "--process",
                process,
            ]
        }
        _ => anyhow::bail!("query_has_no_command"),
    };
    let program = match q {
        Query::OstreeStatus => "/usr/bin/rpm-ostree",
        Query::OwnerUid | Query::CanReboot => "/usr/bin/busctl",
        _ => "/usr/bin/pkcheck",
    };
    Ok((program, words.into_iter().map(String::from).collect()))
}
impl ReadBus for SystemReadBus {
    async fn query(&mut self, query: Query) -> Result<Value> {
        ensure!(
            cfg!(target_os = "linux"),
            "ho5_system_queries_require_linux"
        );
        match query {
            Query::BootId => Ok(json!(
                std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
                    .context("boot_metadata_unavailable")?
                    .trim()
            )),
            Query::Security => {
                let status = std::fs::read_to_string("/proc/self/status")
                    .context("process_metadata_unavailable")?;
                let value = status
                    .lines()
                    .find_map(|l| l.strip_prefix("NoNewPrivs:"))
                    .context("nnp_metadata_missing")?;
                Ok(json!(value.trim() == "1"))
            }
            Query::VerifiedPackages => {
                let mut packages = vec![];
                for p in PACKAGES {
                    let (ok, _) =
                        executor::output(executor::command("/usr/bin/rpm", &["-q", "--", p]), 4096)
                            .await
                            .map_err(|_| anyhow::anyhow!("package_query_failed"))?;
                    if ok {
                        packages.push(p);
                    }
                }
                Ok(json!(packages))
            }
            _ => {
                let process = if matches!(
                    query,
                    Query::PackagesAuthorization | Query::RebootAuthorization
                ) {
                    process_identity()?
                } else {
                    String::new()
                };
                let (program, args) = fixed_argv(query, &process)?;
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                let (ok, text) =
                    executor::output(executor::command(program, &args), 2 * 1024 * 1024)
                        .await
                        .map_err(|_| anyhow::anyhow!("ho5_system_query_failed"))?;
                if matches!(
                    query,
                    Query::PackagesAuthorization | Query::RebootAuthorization
                ) {
                    return Ok(json!(ok));
                }
                ensure!(ok, "ho5_system_query_rejected");
                serde_json::from_str(&text).map_err(|_| anyhow::anyhow!("invalid_ho5_system_reply"))
            }
        }
    }
}
