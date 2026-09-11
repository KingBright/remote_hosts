//! Manifested binary-safe one-way synchronization. No deletion and no archive extraction.
//! RHSYNC1 format: magic, LE u32 JSON length, manifest header, consecutive changed bytes.
use crate::{
    files::{self, Workspace},
    hash, write_private,
};
use anyhow::{Context, Result, ensure};
use cap_std::fs::{Dir, OpenOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    path::{Component, Path},
};
const MAX: usize = 64 * 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    path: String,
    sha256: String,
    size: usize,
    #[serde(default)]
    executable: bool,
    #[serde(default)]
    expected_version: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    manifest_id: String,
    entries: Vec<Entry>,
}
fn guarded(dir: &Dir, path: &str) -> Result<()> {
    files::relative(path)?;
    ensure!(path.len() <= 512, "sync path too long");
    ensure!(
        Path::new(path)
            .components()
            .collect::<std::path::PathBuf>()
            .to_str()
            == Some(path),
        "sync path must use one normalized spelling"
    );
    let mut prefix = std::path::PathBuf::new();
    for p in Path::new(path).components() {
        ensure!(
            matches!(p, Component::Normal(_)),
            "sync path must be normalized"
        );
        prefix.push(p);
        match dir.symlink_metadata(&prefix) {
            Ok(meta) => ensure!(!meta.is_symlink(), "sync refuses symlink components"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
fn current(dir: &Dir, path: &str) -> Result<(String, bool)> {
    guarded(dir, path)?;
    match dir.symlink_metadata(path) {
        Ok(meta) => ensure!(meta.is_file(), "sync refuses special files before opening"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(("absent".into(), false)),
        Err(e) => return Err(e.into()),
    }
    let file = match dir.open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(("absent".into(), false)),
        Err(e) => return Err(e.into()),
    };
    let meta = file.metadata()?;
    ensure!(
        meta.is_file() && meta.len() <= MAX as u64,
        "sync only supports bounded regular files"
    );
    #[cfg(unix)]
    let executable = {
        use cap_std::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    };
    #[cfg(not(unix))]
    let executable = false;
    let mut data = Vec::new();
    file.take((MAX + 1) as u64).read_to_end(&mut data)?;
    ensure!(data.len() <= MAX, "sync source grew beyond limit");
    Ok((hash(data), executable))
}
fn entries(args: &Value) -> Result<Vec<Entry>> {
    let e: Vec<Entry> = serde_json::from_value(args["files"].clone())?;
    ensure!(
        !e.is_empty() && e.len() <= 256,
        "sync requires 1..256 files"
    );
    ensure!(
        serde_json::to_vec(&e)?.len() <= 128 * 1024,
        "sync manifest too large"
    );
    let mut paths = HashSet::new();
    let mut total = 0usize;
    for x in &e {
        files::relative(&x.path)?;
        ensure!(
            x.path.len() <= 512
                && Path::new(&x.path)
                    .components()
                    .collect::<std::path::PathBuf>()
                    .to_str()
                    == Some(x.path.as_str()),
            "invalid normalized sync path"
        );
        ensure!(
            paths.insert(x.path.clone())
                && crate::transfers::valid_hash(&x.sha256)
                && x.size <= MAX,
            "invalid sync entry"
        );
        total = total.checked_add(x.size).context("sync size overflow")?;
    }
    ensure!(total <= 512 * 1024 * 1024, "sync manifest total limit");
    for a in &e {
        for b in &e {
            ensure!(
                a.path == b.path || !Path::new(&a.path).starts_with(Path::new(&b.path)),
                "sync file/parent conflict"
            );
        }
    }
    Ok(e)
}
fn output_entries(dir: &Dir, e: &[Entry]) -> Result<(Vec<Entry>, Vec<Value>)> {
    let mut bound = Vec::new();
    let mut result = Vec::new();
    for x in e {
        let (version, exec) = current(dir, &x.path)?;
        let same = version == x.sha256 && exec == x.executable;
        let mut b = x.clone();
        b.expected_version = Some(version.clone());
        bound.push(b);
        result.push(json!({"path":x.path,"expected_version":version,"sha256":x.sha256,"size":x.size,"executable":x.executable,"action":if same{"unchanged"}else{"upload"}}));
    }
    Ok((bound, result))
}
fn publish(dir: &Dir, path: &str, data: &[u8], executable: bool, expected: &str) -> Result<()> {
    guarded(dir, path)?;
    let p = Path::new(path);
    let parent = p
        .parent()
        .filter(|s| !s.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    dir.create_dir_all(parent)?;
    guarded(dir, path)?;
    let sub = dir.open_dir(parent)?;
    let temp = format!(".remote-hosts-sync-{}.tmp", crate::random());
    let name = p.file_name().context("missing name")?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    let mut f = sub.open_with(&temp, &options)?;
    let outcome = (|| -> Result<()> {
        #[cfg(unix)]
        {
            use cap_std::fs::PermissionsExt;
            let previous = sub
                .metadata(name)
                .map(|m| m.permissions().mode())
                .unwrap_or(0o600);
            let permissions = if executable {
                previous | 0o100
            } else {
                previous & !0o111
            };
            f.set_permissions(cap_std::fs::Permissions::from_mode(permissions))?;
        }
        f.write_all(data)?;
        f.sync_all()?;
        ensure!(
            current(dir, path)?.0 == expected,
            "version_conflict_during_sync"
        );
        if expected == "absent" {
            sub.hard_link(&temp, &sub, name)?;
            sub.remove_file(&temp)?;
        } else {
            sub.rename(&temp, &sub, name)?;
        }
        Ok(())
    })();
    if outcome.is_err() {
        let _ = sub.remove_file(&temp);
    }
    outcome
}
pub(crate) fn run(ws: &Workspace, args: &Value, journal: &Path) -> Result<Value> {
    let requested = entries(args)?;
    let dir = files::directory(ws)?;
    if args["mode"] == "plan" {
        let (bound, views) = output_entries(&dir, &requested)?;
        let id = hash(serde_json::to_vec(
            &json!({"workspace":ws.id,"files":bound}),
        )?);
        let result = json!({"state":"planned","manifest_id":id,"files":bound,"actions":views,"deletes":false,"protocol":1});
        ensure!(
            serde_json::to_vec(&result)?.len() <= 240 * 1024,
            "sync plan response budget"
        );
        return Ok(result);
    }
    ensure!(args["mode"] == "apply", "sync mode must be plan/apply");
    let expected_id = hash(serde_json::to_vec(
        &json!({"workspace":ws.id,"files":requested}),
    )?);
    ensure!(
        args["manifest_id"] == expected_id,
        "sync manifest identity conflict"
    );
    ensure!(
        requested.iter().all(|e| e.expected_version.is_some()),
        "sync apply requires planned target versions"
    );
    let path = files::text(args, "bundle_path")?;
    guarded(&dir, path)?;
    ensure!(
        dir.symlink_metadata(path)?.is_file(),
        "bundle must be a regular file"
    );
    let mut blob = Vec::new();
    dir.open(path)?
        .take((MAX + 1) as u64)
        .read_to_end(&mut blob)?;
    ensure!(
        blob.len() <= MAX && blob.len() >= 12 && &blob[..8] == b"RHSYNC1\n",
        "invalid sync bundle"
    );
    ensure!(
        args["bundle_sha256"] == hash(&blob),
        "sync bundle hash mismatch"
    );
    let n = u32::from_le_bytes(blob[8..12].try_into().unwrap()) as usize;
    ensure!(
        n <= 128 * 1024 && 12 + n <= blob.len(),
        "invalid bundle header size"
    );
    let header: Header = serde_json::from_slice(&blob[12..12 + n])?;
    ensure!(
        header.manifest_id == expected_id,
        "bundle belongs to another manifest"
    );
    let lookup: HashMap<&str, &Entry> = requested.iter().map(|e| (e.path.as_str(), e)).collect();
    let mut blocks = HashMap::new();
    let mut offset = 12 + n;
    for item in &header.entries {
        let e = lookup
            .get(item.path.as_str())
            .context("unexpected bundle path")?;
        ensure!(
            item.sha256 == e.sha256 && item.size == e.size && item.executable == e.executable,
            "bundle metadata conflict"
        );
        let end = offset.checked_add(item.size).context("bundle overflow")?;
        ensure!(end <= blob.len(), "truncated bundle");
        ensure!(
            hash(&blob[offset..end]) == e.sha256,
            "bundle content hash mismatch"
        );
        ensure!(
            blocks
                .insert(item.path.as_str(), &blob[offset..end])
                .is_none(),
            "duplicate bundle file"
        );
        offset = end;
    }
    ensure!(offset == blob.len(), "trailing bundle data");
    // Whole-batch preflight before first publication. A resumed manifest accepts
    // already-satisfied content, never overwrites unrelated edits.
    for e in &requested {
        let (current, exec) = current(&dir, &e.path)?;
        ensure!(
            current == e.sha256 && exec == e.executable
                || Some(&current) == e.expected_version.as_ref(),
            "version_conflict: sync destination changed"
        );
        ensure!(
            current == e.sha256 && exec == e.executable || blocks.contains_key(e.path.as_str()),
            "missing changed payload"
        );
    }
    let mut state = json!({"schema_version":1,"workspace_id":ws.id,"manifest_id":expected_id,"state":"prepared","files":requested,"applied":[],"pending":requested.iter().map(|e|&e.path).collect::<Vec<_>>()});
    write_private(journal, &serde_json::to_vec(&state)?)?;
    let mut changed = 0usize;
    let mut reused = 0usize;
    let mut bytes_written = 0usize;
    for (i, e) in requested.iter().enumerate() {
        let attempt = (|| -> Result<()> {
            let (version, exec) = current(&dir, &e.path)?;
            if version == e.sha256 && exec == e.executable {
                reused += 1;
                return Ok(());
            }
            ensure!(
                Some(&version) == e.expected_version.as_ref(),
                "version_conflict_during_sync"
            );
            let bytes = blocks
                .get(e.path.as_str())
                .context("missing sync payload")?;
            publish(&dir, &e.path, bytes, e.executable, &version)?;
            changed += 1;
            bytes_written += bytes.len();
            Ok(())
        })();
        if let Err(e) = attempt {
            state["state"] = json!("partial");
            state["failed_index"] = json!(i);
            write_private(journal, &serde_json::to_vec(&state)?)?;
            return Ok(
                json!({"error":"partial_sync","error_code":"version_or_io_conflict","state":"partial","manifest_id":expected_id,"applied":state["applied"],"pending":state["pending"],"journal_id":journal.file_stem().map(|n|n.to_string_lossy()),"recovery_action":"recheck target versions; reapply the SAME manifest to satisfy only remaining unchanged preconditions","detail_category":if e.to_string().contains("version_conflict"){"version_conflict"}else{"io_error"}}),
            );
        }
        state["applied"]
            .as_array_mut()
            .unwrap()
            .push(json!({"path":e.path,"version":e.sha256}));
        state["pending"] = json!(
            requested[i + 1..]
                .iter()
                .map(|e| &e.path)
                .collect::<Vec<_>>()
        );
        state["state"] = json!(if i + 1 == requested.len() {
            "completed"
        } else {
            "applying"
        });
        write_private(journal, &serde_json::to_vec(&state)?)?;
    }
    Ok(
        json!({"state":"completed","manifest_id":expected_id,"changed_files":changed,"reused_files":reused,"bytes_written":bytes_written,"files":state["applied"],"journal_id":journal.file_stem().map(|s|s.to_string_lossy()),"atomicity":"per_file","deletes":false,"protocol":1}),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn batch_transfers_only_changed_files_and_replay_does_not_rewrite() {
        let d = tempfile::tempdir().unwrap();
        let ws = Workspace {
            id: "w".into(),
            device_id: "d".into(),
            root: d.path().canonicalize().unwrap(),
        };
        let state = tempfile::tempdir().unwrap();
        let mut items = Vec::new();
        for i in 0..100 {
            let path = format!("f{i}.bin");
            let data = if i < 3 { b"new" } else { b"old" };
            std::fs::write(d.path().join(&path), b"old").unwrap();
            items.push(json!({"path":path,"sha256":hash(data),"size":3}));
        }
        let plan = run(
            &ws,
            &json!({"mode":"plan","files":items}),
            &state.path().join("plan"),
        )
        .unwrap();
        let changed: Vec<_> = plan["files"].as_array().unwrap()[..3].to_vec();
        let header = json!({"manifest_id":plan["manifest_id"],"entries":changed})
            .to_string()
            .into_bytes();
        let mut bundle = b"RHSYNC1\n".to_vec();
        bundle.extend((header.len() as u32).to_le_bytes());
        bundle.extend(header);
        for _ in 0..3 {
            bundle.extend(b"new");
        }
        std::fs::write(d.path().join("bundle"), &bundle).unwrap();
        let args = json!({"mode":"apply","files":plan["files"],"manifest_id":plan["manifest_id"],"bundle_path":"bundle","bundle_sha256":hash(&bundle)});
        let out = run(&ws, &args, &state.path().join("apply")).unwrap();
        assert_eq!(out["changed_files"], 3);
        assert_eq!(out["reused_files"], 97);
        assert_eq!(out["bytes_written"], 9);
        let out = run(&ws, &args, &state.path().join("retry")).unwrap();
        assert_eq!(out["changed_files"], 0);
        std::fs::write(d.path().join("f99.bin"), b"user edit").unwrap();
        assert!(run(&ws, &args, &state.path().join("conflict")).is_err());
    }
    #[test]
    fn paths_and_overlapping_destinations_are_rejected_before_writes() {
        let args = json!({"files":[{"path":"a","size":0,"sha256":hash([])},{"path":"a/b","size":0,"sha256":hash([])}]});
        assert!(entries(&args).is_err());
        assert!(
            entries(&json!({"files":[{"path":"../escape","size":0,"sha256":hash([])}]})).is_err()
        );
    }
}
