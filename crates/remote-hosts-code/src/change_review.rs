//! Complete bounded review: tracked diffs plus explicitly labelled untracked files.
use crate::{
    files::{self, Workspace},
    hash,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{io::Read, time::Duration};
use tokio::io::AsyncReadExt;
const CAP: usize = 16 * 1024 * 1024;
async fn git(ws: &Workspace, args: &[&str], paths: &[String]) -> Result<Vec<u8>> {
    let mut cmd = tokio::process::Command::new("git");
    cmd.current_dir(&ws.root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_LITERAL_PATHSPECS", "1")
        .args(["--no-pager", "-c", "core.fsmonitor=false"])
        .args(args)
        .arg("--")
        .args(paths)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().context("missing git output")?;
    let mut bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(20),
        stdout.take((CAP + 1) as u64).read_to_end(&mut bytes),
    )
    .await
    .context("git review timed out")??;
    if bytes.len() > CAP {
        child.kill().await?;
        anyhow::bail!("review capacity: narrow paths");
    }
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .context("git review exit timed out")??;
    ensure!(status.success(), "git review requires a valid worktree");
    Ok(bytes)
}
pub(crate) async fn read(ws: &Workspace, v: &Value) -> Result<Value> {
    let paths: Vec<String> = v
        .get("paths")
        .map(|p| serde_json::from_value(p.clone()))
        .transpose()?
        .unwrap_or_default();
    ensure!(paths.len() <= 20, "too many diff paths");
    for p in &paths {
        files::relative(p)?;
    }
    let staged = v["staged"] == true;
    let args = if staged {
        vec![
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--no-color",
            "--cached",
        ]
    } else {
        vec!["diff", "--no-ext-diff", "--no-textconv", "--no-color"]
    };
    let bytes = git(ws, &args, &paths).await?;
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    let names = if staged || v["include_untracked"] == false {
        Vec::new()
    } else {
        git(
            ws,
            &["ls-files", "--others", "--exclude-standard", "-z"],
            &paths,
        )
        .await?
    };
    let entries: Vec<&[u8]> = names.split(|b| *b == 0).filter(|s| !s.is_empty()).collect();
    let dir = files::directory(ws)?;
    let mut details = Vec::new();
    let mut skipped = 0;
    for name in &entries {
        if details.len() >= 100 || text.len() >= CAP || serde_json::to_vec(&details)?.len() > 24000
        {
            skipped += 1;
            continue;
        }
        let Ok(path) = std::str::from_utf8(name) else {
            skipped += 1;
            continue;
        };
        files::relative(path)?;
        let meta = dir.symlink_metadata(path)?;
        if meta.is_symlink() || !meta.is_file() {
            text.push_str(&format!(
                "\nUntracked special entry (not followed): {path}\n"
            ));
            details.push(json!({"path":path,"kind":"special_not_followed","expanded":false}));
            continue;
        }
        if meta.len() > 8 * 1024 * 1024 {
            text.push_str(&format!(
                "\nUntracked large file (content omitted): {path}\n"
            ));
            details.push(json!({"path":path,"kind":"large","size":meta.len(),"expanded":false}));
            continue;
        }
        let mut data = Vec::new();
        dir.open(path)?
            .take(8 * 1024 * 1024 + 1)
            .read_to_end(&mut data)?;
        ensure!(
            data.len() <= 8 * 1024 * 1024,
            "file changed during review; narrow paths"
        );
        let version = hash(&data);
        let content = std::str::from_utf8(&data)
            .ok()
            .filter(|s| !s.contains('\0'));
        let expanded = content
            .is_some_and(|s| text.len() + s.len() + s.lines().count() + path.len() * 4 + 512 < CAP);
        if expanded {
            text.push_str(&format!("\ndiff --git a/{path} b/{path}\nnew file (untracked)\n--- /dev/null\n+++ b/{path}\n"));
            for line in content.unwrap().split_inclusive('\n') {
                text.push('+');
                text.push_str(line);
            }
            if !data.ends_with(b"\n") {
                text.push_str("\n\\ No newline at end of file\n");
            }
        } else {
            text.push_str(&format!(
                "\nUntracked binary or budget-limited file: {path} ({version})\n"
            ));
        }
        details.push(json!({"path":path,"kind":if content.is_some(){"text"}else{"binary"},"size":data.len(),"version":version,"expanded":expanded}));
    }
    let cursor = files::number(v, "cursor", 0, 0, 100000000)?;
    let max = files::number(v, "max_bytes", 16000, 1024, 65536)?;
    ensure!(
        cursor <= text.len() && text.is_char_boundary(cursor),
        "invalid diff cursor"
    );
    let version = hash(text.as_bytes());
    if let Some(expected) = v["expected_version"].as_str() {
        ensure!(
            expected == version,
            "version_conflict: diff changed; restart review"
        );
    }
    let (chunk, truncated) = files::bounded(&text[cursor..], max);
    Ok(
        json!({"diff":chunk,"truncated":truncated,"next_cursor":truncated.then_some(cursor+chunk.len()),"version":version,
        "untracked":details,"untracked_count":entries.len(),"untracked_omitted":skipped,"staged":staged,
        "coverage":"tracked Git diff plus nonignored untracked files unless staged/include_untracked=false; special entries never followed","review_protocol":1}),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn untracked_text_and_binary_are_not_empty_diff() {
        let d = tempfile::tempdir().unwrap();
        assert!(
            std::process::Command::new("git")
                .args(["init", "-q"])
                .current_dir(d.path())
                .status()
                .unwrap()
                .success()
        );
        std::fs::write(d.path().join("new.rs"), "fn feature() {}\n").unwrap();
        std::fs::write(d.path().join("binary"), [0, 1, 2]).unwrap();
        let ws = Workspace {
            id: "w".into(),
            device_id: "d".into(),
            root: d.path().canonicalize().unwrap(),
        };
        let v = read(&ws, &json!({})).await.unwrap();
        assert_eq!(v["untracked_count"], 2);
        assert!(v["diff"].as_str().unwrap().contains("+fn feature()"));
        assert!(v["diff"].as_str().unwrap().contains("binary"));
        let first = v["version"].clone();
        std::fs::write(d.path().join("new.rs"), "changed").unwrap();
        assert!(read(&ws, &json!({"expected_version":first})).await.is_err());
    }
}
