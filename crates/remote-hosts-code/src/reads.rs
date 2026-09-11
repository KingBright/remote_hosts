//! Request-local file snapshots and forward-only UTF-8 line fragments.
use crate::{
    files::{self, Workspace},
    hash,
};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;

const SNAPSHOT_BUDGET: usize = 32 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Range {
    path: String,
    start_line: usize,
    end_line: usize,
    expected_version: Option<String>,
    #[serde(default)]
    line_byte_offset: usize,
}
struct Snapshot {
    text: String,
    version: String,
    lines: usize,
}

pub(crate) fn read(ws: &Workspace, args: &Value) -> Result<Value> {
    let ranges: Vec<Range> =
        serde_json::from_value(args.get("requests").cloned().context("missing requests")?)?;
    ensure!(
        !ranges.is_empty() && ranges.len() <= 20,
        "expected 1..20 read ranges"
    );
    let partial = args
        .get("allow_partial")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut remaining = files::number(args, "max_bytes", 16000, 1024, 65536)?;
    let directory = files::directory(ws)?;
    let mut snapshots: HashMap<String, std::result::Result<Snapshot, String>> = HashMap::new();
    let mut used = 0usize;
    let mut physical_reads = 0usize;
    let mut cache_hits = 0usize;
    let mut results = Vec::with_capacity(ranges.len());
    for range in ranges {
        ensure!(
            range.start_line >= 1 && range.end_line >= range.start_line,
            "invalid line range"
        );
        ensure!(
            range.line_byte_offset <= 8 * 1024 * 1024,
            "line_byte_offset outside bounds"
        );
        if remaining == 0 {
            results.push(json!({"path":range.path,"text":"","truncated":true,"next_line":range.start_line,"next_line_byte_offset":range.line_byte_offset,"reason":"response_budget"}));
            continue;
        }
        let snapshot = if let Some(snapshot) = snapshots.get(&range.path) {
            cache_hits += 1;
            snapshot
        } else {
            physical_reads += 1;
            let snapshot = files::read_text(&directory, &range.path)
                .and_then(|text| {
                    ensure!(
                        used.saturating_add(text.len()) <= SNAPSHOT_BUDGET,
                        "batch_snapshot_limit: narrow the batch"
                    );
                    used += text.len();
                    let version = hash(&text);
                    let lines = text.split_inclusive('\n').count();
                    Ok(Snapshot {
                        text,
                        version,
                        lines,
                    })
                })
                .map_err(|e| e.to_string());
            snapshots.entry(range.path.clone()).or_insert(snapshot)
        };
        let result = match snapshot {
            Ok(snapshot) => select(snapshot, &range, &mut remaining),
            Err(message) => Err(anyhow::anyhow!("read_failed: {message}")),
        };
        match result {
            Ok(value) => results.push(value),
            Err(error) if partial => {
                let message = error.to_string();
                let code = if message.starts_with("version_conflict") {
                    "version_conflict"
                } else if message.starts_with("invalid_cursor") {
                    "invalid_cursor"
                } else {
                    "read_failed"
                };
                let mut value = json!({"path":range.path,"error":{"code":code,"message":message},"truncated":false});
                if let Ok(snapshot) = snapshot {
                    value["current_version"] = json!(snapshot.version);
                }
                results.push(value);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(
        json!({"ranges":results,"read_stats":{"physical_reads":physical_reads,"cache_hits":cache_hits,"snapshot_bytes":used}}),
    )
}
fn select(snapshot: &Snapshot, range: &Range, remaining: &mut usize) -> Result<Value> {
    ensure!(
        range
            .expected_version
            .as_ref()
            .is_none_or(|v| v == &snapshot.version),
        "version_conflict: {} current_version={}",
        range.path,
        snapshot.version
    );
    ensure!(
        range.line_byte_offset == 0 || range.expected_version.is_some(),
        "invalid_cursor: resuming a line requires expected_version"
    );
    let mut next = range.start_line;
    let mut offset = range.line_byte_offset;
    let mut selected = String::new();
    let mut touched = None;
    for line in snapshot
        .text
        .split_inclusive('\n')
        .skip(range.start_line - 1)
        .take(range.end_line - range.start_line + 1)
    {
        ensure!(
            offset < line.len() && line.is_char_boundary(offset),
            "invalid_cursor: offset is outside this line or not a UTF-8 boundary"
        );
        let rest = &line[offset..];
        if rest.len() > *remaining && !selected.is_empty() {
            break;
        }
        let (chunk, truncated) = files::bounded(rest, *remaining);
        if chunk.is_empty() {
            break;
        }
        selected.push_str(chunk);
        *remaining -= chunk.len();
        touched = Some(next);
        if truncated {
            offset += chunk.len();
            break;
        }
        next += 1;
        offset = 0;
    }
    ensure!(
        range.line_byte_offset == 0 || touched.is_some(),
        "invalid_cursor: line does not exist"
    );
    let truncated = next <= range.end_line.min(snapshot.lines);
    Ok(
        json!({"path":range.path,"version":snapshot.version,"start_line":range.start_line,
        "end_line":touched.unwrap_or(range.start_line.saturating_sub(1)),"total_lines":snapshot.lines,
        "text":selected,"truncated":truncated,"line_byte_offset":range.line_byte_offset,
        "next_line":truncated.then_some(next),"next_line_byte_offset":truncated.then_some(offset),
        "partial_line":truncated && offset>0,"reason":if truncated {Some("response_budget")} else {None}}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Workspace) {
        let d = tempfile::tempdir().unwrap();
        let ws = Workspace {
            id: "test".into(),
            device_id: "test".into(),
            root: d.path().canonicalize().unwrap(),
        };
        (d, ws)
    }
    #[test]
    fn twenty_ranges_read_and_hash_one_file_once() {
        let (_d, ws) = fixture();
        std::fs::write(ws.root.join("a"), "first\nsecond\n").unwrap();
        let requests: Vec<_> = (0..20)
            .map(|_| json!({"path":"a","start_line":1,"end_line":2}))
            .collect();
        let out = read(&ws, &json!({"requests":requests})).unwrap();
        assert_eq!(out["read_stats"]["physical_reads"], 1);
        assert_eq!(out["read_stats"]["cache_hits"], 19);
        assert!(
            out["ranges"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["text"] == "first\nsecond\n")
        );
    }
    #[test]
    fn utf8_long_line_reconstructs_without_loop_or_loss() {
        let (_d, ws) = fixture();
        let text = format!("{}\nend\n", "你🧪".repeat(11000));
        std::fs::write(ws.root.join("a"), &text).unwrap();
        let mut request = json!({"path":"a","start_line":1,"end_line":2});
        let mut reconstructed = String::new();
        for _ in 0..100 {
            let out = read(&ws, &json!({"requests":[request],"max_bytes":1024})).unwrap();
            let r = &out["ranges"][0];
            assert!(!r["text"].as_str().unwrap().is_empty());
            reconstructed.push_str(r["text"].as_str().unwrap());
            if r["truncated"] == false {
                break;
            }
            request["start_line"] = r["next_line"].clone();
            request["line_byte_offset"] = r["next_line_byte_offset"].clone();
            request["expected_version"] = r["version"].clone();
        }
        assert_eq!(reconstructed, text);
    }
    #[test]
    fn partial_batch_preserves_success_and_reports_conflicts() {
        let (_d, ws) = fixture();
        std::fs::write(ws.root.join("a"), "ok").unwrap();
        let mut args = json!({"requests":[{"path":"missing","start_line":1,"end_line":1},{"path":"a","start_line":1,"end_line":1},{"path":"a","start_line":1,"end_line":1,"expected_version":"old"}]});
        assert!(read(&ws, &args).is_err());
        args["allow_partial"] = json!(true);
        let out = read(&ws, &args).unwrap();
        assert_eq!(out["ranges"][0]["error"]["code"], "read_failed");
        assert_eq!(out["ranges"][1]["text"], "ok");
        assert_eq!(out["ranges"][2]["error"]["code"], "version_conflict");
    }
    #[test]
    fn fragment_requires_version_and_valid_utf8_cursor() {
        let (_d, ws) = fixture();
        std::fs::write(ws.root.join("a"), "你好").unwrap();
        let mut req = json!({"path":"a","start_line":1,"end_line":1,"line_byte_offset":3});
        assert!(read(&ws, &json!({"requests":[req]})).is_err());
        req["expected_version"] = json!(hash("你好"));
        assert_eq!(
            read(&ws, &json!({"requests":[req]})).unwrap()["ranges"][0]["text"],
            "好"
        );
        req["line_byte_offset"] = json!(1);
        assert!(read(&ws, &json!({"requests":[req]})).is_err());
    }
}
