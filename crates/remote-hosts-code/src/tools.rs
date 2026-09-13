//! Small code-oriented tool catalog, shared by the gateway and local agent.
use anyhow::{Context, Result, bail, ensure};
use rmcp::model::Tool;
use serde_json::{Map, Value, json};
use std::sync::LazyLock;

pub fn scope(name: &str) -> Option<&'static str> {
    match name {
        "devices_list" | "workspace_open" | "code_list" | "code_search" | "code_read"
        | "code_symbols" | "code_diff" | "operation_get" | "terminal_read" | "file_download"
        | "workspace_context" => Some("code:read"),
        "code_apply_edits" | "change_resume" | "workspace_gc" | "files_sync" | "file_upload"
        | "transfer_cancel" | "transfer_resume" => Some("code:write"),
        "terminal_exec" | "terminal_input" | "terminal_cancel" => Some("terminal:exec"),
        _ => None,
    }
}
static CATALOG: LazyLock<Vec<Tool>> = LazyLock::new(build_catalog);

pub fn catalog() -> Vec<Tool> {
    CATALOG.clone()
}

/// Enforce the schema subset used by this fixed catalog at both trust boundaries.
/// Error messages include field paths, never argument values or command contents.
pub fn validate(name: &str, arguments: &Value) -> Result<()> {
    let tool = CATALOG
        .iter()
        .find(|t| t.name == name)
        .context("unknown tool")?;
    validate_node(&tool.input_schema, arguments, "$")
}

fn validate_node(schema: &Map<String, Value>, value: &Value, path: &str) -> Result<()> {
    if let Some(choices) = schema.get("enum").and_then(Value::as_array) {
        ensure!(
            choices.contains(value),
            "invalid_arguments: {path} is not an allowed value"
        );
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("object") => {
            let object = value
                .as_object()
                .with_context(|| format!("invalid_arguments: {path} expected object"))?;
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .context("invalid catalog object schema")?;
            for required in schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let key = required
                    .as_str()
                    .context("invalid catalog required field")?;
                ensure!(
                    object.contains_key(key),
                    "invalid_arguments: {path}.{key} is required"
                );
            }
            for (key, item) in object {
                let child = properties
                    .get(key)
                    .and_then(Value::as_object)
                    .with_context(|| format!("invalid_arguments: {path}.{key} is unknown"))?;
                validate_node(child, item, &format!("{path}.{key}"))?;
            }
        }
        Some("array") => {
            let items = value
                .as_array()
                .with_context(|| format!("invalid_arguments: {path} expected array"))?;
            let count = items.len() as u64;
            ensure!(
                schema
                    .get("minItems")
                    .and_then(Value::as_u64)
                    .is_none_or(|n| count >= n)
                    && schema
                        .get("maxItems")
                        .and_then(Value::as_u64)
                        .is_none_or(|n| count <= n),
                "invalid_arguments: {path} array length outside allowed bounds"
            );
            let child = schema
                .get("items")
                .and_then(Value::as_object)
                .context("invalid catalog array schema")?;
            for (index, item) in items.iter().enumerate() {
                validate_node(child, item, &format!("{path}[{index}]"))?;
            }
        }
        Some("string") => ensure!(
            value.is_string(),
            "invalid_arguments: {path} expected string"
        ),
        Some("boolean") => ensure!(
            value.is_boolean(),
            "invalid_arguments: {path} expected boolean"
        ),
        Some("integer") => {
            let number = value.as_u64().with_context(|| {
                format!("invalid_arguments: {path} expected nonnegative integer")
            })?;
            ensure!(
                schema
                    .get("minimum")
                    .and_then(Value::as_u64)
                    .is_none_or(|n| number >= n)
                    && schema
                        .get("maximum")
                        .and_then(Value::as_u64)
                        .is_none_or(|n| number <= n),
                "invalid_arguments: {path} integer outside allowed bounds"
            );
        }
        _ => bail!("unsupported catalog schema type"),
    }
    Ok(())
}

fn build_catalog() -> Vec<Tool> {
    let string = || json!({"type":"string"});
    let integer = |min, max| json!({"type":"integer","minimum":min,"maximum":max});
    let mut tools: Vec<Tool> = vec![];
    let mut add = |name: &str, description: &str, mut properties: Value, required: Vec<&str>| {
        properties["response_mode"] = json!({"type":"string","enum":["full","compact"],"default":"compact","description":"MCP envelope view; compact is the default. full restores diagnostic metadata."});
        let read = scope(name) == Some("code:read") && name != "workspace_open";
        let value = json!({"name":name,"description":description,"inputSchema":{"type":"object","properties":properties,"required":required,"additionalProperties":false},"outputSchema":{"type":"object","additionalProperties":true},"annotations":{"readOnlyHint":read,"destructiveHint":!read&&name!="workspace_open","idempotentHint":true,"openWorldHint":name.starts_with("terminal_")}});
        // Catalog is composed entirely from static, server-controlled JSON.
        if let Ok(tool) = serde_json::from_value(value) {
            tools.push(tool);
        }
    };
    add(
        "devices_list",
        "List authorized devices, freshness and roots, plus gateway schema hash and separately reported agent features. Optional known_tools_sha256 detects a stale client catalog; refresh is controlled by the host, not this server. Choose a device explicitly; never fail over.",
        json!({"known_tools_sha256":string()}),
        vec![],
    );
    add(
        "workspace_open",
        "Open a persistent workspace on one device and project root. Returns workspace_id bound to that device. Reuse it for all subsequent reads, edits and terminals.",
        json!({"device_id":string(),"root":string(),"idempotency_key":string()}),
        vec!["device_id", "root", "idempotency_key"],
    );
    add(
        "code_list",
        "Find unknown file paths using a relative glob; respects gitignore unless include_ignored=true. Returns a bounded page without file contents. Skip this call when the exact path and read range are already known.",
        json!({"workspace_id":string(),"glob":string(),"include_ignored":{"type":"boolean"},"cursor":string(),"limit":integer(1,500)}),
        vec!["workspace_id"],
    );
    add(
        "code_search",
        "Search literal text (default) or regex across project files. Returns bounded line matches with context and file versions. Narrow with glob and continue using cursor.",
        json!({"workspace_id":string(),"query":string(),"regex":{"type":"boolean"},"glob":string(),"include_ignored":{"type":"boolean"},"context_lines":integer(0,5),"cursor":string(),"limit":integer(1,200),"max_bytes":integer(1024,65536)}),
        vec!["workspace_id", "query"],
    );
    add(
        "code_read",
        "Batch-read needed ranges from request-local file snapshots. Long lines return UTF-8 fragments: continue with next_line/next_line_byte_offset and expected_version. allow_partial=true keeps successful ranges alongside per-range errors. Read known ranges directly.",
        json!({"workspace_id":string(),"requests":{"type":"array","minItems":1,"maxItems":20,"items":{"type":"object","properties":{"path":string(),"start_line":integer(1,10000000),"end_line":integer(1,10000000),"expected_version":string(),"line_byte_offset":integer(0,8388608)},"required":["path","start_line","end_line"],"additionalProperties":false}},"max_bytes":integer(1024,65536),"allow_partial":{"type":"boolean"}}),
        vec!["workspace_id", "requests"],
    );
    add(
        "code_symbols",
        "Get syntax-tree declarations and start/end lines for Rust, Python, JavaScript and TypeScript/TSX. Other languages return a labeled lexical outline. This is not compiler-resolved references. Read only chosen symbol ranges next.",
        json!({"workspace_id":string(),"path":string(),"start_line":integer(1,10000000),"limit":integer(1,300)}),
        vec!["workspace_id", "path"],
    );
    add(
        "code_apply_edits",
        "Apply version-checked precise replacements or a unified patch. All files preflight before changes; each file is atomic, and every batch returns a durable change_set_id with before/after versions. Partial or uncertain batches can be inspected and continued with change_resume. Never guess when a version or unique match conflicts. Reuse the same idempotency_key only for an uncertain identical submission. Return compact diffs.",
        json!({"workspace_id":string(),"idempotency_key":string(),"files":{"type":"array","minItems":1,"maxItems":20,"items":{"type":"object","properties":{"path":string(),"expected_version":string(),"action":{"type":"string","enum":["edit","create","delete"]},"edits":{"type":"array","maxItems":100,"items":{"type":"object","properties":{"old_text":string(),"new_text":string()},"required":["old_text","new_text"],"additionalProperties":false}},"patch":string(),"content":string()},"required":["path","expected_version"],"additionalProperties":false}}}),
        vec!["workspace_id", "idempotency_key", "files"],
    );
    add(
        "change_resume",
        "Resume the SAME durable multi-file edit change-set after a partial or uncertain apply. The original change_set_id must belong to this workspace and code_apply_edits. Files already at their recorded after-version are accepted; files still at their recorded before-version may be applied; any third version is preserved as a conflict. Reuse one idempotency_key only for an uncertain identical recovery attempt; after resolving a conflict, start the next recovery attempt with a new key. Never replays shell work or overwrites concurrent edits.",
        json!({"workspace_id":string(),"idempotency_key":string(),"change_set_id":string()}),
        vec!["workspace_id", "idempotency_key", "change_set_id"],
    );
    add(
        "workspace_gc",
        "Preview or apply bounded cleanup of old terminal logs, terminal status rows, finished transfer checkpoints and completed edit journals for this workspace. Active/running work, resumable transfers, partial change-sets, receipt outbox entries and local idempotency results are never deleted. Apply requires preview_id from an unchanged preview plus the same age/max_items policy. Preview and apply are distinct operations, so use distinct idempotency keys.",
        json!({"workspace_id":string(),"idempotency_key":string(),"action":{"type":"string","enum":["preview","apply"]},"older_than_seconds":integer(3600,2592000),"max_items":integer(1,1000),"preview_id":string()}),
        vec!["workspace_id", "idempotency_key", "action"],
    );
    add(
        "code_diff",
        "Review tracked and nonignored untracked files, including binary/special omissions. staged=true selects index only. Continue with cursor plus expected_version to reject changed reviews. Does not stage, commit or reset; external diff, text conversion and pathspec magic disabled.",
        json!({"workspace_id":string(),"paths":{"type":"array","maxItems":20,"items":string()},"staged":{"type":"boolean"},"cursor":integer(0,100000000),"max_bytes":integer(1024,65536),"include_untracked":{"type":"boolean"},"expected_version":string()}),
        vec!["workspace_id"],
    );
    add(
        "terminal_exec",
        "Start a command in the bound workspace. Default pty=false uses real pipes with stdin EOF and combined stdout/stderr; use pty=true only for interactive input. Shell has local-user authority outside code roots. Write-capable action. Default returns terminal_id immediately. Optional wait_ms up to 2000 returns initial output/status without rerunning; a wait timeout does not stop the command. Follow the returned cursor. Never retry uncertain execution with a new key.",
        json!({"workspace_id":string(),"idempotency_key":string(),"command":string(),"pty":{"type":"boolean"},"timeout_seconds":integer(1,7200),"cols":integer(20,500),"rows":integer(5,200),"wait_ms":integer(0,2000)}),
        vec!["workspace_id", "idempotency_key", "command"],
    );
    add(
        "terminal_read",
        "Read bounded terminal output. Non-PTY output defaults to token-optimized compact view; PTY defaults full. compact preserves unknown/error text while collapsing known build/test noise and returns measured savings. Cursor always advances over the durable full log; use output_mode=full with raw_cursor_start to inspect source bytes. Poll the same terminal; never rerun.",
        json!({"workspace_id":string(),"terminal_id":string(),"cursor":integer(0,100000000),"max_bytes":integer(1024,65536),"output_mode":{"type":"string","enum":["compact","full"]}}),
        vec!["workspace_id", "terminal_id"],
    );
    add(
        "terminal_input",
        "Write to the existing interactive terminal. Input is an execution action. Reuse idempotency_key to avoid duplicate keystrokes. Never send credentials through this tool.",
        json!({"workspace_id":string(),"terminal_id":string(),"text":string(),"idempotency_key":string()}),
        vec!["workspace_id", "terminal_id", "text", "idempotency_key"],
    );
    add(
        "terminal_cancel",
        "Cancel this terminal process group and preserve output/status. Only affects the specified terminal in the specified workspace.",
        json!({"workspace_id":string(),"terminal_id":string(),"idempotency_key":string()}),
        vec!["workspace_id", "terminal_id", "idempotency_key"],
    );
    add(
        "operation_get",
        "Observe one operation_id OR 1..20 operation_ids. Optional wait_ms up to 5000 waits for meaningful state/progress changes; cursor is a latest-state fingerprint, not an event replay cursor. Results include gateway queue/dispatch/result lifecycle timing, agent monotonic execution phases when available, linked terminal status/exit_code, and transfer progress. Never subtract gateway wall timestamps from agent clocks. It does not read terminal output, queue, retry or cancel work. Query omitted results individually.",
        json!({"operation_id":string(),"operation_ids":{"type":"array","minItems":1,"maxItems":20,"items":string()},"wait_ms":integer(0,5000),"cursor":string(),"max_bytes":integer(4096,131072)}),
        vec![],
    );
    add(
        "file_upload",
        "Transfer a conversation file to a relative path on the selected device. Binary-safe streaming; default limit is 64 MiB and 0.7+ agents may explicitly negotiate up to 256 MiB with max_bytes. Large transfers are rejected before queueing unless the selected agent reports support. Default refuses overwrite; replacing requires expected_version equal to the existing SHA-256. Optional sha256 verifies incoming bytes. 4 MiB checkpoints survive restarts. operation_get reports source authorization state; use transfer_resume with refreshed file authorization when required. Never put bytes/Base64 or a sandbox path in JSON.",
        json!({"workspace_id":string(),"idempotency_key":string(),"path":string(),"file":{"type":"object","properties":{"download_url":string(),"file_id":string(),"mime_type":string(),"file_name":string()},"required":["download_url","file_id"],"additionalProperties":false},"expected_version":string(),"sha256":string(),"max_bytes":integer(1,268435456)}),
        vec!["workspace_id", "idempotency_key", "path", "file"],
    );
    add(
        "file_download",
        "Snapshot a selected device file and send verified chunks to the gateway; checkpoints resume from the durable receiver offset across restarts. Default limit is 64 MiB and 0.7+ agents may explicitly negotiate up to 256 MiB with max_bytes. Large transfers are rejected before queueing unless the selected agent reports support and both sides enforce storage reserve. Return a temporary HTTPS link plus SHA-256 and size; does not modify the source.",
        json!({"workspace_id":string(),"idempotency_key":string(),"path":string(),"expected_version":string(),"max_bytes":integer(1,268435456)}),
        vec!["workspace_id", "idempotency_key", "path"],
    );
    add(
        "workspace_context",
        "Read bounded workspace transfer/terminal facts, recent durable change-set summaries, state counts and runtime identity. active_only narrows terminal/transfer facts; terminal_cursor and transfer_after continue independent live pages. Cursors bind the workspace and filters; restart pages for a fresh view after state changes. after_event replays durable workspace transitions from the returned scoped event cursor; expired cursors require a fresh snapshot. Reports whether explicit workspace_gc is available. No command text or credentials. Device-wide counts are labelled. Not chat-memory or build-verification evidence.",
        json!({"workspace_id":string(),"cursor":string(),"limit":integer(1,50),"transfer_after":string(),"terminal_cursor":string(),"active_only":{"type":"boolean"},"after_event":string()}),
        vec!["workspace_id"],
    );
    add(
        "transfer_cancel",
        "Request cancellation of an original file transfer. Permission checks include the original operation. Cancellation intent is durable and independent of the transfer lane. Query operation_get until cleanup_complete is true; request acceptance alone is not stopped/cleaned. Published results are not undone.",
        json!({"operation_id":string(),"idempotency_key":string()}),
        vec!["operation_id", "idempotency_key"],
    );
    add(
        "transfer_resume",
        "Resume a paused/awaiting_source file transfer with the SAME operation ID. Does not replay arbitrary commands or overwrite target-version preconditions. operation_get reports whether source authorization is available, expired or needs refresh. Optional file refreshes expired authorization; a changed file_id requires an original expected SHA-256. Retained checkpoints expire after 24 hours. Completed/cancelled operations are not rerun.",
        json!({"operation_id":string(),"idempotency_key":string(),"file":{"type":"object","properties":{"download_url":string(),"file_id":string(),"mime_type":string(),"file_name":string()},"required":["download_url","file_id"],"additionalProperties":false}}),
        vec!["operation_id", "idempotency_key"],
    );
    add(
        "files_sync",
        "Plan or apply a version-bound one-way file set (up to 256 regular files). Plan returns bound files and manifest_id. Upload a RHSYNC1 bundle with file_upload, then apply with bundle_path and bundle_sha256. Only changed content is bundled; exact matches are reused. No deletion, symlinks or archive extraction. Partial results preserve applied/pending items and journal; reapply the same manifest only after checking conflicts. This is a write-capable tool even in plan mode.",
        json!({"workspace_id":string(),"idempotency_key":string(),"mode":{"type":"string","enum":["plan","apply"]},"manifest_id":string(),"bundle_path":string(),"bundle_sha256":string(),"files":{"type":"array","minItems":1,"maxItems":256,"items":{"type":"object","properties":{"path":string(),"sha256":string(),"size":integer(0,67108864),"executable":{"type":"boolean"},"expected_version":string()},"required":["path","sha256","size"],"additionalProperties":false}}}),
        vec!["workspace_id", "idempotency_key", "mode", "files"],
    );
    // Preserve the host file-parameter metadata through rmcp serialization.
    for tool in &mut tools {
        if tool.name == "file_upload"
            || tool.name == "file_download"
            || tool.name == "transfer_resume"
        {
            let mut value = serde_json::to_value(&*tool).expect("static transfer tool");
            value["annotations"]["readOnlyHint"] = json!(false);
            value["annotations"]["openWorldHint"] = json!(true);
            value["annotations"]["destructiveHint"] = json!(tool.name == "file_upload");
            if tool.name == "file_upload" || tool.name == "transfer_resume" {
                value["_meta"] = json!({"openai/fileParams":["file"]});
            }
            *tool = serde_json::from_value(value).expect("static transfer descriptor");
        }
    }
    tools
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_tool_catalog_has_a_fixed_context_budget() -> Result<(), serde_json::Error> {
        let tools = catalog();
        let bytes = serde_json::to_vec(&tools)?.len();
        eprintln!("code_tool_schema_bytes={bytes}");
        assert!(bytes < 96 * 1024);
        Ok(())
    }
}
