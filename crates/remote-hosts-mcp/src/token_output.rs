//! Token-efficient protocol views over durable Remote Hosts output.
//! Shared command classification and text compaction live in `remote-hosts-token-output`;
//! this module only groups durable operation/PTY chunks into Agent-facing views.

use remote_hosts_domain::{OperationOutputChunk, OperationRun, PtyOutputChunk};
use remote_hosts_token_output::{OutputProfile, classify, compact};
use serde_json::{Value, json};

pub(crate) fn classify_operation(operation: &OperationRun) -> OutputProfile {
    // The activity preview can be truncated. Classify the complete stored shell
    // script when available, without exposing its unredacted contents in output.
    if let Some(profile) = &operation.command_profile_json
        && matches!(
            profile["name"].as_str(),
            Some("shell.posix" | "shell.powershell")
        )
    {
        return profile["args"]
            .as_array()
            .and_then(|args| args.last())
            .and_then(Value::as_str)
            .map_or(OutputProfile::Generic, |script| classify(script, false));
    }
    let summary = &operation.redacted_command_summary;
    let command = summary
        .strip_prefix("shell.posix via pooled workspace:")
        .or_else(|| summary.strip_prefix("shell.powershell via pooled workspace:"))
        .unwrap_or(summary);
    classify(command, false)
}

pub(crate) fn compact_operation_text(operation: &OperationRun, input: &str) -> (String, Value) {
    let compacted = compact(
        classify_operation(operation),
        input,
        operation.exit_code.map(i64::from),
    );
    (compacted.output, compacted.metadata)
}

fn joined_operation_text(chunks: &[OperationOutputChunk]) -> String {
    let mut text = String::new();
    for chunk in chunks {
        text.push_str(&chunk.redacted_text);
    }
    text
}

fn joined_pty_text(chunks: &[PtyOutputChunk]) -> String {
    let mut text = String::new();
    for chunk in chunks {
        text.push_str(&chunk.redacted_text);
    }
    text
}

pub(crate) fn compact_operation_chunks(
    operation: &OperationRun,
    chunks: &[OperationOutputChunk],
) -> (Vec<Value>, Option<Value>) {
    let Some(first) = chunks.first() else {
        return (Vec::new(), None);
    };
    let Some(last) = chunks.last() else {
        return (Vec::new(), None);
    };
    let text = joined_operation_text(chunks);
    let compacted = compact(
        classify_operation(operation),
        &text,
        operation.exit_code.map(i64::from),
    );
    let output = json!({
        "sequence": last.sequence,
        "sequence_start": first.sequence,
        "sequence_end": last.sequence,
        "source_chunks": chunks.len(),
        "stream": "compact",
        "text": compacted.output,
        "truncated": chunks.iter().any(|chunk| chunk.truncated)
    });
    (vec![output], Some(compacted.metadata))
}

pub(crate) fn compact_unscoped_operation_chunks(
    chunks: &[OperationOutputChunk],
) -> (Vec<Value>, Option<Value>) {
    if chunks.is_empty() {
        return (Vec::new(), None);
    }
    let raw_bytes = chunks
        .iter()
        .map(|chunk| chunk.redacted_text.len())
        .sum::<usize>();
    let mut groups: Vec<(remote_hosts_domain::OperationId, Vec<&OperationOutputChunk>)> =
        Vec::new();
    for chunk in chunks {
        if let Some((_, group)) = groups.iter_mut().find(|(id, _)| *id == chunk.operation_id) {
            group.push(chunk);
        } else {
            groups.push((chunk.operation_id, vec![chunk]));
        }
    }
    let mut output = Vec::with_capacity(groups.len());
    let mut output_bytes = 0usize;
    let mut folded_lines = 0u64;
    for (operation_id, group) in groups {
        let (Some(first), Some(last)) = (group.first(), group.last()) else {
            continue;
        };
        let mut text = String::new();
        for chunk in &group {
            text.push_str(&chunk.redacted_text);
        }
        let compacted = compact(OutputProfile::Generic, &text, None);
        output_bytes += compacted.output.len();
        folded_lines += compacted.metadata["folded_lines"].as_u64().unwrap_or(0);
        output.push(json!({
            "operation_id": operation_id,
            "sequence": last.sequence,
            "sequence_start": first.sequence,
            "sequence_end": last.sequence,
            "source_chunks": group.len(),
            "stream": "compact",
            "text": compacted.output,
            "truncated": group.iter().any(|chunk| chunk.truncated)
        }));
    }
    let saved = raw_bytes.saturating_sub(output_bytes);
    let mut metadata = json!({
        "profile": "generic",
        "raw_bytes": raw_bytes,
        "output_bytes": output_bytes,
        "saved_tokens": saved / 4,
        "full_output_available": true
    });
    if folded_lines > 0 {
        metadata["folded_lines"] = json!(folded_lines);
    }
    (output, Some(metadata))
}

pub(crate) fn compact_pty_chunks(chunks: &[PtyOutputChunk]) -> (Vec<Value>, Option<Value>) {
    let Some(first) = chunks.first() else {
        return (Vec::new(), None);
    };
    let Some(last) = chunks.last() else {
        return (Vec::new(), None);
    };
    let text = joined_pty_text(chunks);
    // PTY is interactive. Generic compaction only removes terminal decoration,
    // CR redraw history, duplicate blank lines and exact consecutive repetition.
    let compacted = compact(OutputProfile::Generic, &text, None);
    let output = json!({
        "sequence": last.sequence,
        "sequence_start": first.sequence,
        "sequence_end": last.sequence,
        "source_chunks": chunks.len(),
        "stream": "compact",
        "text": compacted.output,
        "truncated": chunks.iter().any(|chunk| chunk.truncated)
    });
    (vec![output], Some(compacted.metadata))
}

#[cfg(test)]
mod tests {
    use super::*;
    use remote_hosts_domain::{
        AccessPathId, AgentSessionId, ConnectorId, HostId, OperationId, OperationState,
        OperationType, OutputStream, WorkspaceId, now_utc,
    };
    use std::fmt::Write as _;

    fn operation(command: &str, exit_code: Option<i32>) -> OperationRun {
        OperationRun {
            id: OperationId::new(),
            host_id: HostId::new(),
            access_path_id: AccessPathId::new(),
            connector_id: ConnectorId::new(),
            session_id: None,
            workspace_id: Some(WorkspaceId::new()),
            agent_session_id: Some(AgentSessionId::new()),
            idempotency_key: None,
            requires_write_lease: false,
            coordination_scope: "host".to_owned(),
            coordination_scopes: vec!["host".to_owned()],
            operation_type: OperationType::ReadonlyExec,
            intent: "test".to_owned(),
            state: if exit_code == Some(0) {
                OperationState::Succeeded
            } else {
                OperationState::Failed
            },
            started_at: now_utc(),
            finished_at: Some(now_utc()),
            exit_code,
            timeout_seconds: 60,
            redacted_command_summary: command.to_owned(),
            command_profile_json: None,
            transport_evidence: None,
            redacted_output_summary: None,
            log_ref: None,
            attempt_count: 1,
            claim_token: None,
            claimed_at: None,
            lease_expires_at: None,
            last_error: None,
        }
    }

    fn chunk(operation: &OperationRun, sequence: u64, text: String) -> OperationOutputChunk {
        OperationOutputChunk {
            id: remote_hosts_domain::OperationOutputChunkId::new(),
            operation_id: operation.id,
            workspace_id: operation.workspace_id.unwrap_or_default(),
            stream: OutputStream::Stdout,
            sequence,
            byte_len: text.len() as u64,
            redacted_text: text,
            truncated: false,
            created_at: now_utc(),
        }
    }

    #[test]
    fn twenty_five_k_token_cargo_test_collapses_below_two_hundred_tokens() {
        let operation = operation(
            "shell.posix via pooled workspace: cargo test --workspace",
            Some(0),
        );
        let mut raw = String::from("running 2500 tests\n");
        for n in 0..2500 {
            let _ = writeln!(
                raw,
                "test integration::very_long_regression_case_{n:04}_with_context ... ok"
            );
        }
        raw.push_str(
            "test result: ok. 2500 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n",
        );
        assert!(raw.len() >= 100_000);
        let chunks = vec![chunk(&operation, 7, raw)];
        let (view, metadata) = compact_operation_chunks(&operation, &chunks);
        let text = view[0]["text"].as_str().unwrap_or_default();
        assert!(
            text.len() <= 800,
            "compact view must stay below ~200 tokens"
        );
        assert!(text.contains("2500 passed, 0 failed, 0 ignored"));
        assert!(
            metadata
                .as_ref()
                .and_then(|value| value["saved_tokens"].as_u64())
                .is_some_and(|saved| saved >= 24_000)
        );
        assert_eq!(view[0]["sequence"], 7);
    }

    #[test]
    fn complete_stored_script_takes_precedence_over_activity_preview() {
        let mut operation = operation("cargo test --workspace", Some(0));
        operation.command_profile_json = Some(json!({
            "name": "shell.posix",
            "args": ["-lc", "cargo test --workspace && cat verification.json"]
        }));
        assert_eq!(classify_operation(&operation), OutputProfile::Generic);

        operation.command_profile_json = Some(json!({
            "name": "shell.posix",
            "args": ["-lc", "cargo test --workspace"]
        }));
        assert_eq!(classify_operation(&operation), OutputProfile::CargoTest);

        operation.command_profile_json = Some(json!({"name": "shell.posix"}));
        assert_eq!(classify_operation(&operation), OutputProfile::Generic);
    }

    #[test]
    fn known_summary_headers_are_removed_without_interpreting_script_keywords() {
        for header in ["shell.posix", "shell.powershell"] {
            let direct = operation(
                &format!("{header} via pooled workspace:\ncargo test"),
                Some(0),
            );
            assert_eq!(classify_operation(&direct), OutputProfile::CargoTest);
            let mixed = operation(
                &format!("{header} via pooled workspace:\ncargo test; cat result.json"),
                Some(0),
            );
            assert_eq!(classify_operation(&mixed), OutputProfile::Generic);
            let truncated = operation(
                &format!("{header} via pooled workspace:\ncargo test\n... <truncated>"),
                Some(0),
            );
            assert_eq!(classify_operation(&truncated), OutputProfile::Generic);
        }
    }

    #[test]
    fn cargo_failure_preserves_diagnostic_and_source_location() {
        let operation = operation("cargo test", Some(101));
        let raw = "Compiling demo v0.1.0\nerror[E0425]: cannot find value `missing_symbol` in this scope\n --> tests/repro.rs:3:13\n  |\n3 | let _ = missing_symbol;\n";
        let (view, _) = compact_operation_chunks(&operation, &[chunk(&operation, 3, raw.into())]);
        let text = view[0]["text"].as_str().unwrap_or_default();
        assert!(text.contains("error[E0425]"));
        assert!(text.contains("tests/repro.rs:3:13"));
        assert!(text.contains("missing_symbol"));
    }

    #[test]
    fn generic_pty_compaction_preserves_unique_prompt_and_folds_refresh_noise() {
        let text = "\x1b[2Kprogress 10%\rprogress 90%\nheartbeat\nheartbeat\nPassword: ".to_owned();
        let pty = PtyOutputChunk {
            id: remote_hosts_domain::PtyOutputChunkId::new(),
            pty_session_id: remote_hosts_domain::PtySessionId::new(),
            workspace_id: WorkspaceId::new(),
            stream: OutputStream::Stdout,
            sequence: 9,
            redacted_text: text,
            byte_len: 0,
            truncated: false,
            created_at: now_utc(),
        };
        let (view, metadata) = compact_pty_chunks(&[pty]);
        let text = view[0]["text"].as_str().unwrap_or_default();
        assert!(text.contains("progress 90%"));
        assert!(text.contains("Password:"));
        assert!(text.contains("[repeated 1x]"));
        assert!(
            metadata
                .as_ref()
                .and_then(|value| value["saved_tokens"].as_u64())
                .is_some_and(|saved| saved > 0)
        );
    }
}
