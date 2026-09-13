//! Token-efficient protocol views over durable terminal output.
//! Shared command classification and text compaction live in `remote-hosts-token-output`;
//! this module only compacts Remote Hosts Code MCP envelopes.

pub(crate) use remote_hosts_token_output::{OutputProfile, classify, compact};
use serde_json::{Value, json};

/// Lifecycle timing is useful for diagnostics, but normal queue/receipt bookkeeping
/// is not useful model context. Keep only states that change the Agent's decision.
fn compact_lifecycle(value: &Value) -> Option<Value> {
    let gateway = &value["gateway"];
    if gateway["available"] == false {
        return Some(json!({"available":false}));
    }
    let delivery = &value["device_receipt_delivery"];
    let blocked = delivery["blocked"].as_u64().unwrap_or(0);
    let queue_ms = gateway["queue_ms"].as_u64().unwrap_or(0);
    if blocked > 0 || queue_ms > 5_000 {
        let mut result = json!({});
        if queue_ms > 5_000 {
            result["queue_ms"] = json!(queue_ms);
        }
        if blocked > 0 {
            result["receipt_blocked"] = json!(blocked);
        }
        return Some(result);
    }
    None
}

fn compact_terminal_status(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object.retain(|key, _| {
        matches!(
            key.as_str(),
            "id" | "state"
                | "exit_code"
                | "pty"
                | "output_complete"
                | "output_truncated"
                | "output_error"
        )
    });
}

fn compact_search(value: &mut Value) {
    let Some(matches) = value.get_mut("matches").and_then(Value::as_array_mut) else {
        return;
    };
    let flat = std::mem::take(matches);
    let count = flat.len();
    let mut files: Vec<Value> = Vec::new();
    for mut hit in flat {
        let path = hit
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let version = hit
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if let Some(object) = hit.as_object_mut() {
            object.remove("path");
            object.remove("version");
        }
        if let Some(group) = files.iter_mut().find(|group| {
            group["path"].as_str() == Some(path.as_str())
                && group["version"].as_str() == Some(version.as_str())
        }) {
            group["hits"].as_array_mut().expect("hits array").push(hit);
        } else {
            files.push(json!({"path":path,"version":version,"hits":[hit]}));
        }
    }
    if let Some(object) = value.as_object_mut() {
        object.remove("matches");
        object.insert("match_count".into(), json!(count));
        object.insert("files".into(), json!(files));
    }
}

fn compact_completed_shape(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object.remove("operation_lifecycle");
    object.remove("timing");
    object.remove("read_stats");
    if value.get("matches").is_some() {
        compact_search(value);
    }
    if let Some(terminal) = value.pointer_mut("/terminal_observation/terminal") {
        compact_terminal_status(terminal);
    }
}

fn compact_devices(value: &mut Value) {
    let Some(devices) = value.get_mut("devices").and_then(Value::as_array_mut) else {
        return;
    };
    for device in devices {
        let Some(object) = device.as_object_mut() else {
            continue;
        };
        if let Some(capabilities) = object
            .get_mut("capabilities")
            .and_then(Value::as_object_mut)
        {
            capabilities.retain(|key, _| {
                matches!(
                    key.as_str(),
                    "version"
                        | "wire_protocol"
                        | "tool_schema_revision"
                        | "skill_revision"
                        | "skill_consistent"
                        | "platform"
                        | "arch"
                        | "home_dir"
                        | "roots"
                        | "allow_write"
                        | "allow_exec"
                )
            });
        }
        if let Some(features) = object.get_mut("runtime_features")
            && let Some(names) = features.get("names").and_then(Value::as_array)
        {
            *features = json!({"protocol":features["protocol"],"count":names.len()});
        }
        let empty_delivery = object.get("receipt_delivery").is_some_and(|delivery| {
            ["pending", "sending", "blocked"]
                .iter()
                .all(|key| delivery[*key].as_u64().unwrap_or(0) == 0)
        });
        if empty_delivery {
            object.remove("receipt_delivery");
            object.remove("receipt_delivery_stale");
        }
        if object
            .get("maintenance")
            .is_some_and(|m| m["state"] == "open")
        {
            object.remove("maintenance");
        }
        if object.get("upgrade").is_some_and(Value::is_null) {
            object.remove("upgrade");
        } else if let Some(upgrade) = object.get_mut("upgrade") {
            let receipt = &upgrade["receipt"];
            *upgrade = json!({"version":receipt["version"],"state":receipt["state"]});
        }
    }
    if let Some(gateway) = value.get_mut("gateway").and_then(Value::as_object_mut) {
        gateway.retain(|key, _| {
            matches!(
                key.as_str(),
                "version"
                    | "wire_protocol"
                    | "min_agent_wire_protocol"
                    | "max_agent_wire_protocol"
                    | "tool_schema_revision"
                    | "tools_sha256"
                    | "tool_count"
                    | "refresh_required"
                    | "client_schema_comparison"
                    | "host_schema_status"
                    | "default_file_bytes"
                    | "max_file_bytes"
            )
        });
    }
}

/// Compact the MCP structured result itself, not only its duplicate text rendering.
/// Completed success metadata is recoverable with response_mode=full, so the default
/// view keeps task evidence and removes transport bookkeeping.
pub(crate) fn compact_response(tool: &str, mut value: Value) -> Value {
    if value.get("error").is_some() || value.get("failure_type").is_some() {
        return value;
    }
    let pending = value
        .get("pending")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if pending {
        let compacted = value.get("operation_lifecycle").and_then(compact_lifecycle);
        if let Some(object) = value.as_object_mut() {
            match compacted {
                Some(lifecycle) => {
                    object.insert("operation_lifecycle".into(), lifecycle);
                }
                None => {
                    object.remove("operation_lifecycle");
                }
            }
            object.remove("timing");
        }
    } else {
        compact_completed_shape(&mut value);
    }
    match tool {
        "devices_list" | "fleet_status" => compact_devices(&mut value),
        "code_search" => compact_search(&mut value),
        "terminal_read" => {
            let complete = value
                .pointer("/terminal/output_complete")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(terminal) = value.get_mut("terminal") {
                compact_terminal_status(terminal);
            }
            if let Some(object) = value.as_object_mut() {
                // These are fixed protocol facts in the compact path. Full mode still
                // exposes them for diagnostics, while compact keeps only information
                // that changes the Agent's next decision.
                object.remove("cursor_format");
                object.remove("output_stream");
                object.remove("output_view");
                if complete {
                    object.remove("retry_after_ms");
                }
                if object.get("output").and_then(Value::as_str) == Some("") {
                    object.remove("output");
                }
            }
        }
        "operation_get" => {
            if let Some(operations) = value.get_mut("operations").and_then(Value::as_array_mut) {
                for operation in operations {
                    compact_completed_shape(operation);
                }
            }
            // The common single-operation path returns the original tool's shape
            // directly, so shape-based compaction keeps async results as lean as
            // direct results without adding another repeated `operation_tool` field.
            compact_completed_shape(&mut value);
        }
        "terminal_exec" => {
            if let Some(terminal) = value.pointer_mut("/terminal_observation/terminal") {
                compact_terminal_status(terminal);
            }
        }
        _ => {}
    }
    value
}

pub(crate) fn compact_text(tool: &str, value: &Value) -> String {
    if let Some(error) = value.get("error").and_then(Value::as_str) {
        let mut text = error.chars().take(512).collect::<String>();
        if error.chars().count() > 512 {
            text.push('…');
        }
        return format!("{tool}: error: {text}");
    }
    match tool {
        "terminal_read" => format!(
            "terminal_read: {}/{}",
            value
                .pointer("/terminal/state")
                .and_then(Value::as_str)
                .unwrap_or("result"),
            value
                .pointer("/terminal/exit_code")
                .map_or_else(|| "?".into(), Value::to_string)
        ),
        "fleet_status" => format!(
            "fleet_status: {}/{} converged; online={}; gateway={}; all_converged={}",
            value
                .pointer("/summary/devices_converged")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            value
                .pointer("/summary/devices_total")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            value
                .pointer("/summary/devices_online")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            value
                .pointer("/gateway/version")
                .and_then(Value::as_str)
                .unwrap_or("?"),
            value
                .get("all_converged")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        ),
        "devices_list" => {
            let devices = value
                .get("devices")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let online = devices.iter().filter(|d| d["online"] == true).count();
            format!(
                "devices_list: {online}/{} online; gateway={}",
                devices.len(),
                value
                    .pointer("/gateway/version")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
            )
        }
        "code_read" => format!(
            "code_read: {} range(s)",
            value
                .get("ranges")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0)
        ),
        "code_search" => format!(
            "code_search: {} match(es)",
            value
                .get("match_count")
                .and_then(Value::as_u64)
                .or_else(|| value
                    .get("matches")
                    .and_then(Value::as_array)
                    .map(|v| v.len() as u64))
                .unwrap_or(0)
        ),
        _ => format!(
            "{tool}: {}",
            value.get("state").and_then(Value::as_str).unwrap_or("ok")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_high_noise_commands_without_touching_ptys() {
        assert_eq!(
            classify("cargo test --workspace", false),
            OutputProfile::CargoTest
        );
        assert_eq!(
            classify("cargo.exe clippy --all-targets", false),
            OutputProfile::CargoClippy
        );
        assert_eq!(
            classify("cargo check -p app", false),
            OutputProfile::CargoBuild
        );
        assert_eq!(
            classify("python -m pytest -q", false),
            OutputProfile::Pytest
        );
        assert_eq!(classify("cargo test", true), OutputProfile::Generic);
    }

    #[test]
    fn cargo_test_success_collapses_to_a_tiny_summary() {
        let mut raw = String::from(
            "   Compiling demo v0.1.0\n     Running unittests src/lib.rs\n\nrunning 100 tests\n",
        );
        for n in 0..100 {
            raw.push_str(&format!("test tests::case_{n} ... ok\n"));
        }
        raw.push_str(
            "\ntest result: ok. 100 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 1.00s\n",
        );
        let compacted = compact(OutputProfile::CargoTest, &raw, Some(0));
        assert!(compacted.output.contains("100 passed, 0 failed, 2 ignored"));
        assert!(!compacted.output.contains("case_99"));
        assert!(compacted.output.len() * 20 < raw.len());
    }

    #[test]
    fn twenty_five_k_token_cargo_test_regression_stays_below_two_hundred_tokens() {
        let mut raw = String::from("running 2500 tests\n");
        for n in 0..2500 {
            raw.push_str(&format!(
                "test integration::very_long_regression_case_{n:04}_with_context ... ok\n"
            ));
        }
        raw.push_str(
            "test result: ok. 2500 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n",
        );
        assert!(
            raw.len() >= 100_000,
            "fixture should model at least ~25k tokens"
        );
        let compacted = compact(OutputProfile::CargoTest, &raw, Some(0));
        assert!(
            compacted.output.len() <= 800,
            "compact view must stay below ~200 tokens"
        );
        assert!(compacted.metadata["saved_tokens"].as_u64().unwrap() >= 24_000);
        assert!(
            compacted
                .output
                .contains("2500 passed, 0 failed, 0 ignored")
        );
    }

    #[test]
    fn cargo_test_failure_preserves_diagnostics_and_locations() {
        let raw = "running 2 tests\ntest tests::good ... ok\ntest tests::bad ... FAILED\n\nfailures:\n---- tests::bad stdout ----\nthread 'tests::bad' panicked at src/lib.rs:42:9:\nassertion `left == right` failed\n  left: 1\n right: 2\n\nfailures:\n    tests::bad\n\ntest result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n";
        let compacted = compact(OutputProfile::CargoTest, raw, Some(101));
        assert!(compacted.output.contains("src/lib.rs:42:9"));
        assert!(
            compacted
                .output
                .contains("assertion `left == right` failed")
        );
        assert!(compacted.output.contains("tests::bad"));
        assert!(!compacted.output.contains("tests::good ... ok"));
    }

    #[test]
    fn cargo_test_compile_failure_keeps_primary_rustc_diagnostic() {
        let raw = "   Compiling demo v0.1.0\nerror[E0425]: cannot find value `missing_symbol` in this scope\n --> tests/repro.rs:3:13\n  |\n3 |     let _ = missing_symbol;\n  |             ^^^^^^^^^^^^^^ not found in this scope\nerror: could not compile `demo` (test \"repro\") due to 1 previous error\n";
        let compacted = compact(OutputProfile::CargoTest, raw, Some(101));
        assert!(compacted.output.contains("error[E0425]"));
        assert!(compacted.output.contains("tests/repro.rs:3:13"));
        assert!(compacted.output.contains("missing_symbol"));
    }

    #[test]
    fn generic_view_only_removes_terminal_decoration_and_duplicate_blanks() {
        let raw = "\x1b[31merror\x1b[0m\n\n\nkeep me\n";
        let compacted = compact(OutputProfile::Generic, raw, Some(1));
        assert!(compacted.output.contains("error"));
        assert!(compacted.output.contains("keep me"));
        assert!(!compacted.output.contains("\x1b[31m"));
    }

    #[test]
    fn generic_view_losslessly_folds_consecutive_repetition() {
        let raw = "heartbeat\nheartbeat\nheartbeat\nunique\n";
        let compacted = compact(OutputProfile::Generic, raw, Some(0));
        assert_eq!(compacted.output, "heartbeat\n[repeated 2x]\nunique\n");
        assert_eq!(compacted.metadata["folded_lines"], 2);
    }

    #[test]
    fn compact_success_drops_transport_bookkeeping_but_keeps_task_evidence() {
        let value = json!({
            "operation_id":"op","device_id":"dev","state":"completed","result":"kept",
            "timing":{"total_ms":12},
            "operation_lifecycle":{"gateway":{"queue_ms":3,"dispatch_to_result_ms":7},"device_receipt_delivery":{"pending":0,"sending":0,"blocked":0}}
        });
        let compacted = compact_response("code_apply_edits", value);
        assert_eq!(compacted["result"], "kept");
        assert!(compacted.get("operation_lifecycle").is_none());
        assert!(compacted.get("timing").is_none());
    }

    #[test]
    fn ordinary_pending_result_drops_non_actionable_lifecycle_noise() {
        let value = json!({
            "operation_id":"op","state":"dispatched","pending":true,"retry_after_ms":1000,
            "operation_lifecycle":{"gateway":{"available":true,"queue_ms":22,"dispatch_to_result_ms":null},"device_receipt_delivery":{"pending":1,"sending":0,"blocked":0}}
        });
        let compacted = compact_response("terminal_exec", value);
        assert!(compacted.get("operation_lifecycle").is_none());
        assert_eq!(compacted["retry_after_ms"], 1000);
    }

    #[test]
    fn blocked_pending_result_keeps_actionable_lifecycle_signal() {
        let value = json!({
            "operation_id":"op","state":"dispatched","pending":true,
            "operation_lifecycle":{"gateway":{"available":true,"queue_ms":22,"dispatch_to_result_ms":null},"device_receipt_delivery":{"pending":1,"sending":0,"blocked":2}}
        });
        let compacted = compact_response("terminal_exec", value);
        assert_eq!(compacted["operation_lifecycle"]["receipt_blocked"], 2);
    }

    #[test]
    fn compact_search_groups_repeated_path_and_version_fields() {
        let value = json!({"matches":[
            {"path":"src/lib.rs","version":"abc","line":1,"text":"alpha"},
            {"path":"src/lib.rs","version":"abc","line":9,"text":"beta"},
            {"path":"src/main.rs","version":"def","line":2,"text":"gamma"}
        ],"next_cursor":null,"skipped_files":0});
        let compacted = compact_response("code_search", value);
        assert_eq!(compacted["match_count"], 3);
        assert_eq!(compacted["files"].as_array().unwrap().len(), 2);
        assert_eq!(compacted["files"][0]["hits"].as_array().unwrap().len(), 2);
        assert!(compacted.get("matches").is_none());
    }

    #[test]
    fn compact_terminal_read_drops_constant_protocol_envelope() {
        let value = json!({
            "terminal":{"id":"t","state":"exited","exit_code":0,"pty":false,"output_complete":true,"output_truncated":false,"output_error":null,"output_profile":"cargo_test"},
            "output":"","cursor":65536,"has_more":false,"retry_after_ms":500,
            "cursor_format":"sanitized_utf8_v1","output_stream":"combined","output_view":"compact",
            "raw_cursor_start":0,"compression":{"profile":"cargo_test","raw_bytes":65536,"output_bytes":0,"saved_tokens":16384,"full":true}
        });
        let compacted = compact_response("terminal_read", value);
        assert!(compacted.get("cursor_format").is_none());
        assert!(compacted.get("output_stream").is_none());
        assert!(compacted.get("output_view").is_none());
        assert!(compacted.get("retry_after_ms").is_none());
        assert!(compacted.get("output").is_none());
        assert!(compacted["terminal"].get("output_profile").is_none());
        assert_eq!(compacted["cursor"], 65536);
        assert_eq!(compacted["compression"]["saved_tokens"], 16384);
    }

    #[test]
    fn compact_devices_removes_repeated_feature_and_empty_delivery_noise() {
        let value = json!({"devices":[{"device_id":"d","name":"box","online":true,
            "capabilities":{"version":"1","roots":["/w"],"allow_write":true,"allow_exec":true,"session":"secretless-session","transfer_limits":{"protocol":1}},
            "runtime_features":{"protocol":1,"names":["a","b","c"]},
            "receipt_delivery":{"pending":0,"sending":0,"blocked":0},"receipt_delivery_stale":false,
            "maintenance":{"state":"open","protocol":1},"upgrade":null}],
            "gateway":{"version":"1","tools_sha256":"h","tool_count":21,"refresh_required":false,"optional_inputs":{"huge":"noise"}}});
        let compacted = compact_response("devices_list", value);
        assert_eq!(compacted["devices"][0]["runtime_features"]["count"], 3);
        assert!(compacted["devices"][0].get("receipt_delivery").is_none());
        assert!(
            compacted["devices"][0]["capabilities"]
                .get("session")
                .is_none()
        );
        assert!(compacted["gateway"].get("optional_inputs").is_none());
    }
}
