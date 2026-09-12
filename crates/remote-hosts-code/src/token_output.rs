//! Token-efficient views over durable terminal output.
//! Raw sanitized logs remain the source of truth; this module only changes what
//! the model sees by default. Filters are conservative: known routine lines are
//! removed while unknown and failure text is preserved verbatim.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutputProfile {
    #[default]
    Generic,
    CargoTest,
    CargoBuild,
    CargoClippy,
    Pytest,
}
impl OutputProfile {
    pub(crate) fn is_generic(&self) -> bool {
        *self == Self::Generic
    }
}

pub(crate) fn classify(command: &str, interactive: bool) -> OutputProfile {
    if interactive {
        return OutputProfile::Generic;
    }
    let normalized = command
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
        .replace("cargo.exe ", "cargo ");
    if normalized.contains("cargo nextest") || normalized.contains("cargo test") {
        OutputProfile::CargoTest
    } else if normalized.contains("cargo clippy") {
        OutputProfile::CargoClippy
    } else if normalized.contains("cargo build") || normalized.contains("cargo check") {
        OutputProfile::CargoBuild
    } else if normalized.contains("pytest") || normalized.contains("python -m pytest") {
        OutputProfile::Pytest
    } else {
        OutputProfile::Generic
    }
}

#[derive(Debug)]
pub(crate) struct CompactOutput {
    pub output: String,
    pub metadata: Value,
}

fn strip_terminal_controls(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != 0x1b {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        i += 1;
        if i >= bytes.len() {
            break;
        }
        match bytes[i] {
            b'[' => {
                i += 1;
                while i < bytes.len() {
                    let b = bytes[i];
                    i += 1;
                    if (0x40..=0x7e).contains(&b) {
                        break;
                    }
                }
            }
            b']' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == 0x07 {
                        i += 1;
                        break;
                    }
                    if bytes[i] == 0x1b && i + 1 < bytes.len() && bytes[i + 1] == b'\\' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn normalized_lines(input: &str) -> Vec<String> {
    strip_terminal_controls(input)
        .split('\n')
        .map(|line| {
            line.rsplit('\r')
                .find(|part| !part.is_empty())
                .unwrap_or("")
                .trim_end()
                .to_owned()
        })
        .collect()
}

fn result_counts(line: &str) -> (u64, u64, u64) {
    let words: Vec<_> = line.split_whitespace().collect();
    let mut passed = 0;
    let mut failed = 0;
    let mut ignored = 0;
    for (index, word) in words.iter().enumerate() {
        let value = index
            .checked_sub(1)
            .and_then(|i| words[i].trim_end_matches(';').parse().ok())
            .unwrap_or(0);
        match word.trim_end_matches(';') {
            "passed" => passed += value,
            "failed" => failed += value,
            "ignored" => ignored += value,
            _ => {}
        }
    }
    (passed, failed, ignored)
}

fn cargo_progress(line: &str) -> bool {
    let t = line.trim_start();
    [
        "Compiling ",
        "Checking ",
        "Fresh ",
        "Downloading ",
        "Downloaded ",
        "Blocking waiting for file lock",
    ]
    .iter()
    .any(|prefix| t.starts_with(prefix))
        || t.starts_with("Finished `")
        || t.starts_with("Running unittests ")
        || t.starts_with("Running tests/")
        || t.starts_with("Running benches/")
        || t.starts_with("Doc-tests ")
        || t.starts_with("running ") && (t.ends_with(" test") || t.ends_with(" tests"))
}

fn passing_test_line(line: &str) -> bool {
    let t = line.trim();
    t.starts_with("test ") && (t.ends_with(" ... ok") || t.ends_with(" ... ignored"))
}

fn pytest_progress(line: &str) -> bool {
    let t = line.trim();
    if t.is_empty()
        || t.starts_with("============================= test session starts")
        || t.starts_with("platform ")
        || t.starts_with("collected ")
    {
        return true;
    }
    let progress = t.strip_suffix("[100%]").unwrap_or(t).trim();
    !progress.is_empty()
        && progress
            .chars()
            .all(|c| matches!(c, '.' | 's' | 'x' | 'X' | '%' | '[' | ']' | '0'..='9'))
}

pub(crate) fn compact(
    profile: OutputProfile,
    input: &str,
    exit_code: Option<i64>,
) -> CompactOutput {
    let raw_bytes = input.len();
    let lines = normalized_lines(input);
    let mut kept = Vec::new();
    let mut omitted = 0usize;
    let mut passed = 0u64;
    let mut failed = 0u64;
    let mut ignored = 0u64;
    let mut previous_blank = true;

    for line in lines {
        let trimmed = line.trim();
        let drop = match profile {
            OutputProfile::CargoTest => {
                if trimmed.starts_with("test result: ok.") {
                    let counts = result_counts(trimmed);
                    passed += counts.0;
                    failed += counts.1;
                    ignored += counts.2;
                    true
                } else {
                    cargo_progress(&line) || passing_test_line(&line) || trimmed.is_empty()
                }
            }
            OutputProfile::CargoBuild | OutputProfile::CargoClippy => {
                cargo_progress(&line) || trimmed.is_empty()
            }
            OutputProfile::Pytest => pytest_progress(&line),
            OutputProfile::Generic => false,
        };
        if drop {
            omitted += 1;
            continue;
        }
        if profile == OutputProfile::CargoTest && trimmed.starts_with("test result: FAILED.") {
            let counts = result_counts(trimmed);
            passed += counts.0;
            failed += counts.1;
            ignored += counts.2;
        }
        if profile == OutputProfile::Generic && trimmed.is_empty() {
            if previous_blank {
                omitted += 1;
                continue;
            }
            previous_blank = true;
        } else {
            previous_blank = false;
        }
        kept.push(line);
    }

    let profile_name = serde_json::to_value(profile)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_else(|| "generic".into());
    if profile != OutputProfile::Generic && omitted > 0 {
        let state = exit_code.map_or("running".into(), |code| format!("exit={code}"));
        let counts = if profile == OutputProfile::CargoTest && passed + failed + ignored > 0 {
            format!("; tests={passed} passed/{failed} failed/{ignored} ignored")
        } else {
            String::new()
        };
        kept.push(format!(
            "[compact {profile_name}: {state}{counts}; omitted {omitted} routine lines]"
        ));
    }
    let mut output = kept.join("\n");
    if input.ends_with('\n') && !output.is_empty() {
        output.push('\n');
    }
    if output.len() > raw_bytes && profile == OutputProfile::Generic {
        output = strip_terminal_controls(input);
    }
    let output_bytes = output.len();
    let saved_bytes = raw_bytes.saturating_sub(output_bytes);
    let metadata = json!({
        "view":"compact",
        "profile":profile,
        "raw_bytes":raw_bytes,
        "output_bytes":output_bytes,
        "omitted_lines":omitted,
        "estimated_input_tokens":raw_bytes.div_ceil(4),
        "estimated_output_tokens":output_bytes.div_ceil(4),
        "estimated_tokens_saved":saved_bytes/4,
        "savings_pct": if raw_bytes == 0 { 0.0 } else { (saved_bytes as f64 * 1000.0 / raw_bytes as f64).round() / 10.0 },
        "full_output_available":true
    });
    CompactOutput { output, metadata }
}

fn compact_lifecycle(value: &Value) -> Value {
    let gateway = &value["gateway"];
    if gateway["available"] == false {
        return json!({"available":false});
    }
    let delivery = &value["device_receipt_delivery"];
    let pending = delivery["pending"].as_u64().unwrap_or(0);
    let sending = delivery["sending"].as_u64().unwrap_or(0);
    let blocked = delivery["blocked"].as_u64().unwrap_or(0);
    let mut result = json!({
        "queue_ms":gateway["queue_ms"],
        "dispatch_to_result_ms":gateway["dispatch_to_result_ms"]
    });
    if pending + sending + blocked > 0 {
        result["receipt_delivery"] = json!({"pending":pending,"sending":sending,"blocked":blocked});
    }
    result
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
                | "output_profile"
        )
    });
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
                    "version" | "roots" | "allow_write" | "allow_exec"
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
    if let Some(object) = value.as_object_mut() {
        if pending {
            if let Some(lifecycle) = object.get_mut("operation_lifecycle") {
                *lifecycle = compact_lifecycle(lifecycle);
            }
        } else {
            object.remove("operation_lifecycle");
            object.remove("timing");
        }
        if tool == "code_read" {
            object.remove("read_stats");
        }
    }
    match tool {
        "devices_list" => compact_devices(&mut value),
        "terminal_read" => {
            if let Some(terminal) = value.get_mut("terminal") {
                compact_terminal_status(terminal);
            }
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
            "terminal_read: {} exit={} output={}B saved~{}t",
            value
                .pointer("/terminal/state")
                .and_then(Value::as_str)
                .unwrap_or("result"),
            value
                .pointer("/terminal/exit_code")
                .map_or_else(|| "?".into(), Value::to_string),
            value
                .get("output")
                .and_then(Value::as_str)
                .map(str::len)
                .unwrap_or(0),
            value
                .pointer("/compression/estimated_tokens_saved")
                .and_then(Value::as_u64)
                .unwrap_or(0)
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
                .get("matches")
                .and_then(Value::as_array)
                .map(Vec::len)
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
        assert!(compacted.output.contains("100 passed/0 failed/2 ignored"));
        assert!(!compacted.output.contains("case_99"));
        assert!(compacted.output.len() * 10 < raw.len());
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
    fn generic_view_only_removes_terminal_decoration_and_duplicate_blanks() {
        let raw = "\x1b[31merror\x1b[0m\n\n\nkeep me\n";
        let compacted = compact(OutputProfile::Generic, raw, Some(1));
        assert!(compacted.output.contains("error"));
        assert!(compacted.output.contains("keep me"));
        assert!(!compacted.output.contains("\x1b[31m"));
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
