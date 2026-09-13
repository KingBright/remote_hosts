//! Shared token-efficient views for all Remote Hosts frontends.
//! Durable output remains the source of truth; this crate only builds model-facing views.

use serde_json::{Value, json};

/// Semantic output family used to select conservative compaction rules.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputProfile {
    /// Unknown or interactive output; only visual-equivalent cleanup is allowed.
    #[default]
    Generic,
    /// Cargo test/nextest output.
    CargoTest,
    /// Cargo build/check output.
    CargoBuild,
    /// Cargo clippy output.
    CargoClippy,
    /// Pytest output.
    Pytest,
    /// Git log history output.
    GitLog,
    /// Git status output.
    GitStatus,
    /// Ripgrep/grep enumeration output.
    Search,
}

impl OutputProfile {
    /// Returns whether only conservative generic cleanup is allowed.
    #[must_use]
    pub fn is_generic(&self) -> bool {
        *self == Self::Generic
    }

    /// Stable profile label used in metadata.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::CargoTest => "cargo_test",
            Self::CargoBuild => "cargo_build",
            Self::CargoClippy => "cargo_clippy",
            Self::Pytest => "pytest",
            Self::GitLog => "git_log",
            Self::GitStatus => "git_status",
            Self::Search => "search",
        }
    }
}

/// Compact text plus measurable savings metadata.
#[derive(Debug)]
pub struct CompactOutput {
    /// Model-facing compact text.
    pub output: String,
    /// Raw/output byte counts, token estimate and recovery metadata.
    pub metadata: Value,
}

/// Classifies a command without executing or rewriting it.
#[must_use]
pub fn classify(command: &str, interactive: bool) -> OutputProfile {
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
    } else if normalized.contains("python -m pytest") || normalized.contains("pytest") {
        OutputProfile::Pytest
    } else if normalized.contains("git log") {
        OutputProfile::GitLog
    } else if normalized.contains("git status") {
        OutputProfile::GitStatus
    } else if normalized.starts_with("rg ")
        || normalized.contains(" rg ")
        || normalized.starts_with("grep ")
        || normalized.contains(" grep ")
    {
        OutputProfile::Search
    } else {
        OutputProfile::Generic
    }
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
                    let byte = bytes[i];
                    i += 1;
                    if (0x40..=0x7e).contains(&byte) {
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
        .lines()
        .map(|line| {
            // Progress bars commonly redraw one logical line with CR. Keep only
            // the final visible frame, which is what a human terminal shows.
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
    let line = line.trim_start();
    [
        "Compiling ",
        "Checking ",
        "Fresh ",
        "Downloading ",
        "Downloaded ",
        "Blocking waiting for file lock",
    ]
    .iter()
    .any(|prefix| line.starts_with(prefix))
        || line.starts_with("Finished `")
        || line.starts_with("Running unittests ")
        || line.starts_with("Running tests/")
        || line.starts_with("Running benches/")
        || line.starts_with("Doc-tests ")
        || line.starts_with("running ") && (line.ends_with(" test") || line.ends_with(" tests"))
}

fn passing_test_line(line: &str) -> bool {
    let line = line.trim();
    line.starts_with("test ") && (line.ends_with(" ... ok") || line.ends_with(" ... ignored"))
}

fn pytest_progress(line: &str) -> bool {
    let line = line.trim();
    if line.is_empty()
        || line.starts_with("============================= test session starts")
        || line.starts_with("platform ")
        || line.starts_with("collected ")
    {
        return true;
    }
    let progress = line.strip_suffix("[100%]").unwrap_or(line).trim();
    !progress.is_empty()
        && progress
            .chars()
            .all(|c| matches!(c, '.' | 's' | 'x' | 'X' | '%' | '[' | ']' | '0'..='9'))
}

fn fold_repeated_lines(lines: &[String]) -> (Vec<String>, usize) {
    let mut output = Vec::with_capacity(lines.len());
    let mut folded = 0usize;
    let mut index = 0;
    while index < lines.len() {
        let mut end = index + 1;
        while end < lines.len() && lines[end] == lines[index] {
            end += 1;
        }
        let count = end - index;
        output.push(lines[index].clone());
        if count > 1 {
            folded += count - 1;
            output.push(format!("[repeated {}x]", count - 1));
        }
        index = end;
    }
    (output, folded)
}

fn sample_lines(lines: Vec<String>, head: usize, tail: usize) -> (Vec<String>, usize) {
    if lines.len() <= head + tail {
        return (lines, 0);
    }
    let omitted = lines.len() - head - tail;
    let mut sampled = Vec::with_capacity(head + tail + 1);
    sampled.extend(lines.iter().take(head).cloned());
    sampled.push(format!(
        "[... {omitted} lines omitted; request output_mode=full for exact output ...]"
    ));
    sampled.extend(lines.iter().skip(lines.len() - tail).cloned());
    (sampled, omitted)
}

/// Builds a conservative compact view. Unknown/error text is preserved.
#[must_use]
pub fn compact(profile: OutputProfile, input: &str, exit_code: Option<i64>) -> CompactOutput {
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
            OutputProfile::Generic
            | OutputProfile::GitLog
            | OutputProfile::GitStatus
            | OutputProfile::Search => false,
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
        if trimmed.is_empty() {
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

    if profile == OutputProfile::CargoTest && passed + failed + ignored > 0 {
        kept.push(format!(
            "tests: {passed} passed, {failed} failed, {ignored} ignored"
        ));
    } else if exit_code.is_some_and(|code| code != 0) && kept.is_empty() && omitted > 0 {
        kept.push(format!(
            "exit {}; {omitted} routine lines omitted",
            exit_code.unwrap_or_default()
        ));
    }

    // Commands whose value is dominated by large enumerations are sampled in compact
    // mode. Exact output is still durable and available with output_mode=full.
    let (kept, sampled_lines) = match profile {
        OutputProfile::GitLog => sample_lines(kept, 30, 0),
        OutputProfile::GitStatus | OutputProfile::Search => sample_lines(kept, 80, 20),
        OutputProfile::CargoTest
        | OutputProfile::CargoBuild
        | OutputProfile::CargoClippy
        | OutputProfile::Pytest => sample_lines(kept, 120, 120),
        OutputProfile::Generic => (kept, 0),
    };
    omitted += sampled_lines;

    let (kept, folded_lines) = fold_repeated_lines(&kept);
    let mut text = kept.join("\n");
    if input.ends_with('\n') && !text.is_empty() {
        text.push('\n');
    }
    let output_bytes = text.len();
    let saved_bytes = raw_bytes.saturating_sub(output_bytes);
    let mut metadata = json!({
        "profile": profile.label(),
        "raw_bytes": raw_bytes,
        "output_bytes": output_bytes,
        "saved_tokens": saved_bytes / 4,
        "full_output_available": true
    });
    if omitted > 0 {
        metadata["omitted_lines"] = json!(omitted);
    }
    if folded_lines > 0 {
        metadata["folded_lines"] = json!(folded_lines);
    }
    CompactOutput {
        output: text,
        metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    #[test]
    fn cargo_success_collapses_large_routine_output() {
        let mut raw = String::from("running 2500 tests\n");
        for index in 0..2500 {
            let _ = writeln!(
                raw,
                "test integration::very_long_regression_case_{index:04}_with_context ... ok"
            );
        }
        raw.push_str(
            "test result: ok. 2500 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n",
        );
        let result = compact(OutputProfile::CargoTest, &raw, Some(0));
        assert!(result.output.len() <= 800);
        assert!(result.output.contains("2500 passed, 0 failed, 0 ignored"));
        assert!(result.metadata["saved_tokens"].as_u64().unwrap_or_default() >= 24_000);
    }

    #[test]
    fn failures_and_locations_survive_compaction() {
        let raw = "Compiling demo\nerror[E0425]: missing value\n --> tests/repro.rs:3:13\n";
        let result = compact(OutputProfile::CargoTest, raw, Some(101));
        assert!(result.output.contains("error[E0425]"));
        assert!(result.output.contains("tests/repro.rs:3:13"));
    }

    #[test]
    fn generic_view_keeps_interaction_and_folds_duplicate_refreshes() {
        let raw = "\x1b[2Kprogress 10%\rprogress 90%\nheartbeat\nheartbeat\nPassword: ";
        let result = compact(OutputProfile::Generic, raw, None);
        assert!(result.output.contains("progress 90%"));
        assert!(result.output.contains("Password:"));
        assert!(result.output.contains("[repeated 1x]"));
    }

    #[test]
    fn git_and_search_enumerations_are_bounded() {
        let search = (0..500)
            .map(|index| format!("src/file.rs:{index}:match"))
            .collect::<Vec<_>>()
            .join("\n");
        let result = compact(OutputProfile::Search, &search, Some(0));
        assert!(result.output.lines().count() <= 101);
        assert_eq!(result.metadata["full_output_available"], true);

        let log = (0..100)
            .map(|index| format!("{index:040x} commit {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let result = compact(OutputProfile::GitLog, &log, Some(0));
        assert!(result.output.lines().count() <= 31);
    }

    #[test]
    fn classifier_covers_high_noise_commands_and_protects_interactive_sessions() {
        assert_eq!(
            classify("cargo test --workspace", false),
            OutputProfile::CargoTest
        );
        assert_eq!(classify("cargo clippy", false), OutputProfile::CargoClippy);
        assert_eq!(classify("git log --oneline", false), OutputProfile::GitLog);
        assert_eq!(
            classify("git status --short", false),
            OutputProfile::GitStatus
        );
        assert_eq!(classify("rg -n error .", false), OutputProfile::Search);
        assert_eq!(classify("cargo test", true), OutputProfile::Generic);
    }
}
