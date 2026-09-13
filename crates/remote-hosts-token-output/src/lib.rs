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
    let command = command.trim();
    // A shell script, pipeline or expansion can emit unrelated text. Do not
    // infer its output type from keywords anywhere in the command string.
    // Deliberately prefer a false negative over interpreting shell syntax.
    if interactive
        || command.chars().any(|c| {
            matches!(
                c,
                '\n' | '\r' | ';' | '|' | '&' | '`' | '$' | '(' | ')' | '{' | '}' | '<' | '>'
            )
        })
    {
        return OutputProfile::Generic;
    }
    let mut words = command.split_whitespace().peekable();
    if words.peek().copied() == Some("env") {
        words.next();
    }
    while words
        .peek()
        .is_some_and(|word| environment_assignment(word))
    {
        words.next();
    }
    let Some(program) = words.next() else {
        return OutputProfile::Generic;
    };
    let executable = program
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(program)
        .to_ascii_lowercase();
    let mut subcommand = words.next().unwrap_or_default();
    match executable.as_str() {
        "cargo" | "cargo.exe" => {
            if subcommand.starts_with('+') {
                subcommand = words.next().unwrap_or_default();
            }
            match subcommand {
                "test" | "nextest" => OutputProfile::CargoTest,
                "build" | "check" => OutputProfile::CargoBuild,
                "clippy" => OutputProfile::CargoClippy,
                _ => OutputProfile::Generic,
            }
        }
        "git" | "git.exe" => match subcommand {
            "log" => OutputProfile::GitLog,
            "status" => OutputProfile::GitStatus,
            _ => OutputProfile::Generic,
        },
        "pytest" | "pytest.exe" => OutputProfile::Pytest,
        "python" | "python3" | "python.exe" | "python3.exe"
            if subcommand == "-m" && words.next() == Some("pytest") =>
        {
            OutputProfile::Pytest
        }
        "rg" | "rg.exe" | "grep" | "grep.exe" => OutputProfile::Search,
        _ => OutputProfile::Generic,
    }
}

fn environment_assignment(word: &str) -> bool {
    let Some((name, _)) = word.split_once('=') else {
        return false;
    };
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
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

fn enumeration_line(profile: OutputProfile, line: &str) -> bool {
    match profile {
        OutputProfile::GitLog => line.split_once(' ').is_some_and(|(hash, _)| {
            (7..=64).contains(&hash.len()) && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        }),
        OutputProfile::GitStatus => {
            let bytes = line.as_bytes();
            bytes.len() > 3
                && bytes[2] == b' '
                && (matches!((bytes[0], bytes[1]), (b'?', b'?') | (b'!', b'!'))
                    || (bytes[..2].iter().all(|byte| b" MADRCUT".contains(byte))
                        && bytes[..2] != [b' ', b' ']))
        }
        OutputProfile::Search => {
            let Some((path, mut rest)) = line.split_once(':') else {
                return false;
            };
            if path.is_empty() {
                return false;
            }
            // A Windows drive letter is part of the path, not a line number.
            if path.len() == 1 && rest.starts_with(['/', '\\']) {
                let Some((_, after_path)) = rest.split_once(':') else {
                    return false;
                };
                rest = after_path;
            }
            rest.split_once(':').is_some_and(|(number, _)| {
                !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
            })
        }
        _ => false,
    }
}

fn sample_enumeration(
    profile: OutputProfile,
    lines: Vec<String>,
    head: usize,
    tail: usize,
) -> (Vec<String>, usize) {
    let count = lines
        .iter()
        .filter(|line| enumeration_line(profile, line))
        .count();
    if count <= head + tail {
        return (lines, 0);
    }
    let mut sampled = Vec::with_capacity(head + tail + 1);
    let mut seen = 0;
    let mut pending = 0;
    let mut omitted = 0;
    let omission = |count| {
        format!("[... {count} lines omitted; request output_mode=full for exact output ...]")
    };
    for line in lines {
        let known = enumeration_line(profile, &line);
        let skip = known && seen >= head && seen < count - tail;
        if known {
            seen += 1;
        }
        if skip {
            pending += 1;
            omitted += 1;
        } else {
            if pending > 0 {
                sampled.push(omission(pending));
                pending = 0;
            }
            sampled.push(line);
        }
    }
    if pending > 0 {
        sampled.push(omission(pending));
    }
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

    // Only successful, positively identified enumeration records may be sampled.
    // Unknown text and diagnostics must survive even between omitted records;
    // source pagination, not semantic head/tail truncation, bounds large errors.
    let (kept, sampled_lines) = match (profile, exit_code) {
        (OutputProfile::GitLog, Some(0)) => sample_enumeration(profile, kept, 30, 0),
        (OutputProfile::GitStatus | OutputProfile::Search, Some(0)) => {
            sample_enumeration(profile, kept, 80, 20)
        }
        _ => (kept, 0),
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

    #[test]
    fn classifier_does_not_treat_arguments_or_compound_scripts_as_commands() {
        for command in [
            "printf 'cargo test'",
            "echo pytest",
            "git log --oneline; cat AGENTS.md",
            "cargo test && cat verification.json",
            "cargo test | cat",
            "cargo test\nprintf 'important instructions'",
            "cargo testhelper",
            "python3 -c 'print(\"pytest\")'",
            "my-pytest-runner",
            "env NOTE=pytest ls",
        ] {
            assert_eq!(
                classify(command, false),
                OutputProfile::Generic,
                "{command}"
            );
        }
    }

    #[test]
    fn classifier_accepts_direct_executables_and_simple_environment_prefixes() {
        for command in [
            "cargo.exe test --workspace",
            "/opt/rust/bin/cargo test",
            "C:\\Rust\\bin\\cargo.exe test",
            "CARGO_TERM_COLOR=never cargo test",
            "env CARGO_TERM_COLOR=never cargo +stable test",
        ] {
            assert_eq!(
                classify(command, false),
                OutputProfile::CargoTest,
                "{command}"
            );
        }
        assert_eq!(
            classify("python3 -m pytest -q", false),
            OutputProfile::Pytest
        );
    }

    #[test]
    fn long_diagnostics_are_not_head_tail_sampled() {
        let mut raw = String::new();
        for index in 0..400 {
            let _ = writeln!(raw, "error[E{index:04}]: unique diagnostic {index}");
            let _ = writeln!(raw, " --> src/case_{index}.rs:17:4");
        }
        for profile in [
            OutputProfile::CargoTest,
            OutputProfile::CargoBuild,
            OutputProfile::CargoClippy,
            OutputProfile::Pytest,
        ] {
            let result = compact(profile, &raw, Some(101));
            assert_eq!(result.output, raw, "{} lost diagnostics", profile.label());
        }
    }

    fn enumeration_fixture(profile: OutputProfile, count: usize) -> String {
        (0..count)
            .map(|index| match profile {
                OutputProfile::GitLog => format!("{index:040x} commit {index}"),
                OutputProfile::GitStatus => format!(" M src/file_{index}.rs"),
                OutputProfile::Search => format!("src/file.rs:{index}:match"),
                _ => unreachable!("enumeration profile required"),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn enumeration_compaction_preserves_unknown_text_and_middle_diagnostics() {
        for profile in [
            OutputProfile::GitLog,
            OutputProfile::GitStatus,
            OutputProfile::Search,
        ] {
            let raw = enumeration_fixture(profile, 300);
            let mut lines: Vec<_> = raw.lines().map(str::to_owned).collect();
            lines.insert(150, "IMPORTANT: keep this instruction verbatim".to_owned());
            lines.insert(
                151,
                "error: recoverable failure at src/critical.rs:42:7".to_owned(),
            );
            let result = compact(profile, &lines.join("\n"), Some(0));
            assert!(
                result
                    .output
                    .contains("IMPORTANT: keep this instruction verbatim")
            );
            assert!(
                result
                    .output
                    .contains("error: recoverable failure at src/critical.rs:42:7")
            );
            assert!(
                result.metadata["omitted_lines"]
                    .as_u64()
                    .unwrap_or_default()
                    > 0
            );
        }
    }

    #[test]
    fn pending_and_failed_enumerations_are_not_sampled() {
        for profile in [
            OutputProfile::GitLog,
            OutputProfile::GitStatus,
            OutputProfile::Search,
        ] {
            let raw = enumeration_fixture(profile, 300);
            for exit_code in [None, Some(1), Some(128)] {
                let result = compact(profile, &raw, exit_code);
                assert_eq!(result.output, raw, "{} {exit_code:?}", profile.label());
            }
        }
    }

    #[test]
    fn unknown_enumeration_formats_are_not_sampled() {
        let raw = (0..300)
            .map(|index| format!("unrecognized format with important detail {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        for profile in [
            OutputProfile::GitLog,
            OutputProfile::GitStatus,
            OutputProfile::Search,
        ] {
            assert_eq!(compact(profile, &raw, Some(0)).output, raw);
        }
    }
}
