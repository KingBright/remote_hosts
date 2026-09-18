//! Deterministic Rust libtest evidence extraction. Test execution is not business acceptance.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, BufReader},
    path::Path,
};
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Tests {
    pub framework: String,
    pub summaries: u64,
    pub selected: u64,
    pub executed: u64,
    pub passed: u64,
    pub failed: u64,
    pub ignored: u64,
    pub filtered_out: u64,
    pub failed_cases: Vec<String>,
    pub output_complete: bool,
    pub evidence_complete: bool,
}
impl Tests {
    fn line(&mut self, line: &str) -> Result<()> {
        if let Some(result) = line.strip_prefix("test result: ") {
            let (_, counts) = result.split_once(". ").context("malformed_test_summary")?;
            let mut passed = None;
            let mut failed = None;
            let mut ignored = None;
            for field in counts.split(';') {
                let mut words = field.split_whitespace();
                let Some(n) = words.next() else {
                    continue;
                };
                let Ok(n) = n.parse::<u64>() else {
                    continue;
                };
                match words.next() {
                    Some("passed") => passed = Some(n),
                    Some("failed") => failed = Some(n),
                    Some("ignored") => ignored = Some(n),
                    Some("filtered") => self.filtered_out = self.filtered_out.saturating_add(n),
                    _ => {}
                }
            }
            let (passed, failed, ignored) = (
                passed.context("missing_pass_count")?,
                failed.context("missing_fail_count")?,
                ignored.context("missing_ignore_count")?,
            );
            self.summaries += 1;
            self.passed = self.passed.saturating_add(passed);
            self.failed = self.failed.saturating_add(failed);
            self.ignored = self.ignored.saturating_add(ignored);
        } else if let Some(test) = line
            .strip_prefix("test ")
            .and_then(|s| s.strip_suffix(" ... FAILED"))
            && self.failed_cases.len() < 1000
        {
            self.failed_cases.push(test.chars().take(256).collect());
        }
        Ok(())
    }
    fn finish(&mut self, exit_success: bool) {
        self.executed = self.passed.saturating_add(self.failed);
        self.selected = self.executed.saturating_add(self.ignored);
        self.evidence_complete = self.output_complete
            && exit_success
            && self.summaries > 0
            && self.executed > 0
            && self.failed == 0
            && self.failed_cases.is_empty();
    }
}
pub fn from_logs(
    stdout: &Path,
    stderr: &Path,
    output_complete: bool,
    exit_success: bool,
) -> Result<Tests> {
    let mut tests = Tests {
        framework: "rust-libtest".into(),
        output_complete,
        ..Default::default()
    };
    for path in [stdout, stderr] {
        ensure!(
            std::fs::metadata(path)?.len() <= 32 * 1024 * 1024,
            "test_output_exceeds_capture_budget"
        );
        for line in BufReader::new(std::fs::File::open(path)?).lines() {
            tests.line(&line?)?;
        }
    }
    tests.finish(exit_success);
    Ok(tests)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn parse(text: &str, complete: bool, success: bool) -> Tests {
        let mut tests = Tests {
            framework: "rust-libtest".into(),
            output_complete: complete,
            ..Default::default()
        };
        for line in text.lines() {
            tests.line(line).unwrap();
        }
        tests.finish(success);
        tests
    }
    #[test]
    fn zero_or_ignored_suites_are_not_verification() {
        for text in [
            "",
            "test result: ok. 0 passed; 0 failed; 0 ignored;",
            "test result: ok. 0 passed; 0 failed; 7 ignored;",
        ] {
            let t = parse(text, true, true);
            assert!(!t.evidence_complete);
            assert_eq!(t.executed, 0);
        }
    }
    #[test]
    fn exit_zero_cannot_hide_failed_case_or_incomplete_output() {
        assert!(
            !parse(
                "test result: FAILED. 2 passed; 1 failed; 0 ignored;",
                true,
                true
            )
            .evidence_complete
        );
        assert!(
            !parse(
                "test result: ok. 2 passed; 0 failed; 0 ignored;",
                false,
                true
            )
            .evidence_complete
        );
        assert!(
            !parse(
                "test fabricated ... FAILED\ntest result: ok. 2 passed; 0 failed; 0 ignored;",
                true,
                true
            )
            .evidence_complete
        );
    }
    #[test]
    fn counts_are_separate_and_positive_suites_pass() {
        let t = parse(
            "test result: ok. 2 passed; 0 failed; 3 ignored; 0 measured; 4 filtered out;\ntest result: ok. 1 passed; 0 failed; 0 ignored;",
            true,
            true,
        );
        assert_eq!(
            (t.selected, t.executed, t.passed, t.ignored, t.filtered_out),
            (6, 3, 3, 3, 4)
        );
        assert!(t.evidence_complete);
    }
}
