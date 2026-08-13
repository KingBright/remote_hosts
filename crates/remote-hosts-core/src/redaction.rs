//! Output redaction utilities.

use regex::Regex;

/// Maximum characters retained in one human-readable command preview.
pub const COMMAND_PREVIEW_MAX_CHARS: usize = 1_200;
/// Maximum lines retained in one human-readable command preview.
pub const COMMAND_PREVIEW_MAX_LINES: usize = 12;

/// Secret redactor for logs and agent-visible summaries.
#[derive(Debug)]
pub struct SecretRedactor {
    patterns: Vec<(Regex, &'static str)>,
}

impl SecretRedactor {
    /// Builds the default redactor.
    ///
    /// # Errors
    ///
    /// Returns a regex compilation error if one of the built-in patterns is invalid.
    pub fn new() -> Result<Self, regex::Error> {
        let patterns = vec![
            (
                Regex::new(
                    r#"(?i)(password|passwd|pwd|token|secret|api[_-]?key|private[_-]?key)\s*[:=]\s*("[^"]+"|'[^']+'|\S+)"#,
                )?,
                "$1=<redacted>",
            ),
            (
                Regex::new(r#"(?i)(\bsshpass\s+-p\s+)("[^"]+"|'[^']+'|\S+)"#)?,
                "$1<redacted>",
            ),
            (
                Regex::new(
                    r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----",
                )?,
                "<redacted>",
            ),
        ];

        Ok(Self { patterns })
    }

    /// Redacts sensitive values in an output string.
    pub fn redact(&self, input: &str) -> String {
        self.patterns
            .iter()
            .fold(input.to_owned(), |current, (regex, replacement)| {
                regex.replace_all(&current, *replacement).to_string()
            })
    }

    /// Produces a bounded, redacted preview suitable for activity feeds and agent responses.
    #[must_use]
    pub fn command_preview(&self, input: &str) -> String {
        bounded_preview(
            &self.redact(input),
            COMMAND_PREVIEW_MAX_CHARS,
            COMMAND_PREVIEW_MAX_LINES,
        )
    }
}

fn bounded_preview(input: &str, max_chars: usize, max_lines: usize) -> String {
    let mut output = String::new();
    let mut chars = 0_usize;
    let mut truncated = false;

    for (line_index, line) in input.lines().enumerate() {
        if line_index == max_lines {
            truncated = true;
            break;
        }
        if line_index > 0 {
            output.push('\n');
            chars += 1;
        }
        for character in line.chars() {
            if chars == max_chars {
                truncated = true;
                break;
            }
            output.push(character);
            chars += 1;
        }
        if truncated {
            break;
        }
    }
    if input.ends_with('\n') && !truncated && chars < max_chars {
        output.push('\n');
    }
    if truncated {
        output.push_str("\n... <truncated>");
    }
    output
}

impl Default for SecretRedactor {
    fn default() -> Self {
        Self::new().unwrap_or_else(|error| {
            tracing::error!(%error, "failed to build secret redactor; falling back to no patterns");
            Self {
                patterns: Vec::new(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SecretRedactor;

    #[test]
    fn redacts_password_assignment() -> Result<(), regex::Error> {
        let redactor = SecretRedactor::new()?;
        let output = redactor.redact("SSH_PASSWORD=hunter2");
        assert_eq!(output, "SSH_PASSWORD=<redacted>");
        Ok(())
    }

    #[test]
    fn redacts_sshpass_inline_password() -> Result<(), regex::Error> {
        let redactor = SecretRedactor::new()?;
        let output = redactor.redact(
            "sshpass -p super-secret-value ssh -o StrictHostKeyChecking=no user@example.test",
        );
        assert_eq!(
            output,
            "sshpass -p <redacted> ssh -o StrictHostKeyChecking=no user@example.test"
        );
        Ok(())
    }

    #[test]
    fn redacts_sshpass_long_inline_password() -> Result<(), regex::Error> {
        let redactor = SecretRedactor::new()?;
        let output = redactor.redact("sshpass --password='quoted secret' ssh user@example.test");
        assert_eq!(
            output,
            "sshpass --password=<redacted> ssh user@example.test"
        );
        Ok(())
    }

    #[test]
    fn redacts_private_key_block() -> Result<(), regex::Error> {
        let redactor = SecretRedactor::new()?;
        let output = redactor
            .redact("-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----");
        assert!(output.contains("<redacted>"));
        Ok(())
    }

    #[test]
    fn command_preview_is_readable_redacted_and_bounded() -> Result<(), regex::Error> {
        let redactor = SecretRedactor::new()?;
        let input = format!(
            "set -e\nPASSWORD=hunter2\nkubectl get pods\n{}",
            "echo line\n".repeat(40)
        );
        let preview = redactor.command_preview(&input);
        assert!(preview.contains("kubectl get pods"));
        assert!(preview.contains("PASSWORD=<redacted>"));
        assert!(!preview.contains("hunter2"));
        assert!(preview.contains("<truncated>"));
        assert!(preview.lines().count() <= super::COMMAND_PREVIEW_MAX_LINES + 1);
        Ok(())
    }
}
