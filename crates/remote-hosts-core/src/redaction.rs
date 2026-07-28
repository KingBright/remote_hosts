//! Output redaction utilities.

use regex::Regex;

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
}
