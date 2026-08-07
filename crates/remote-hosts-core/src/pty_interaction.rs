//! Detects generic, agent-visible interactive prompts from redacted PTY output.

use std::sync::LazyLock;

use regex::Regex;
use remote_hosts_domain::{PtyInteraction, PtyInteractionKind};
use time::OffsetDateTime;

/// Detects a live interactive prompt from the recent redacted PTY output tail.
#[must_use]
pub fn detect_pty_interaction(text: &str, observed_at: OffsetDateTime) -> Option<PtyInteraction> {
    let trimmed = text.trim_end();
    let (kind, confidence) = if host_key_confirmation().is_some_and(|regex| regex.is_match(trimmed))
    {
        (PtyInteractionKind::HostKeyConfirmation, 100)
    } else if sudo_password().is_some_and(|regex| regex.is_match(trimmed)) {
        (PtyInteractionKind::SudoPassword, 100)
    } else if password().is_some_and(|regex| regex.is_match(trimmed)) {
        (PtyInteractionKind::Password, 92)
    } else if confirmation().is_some_and(|regex| regex.is_match(trimmed)) {
        (PtyInteractionKind::Confirmation, 92)
    } else if pager().is_some_and(|regex| regex.is_match(trimmed)) {
        (PtyInteractionKind::Pager, 100)
    } else if selection_menu().is_some_and(|regex| regex.is_match(trimmed)) {
        (PtyInteractionKind::SelectionMenu, 84)
    } else {
        return None;
    };

    Some(PtyInteraction {
        kind,
        confidence,
        observed_at,
    })
}

fn host_key_confirmation() -> Option<&'static Regex> {
    static PATTERN: LazyLock<Option<Regex>> = LazyLock::new(|| {
        compile_pattern(r"(?i)are you sure you want to continue connecting[^\n]*\?$")
    });
    PATTERN.as_ref()
}

fn sudo_password() -> Option<&'static Regex> {
    static PATTERN: LazyLock<Option<Regex>> =
        LazyLock::new(|| compile_pattern(r"(?i)\[sudo\]\s+password for [^:\n]{1,160}:$"));
    PATTERN.as_ref()
}

fn password() -> Option<&'static Regex> {
    static PATTERN: LazyLock<Option<Regex>> = LazyLock::new(|| {
        compile_pattern(r"(?i)(?:password|passphrase)(?:\s+for\s+[^:\n]{1,160})?\s*:$")
    });
    PATTERN.as_ref()
}

fn confirmation() -> Option<&'static Regex> {
    static PATTERN: LazyLock<Option<Regex>> = LazyLock::new(|| {
        compile_pattern(r"(?i)(?:\[(?:y/n|yes/no|y/N|Y/n|Y/N)\]|\((?:y/n|yes/no)\))$")
    });
    PATTERN.as_ref()
}

fn pager() -> Option<&'static Regex> {
    static PATTERN: LazyLock<Option<Regex>> = LazyLock::new(|| {
        compile_pattern(r"(?i)(?:--more--|\(end\)|press (?:any key|enter) to continue)$")
    });
    PATTERN.as_ref()
}

fn selection_menu() -> Option<&'static Regex> {
    static PATTERN: LazyLock<Option<Regex>> = LazyLock::new(|| {
        compile_pattern(
            r"(?i)(?:select|choose|enter)\s+(?:an?\s+)?(?:option|choice|number)\s*[:>]$",
        )
    });
    PATTERN.as_ref()
}

fn compile_pattern(pattern: &str) -> Option<Regex> {
    Regex::new(pattern)
        .map_err(|error| tracing::error!(%error, %pattern, "PTY interaction regex is invalid"))
        .ok()
}

#[cfg(test)]
mod tests {
    use remote_hosts_domain::PtyInteractionKind;
    use time::OffsetDateTime;

    use super::detect_pty_interaction;

    #[test]
    fn detects_common_input_prompts_without_matching_output_mentions() {
        let now = OffsetDateTime::now_utc();
        let cases = [
            (
                "[sudo] password for ops: ",
                PtyInteractionKind::SudoPassword,
            ),
            (
                "Are you sure you want to continue connecting (yes/no/[fingerprint])? ",
                PtyInteractionKind::HostKeyConfirmation,
            ),
            (
                "Proceed with deployment? [y/N] ",
                PtyInteractionKind::Confirmation,
            ),
            ("--More--", PtyInteractionKind::Pager),
            ("Select an option: ", PtyInteractionKind::SelectionMenu),
        ];

        for (text, expected) in cases {
            let interaction = detect_pty_interaction(text, now);
            assert_eq!(
                interaction.as_ref().map(|interaction| &interaction.kind),
                Some(&expected),
                "expected interaction for {text:?}"
            );
            if let Some(interaction) = interaction {
                assert_eq!(interaction.observed_at, now);
            }
        }

        assert!(detect_pty_interaction("documentation mentions password prompts", now).is_none());
        assert!(detect_pty_interaction("deployment completed successfully", now).is_none());
    }
}
