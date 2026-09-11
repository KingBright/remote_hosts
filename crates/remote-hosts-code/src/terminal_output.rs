//! Append-only UTF-8 output. Raw terminal bytes are never written to this log.
//! Pattern redaction is best effort, not a sandbox against hostile shell output.
use anyhow::{Context, Result, ensure};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

pub(crate) const OUTPUT_CAP: usize = 8 * 1024 * 1024;
const REDACTED: &str = "[REDACTED]";
const KEYS: [&str; 6] = [
    "password", "token", "secret", "apikey", "api_key", "api-key",
];

#[derive(Default)]
enum ValueState {
    #[default]
    Text,
    Start,
    Bare,
    Quoted {
        quote: char,
        escaped: bool,
    },
}

/// No value bytes are published before their terminator arrives. Unbounded values
/// are discarded incrementally rather than buffered. Ordinary prompts need no LF.
#[derive(Default)]
struct Credentials {
    state: ValueState,
    tail: String,
    key: bool,
    closing_key_quote: bool,
}
impl Credentials {
    fn push(&mut self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        for c in text.chars() {
            match &mut self.state {
                ValueState::Start => {
                    if c.is_whitespace() {
                        continue;
                    }
                    self.state = if c == '\'' || c == '"' {
                        ValueState::Quoted {
                            quote: c,
                            escaped: false,
                        }
                    } else {
                        ValueState::Bare
                    };
                }
                ValueState::Bare => {
                    if c.is_whitespace() {
                        out.push(c);
                        self.state = ValueState::Text;
                    }
                }
                ValueState::Quoted { quote, escaped } => {
                    if *escaped {
                        *escaped = false;
                    } else if c == '\\' {
                        *escaped = true;
                    } else if c == *quote {
                        self.state = ValueState::Text;
                    }
                }
                ValueState::Text => {
                    out.push(c);
                    if self.key && (c == ':' || c == '=') {
                        out.push_str(REDACTED);
                        self.state = ValueState::Start;
                        self.tail.clear();
                        self.key = false;
                        self.closing_key_quote = false;
                    } else if self.key && c.is_whitespace() {
                        // Whitespace in the key/separator prefix is already safe to publish.
                    } else if self.key && (c == '\'' || c == '"') && !self.closing_key_quote {
                        self.closing_key_quote = true;
                    } else {
                        self.closing_key_quote = false;
                        if c.is_ascii_alphabetic() || c == '_' || c == '-' {
                            self.tail.push(c.to_ascii_lowercase());
                            if self.tail.len() > 8 {
                                self.tail.remove(0);
                            }
                        } else {
                            self.tail.clear();
                        }
                        self.key = KEYS.iter().any(|key| self.tail.ends_with(key));
                    }
                }
            }
        }
        out
    }
}

struct Sanitizer {
    utf8: Vec<u8>,
    secret: String,
    prefix: String,
    credentials: Credentials,
}
impl Sanitizer {
    fn new(secret: String) -> Self {
        Self {
            utf8: Vec::new(),
            secret,
            prefix: String::new(),
            credentials: Credentials::default(),
        }
    }
    fn exact(&mut self, text: &str) -> String {
        if self.secret.is_empty() {
            return text.to_owned();
        }
        let mut out = String::with_capacity(text.len());
        for c in text.chars() {
            self.prefix.push(c);
            while !self.secret.starts_with(&self.prefix) {
                let first = self.prefix.chars().next().expect("nonempty prefix");
                out.push(first);
                self.prefix.drain(..first.len_utf8());
            }
            if self.prefix == self.secret {
                out.push_str(REDACTED);
                self.prefix.clear();
            }
        }
        out
    }
    fn push(&mut self, bytes: &[u8]) -> String {
        self.utf8.extend_from_slice(bytes);
        let mut decoded = String::with_capacity(self.utf8.len());
        let mut consumed = 0;
        while consumed < self.utf8.len() {
            match std::str::from_utf8(&self.utf8[consumed..]) {
                Ok(text) => {
                    decoded.push_str(text);
                    consumed = self.utf8.len();
                }
                Err(error) => {
                    let valid_end = consumed + error.valid_up_to();
                    decoded.push_str(
                        std::str::from_utf8(&self.utf8[consumed..valid_end])
                            .expect("valid UTF-8 prefix"),
                    );
                    consumed = valid_end;
                    match error.error_len() {
                        Some(len) => {
                            decoded.push('\u{fffd}');
                            consumed += len;
                        }
                        None => break, // At most three bytes; never publish a temporary replacement.
                    }
                }
            }
        }
        self.utf8.drain(..consumed);
        let exact = self.exact(&decoded);
        self.credentials.push(&exact)
    }
    fn finish(&mut self) -> String {
        let mut out = String::new();
        if !self.utf8.is_empty() {
            self.utf8.clear();
            let exact = self.exact("\u{fffd}");
            out.push_str(&self.credentials.push(&exact));
        }
        // A proper prefix is not a complete secret. Only flush it at a real EOF,
        // never on a capture limit, I/O failure or forced reader shutdown.
        out.push_str(&self.credentials.push(&std::mem::take(&mut self.prefix)));
        out
    }
}

pub(crate) struct Capture {
    file: File,
    sanitizer: Sanitizer,
    remaining: usize,
    pub written: usize,
    pub truncated: bool,
    pub error: Option<&'static str>,
    pub complete: bool,
}
impl Capture {
    pub fn new(path: &Path, secret: String) -> Result<Self> {
        crate::write_private(path, b"")?;
        let file = File::options().append(true).open(path)?;
        Ok(Self {
            file,
            sanitizer: Sanitizer::new(secret),
            remaining: OUTPUT_CAP,
            written: 0,
            truncated: false,
            error: None,
            complete: false,
        })
    }
    fn append(&mut self, text: &str) {
        let (text, truncated) = crate::files::bounded(text, OUTPUT_CAP - self.written);
        self.truncated |= truncated;
        if self.error.is_none() {
            if self.file.write_all(text.as_bytes()).is_ok() {
                self.written += text.len();
            } else {
                self.error = Some("output_write_failed");
                self.truncated = true;
            }
        }
    }
    pub fn push(&mut self, bytes: &[u8]) {
        if self.complete || self.error.is_some() {
            return;
        }
        let n = bytes.len().min(self.remaining);
        self.remaining -= n;
        self.truncated |= n < bytes.len();
        if n > 0 {
            let safe = self.sanitizer.push(&bytes[..n]);
            self.append(&safe);
        }
    }
    pub fn fail(&mut self, error: &'static str) {
        if !self.complete {
            self.error = Some(error);
            self.truncated = true;
        }
    }
    pub fn finish(&mut self) {
        if self.complete {
            return;
        }
        if !self.truncated && self.error.is_none() {
            let safe = self.sanitizer.finish();
            self.append(&safe);
        }
        if self.file.sync_all().is_err() {
            self.fail("output_sync_failed");
        }
        self.complete = true;
    }
}

/// O(page size) memory and I/O, independent of the log length. The live writer's
/// committed length prevents reading a partially written UTF-8 character.
pub(crate) fn read_page(
    path: &Path,
    cursor: usize,
    max: usize,
    committed: Option<usize>,
) -> Result<(String, usize, bool)> {
    let mut file = File::open(path).context("terminal output unavailable")?;
    let len = committed.unwrap_or(file.metadata()?.len() as usize);
    ensure!(cursor <= len, "invalid output cursor");
    file.seek(SeekFrom::Start(cursor as u64))?;
    let mut bytes = Vec::with_capacity(max + 4);
    file.take((len - cursor).min(max + 4) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.first().is_none_or(|b| b & 0xc0 != 0x80),
        "invalid output cursor: not a UTF-8 boundary"
    );
    let valid = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        Err(error) if error.error_len().is_none() && error.valid_up_to() >= max => {
            std::str::from_utf8(&bytes[..error.valid_up_to()])?
        }
        Err(_) => anyhow::bail!("terminal output is not valid UTF-8; recovery required"),
    };
    let (chunk, _) = crate::files::bounded(valid, max);
    let next = cursor + chunk.len();
    Ok((chunk.to_owned(), next, next < len))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sanitize(bytes: &[u8], step: usize, secret: &str) -> String {
        let mut s = Sanitizer::new(secret.into());
        let mut result = String::new();
        for chunk in bytes.chunks(step) {
            result.push_str(&s.push(chunk));
        }
        result.push_str(&s.finish());
        result
    }
    #[test]
    fn arbitrary_chunk_boundaries_preserve_text_and_redaction() {
        let text = "你好🧪 aababa xyz password = 'a b\\'c'\nTOKEN:xyz\napi_key=hello\nready> ";
        let expected = "你好🧪 a[REDACTED] xyz password =[REDACTED]\nTOKEN:[REDACTED]\napi_key=[REDACTED]\nready> ";
        for step in 1..=text.len() {
            assert_eq!(
                sanitize(text.as_bytes(), step, "ababa"),
                expected,
                "chunk={step}"
            );
        }
    }
    #[test]
    fn incomplete_utf8_is_replaced_only_at_eof() {
        let mut s = Sanitizer::new("never".into());
        assert_eq!(s.push(b"ok\xe4"), "ok");
        assert_eq!(s.push(b"\xbd"), "");
        assert_eq!(s.finish(), "\u{fffd}");
        assert_eq!(sanitize(b"a\xffb", 1, "never"), "a\u{fffd}b");
    }
    #[test]
    fn very_long_unterminated_values_are_not_buffered() {
        let mut s = Sanitizer::new("actual-device-secret".into());
        assert_eq!(s.push(b"password='"), "password=[REDACTED]");
        for _ in 0..128 {
            assert_eq!(s.push(&vec![b'x'; 8192]), "");
        }
        assert_eq!(s.finish(), "");
        assert!(s.utf8.len() <= 3 && s.prefix.len() < s.secret.len());
    }
    #[test]
    fn json_keys_and_overlapping_key_suffixes_are_redacted() {
        for spelling in [
            "password", "TOKEN", "Secret", "apikey", "API_KEY", "api-key",
        ] {
            let text = format!("{spelling}=synthetic-value\n");
            for step in 1..=text.len() {
                assert_eq!(
                    sanitize(text.as_bytes(), step, ""),
                    format!("{spelling}=[REDACTED]\n")
                );
            }
        }
        assert_eq!(
            sanitize(b"{\"token\":\"hello\"} secretoken=world\n", 1, ""),
            "{\"token\":[REDACTED]} secretoken=[REDACTED]\n"
        );
    }
    #[test]
    fn capture_is_private_bounded_and_does_not_store_raw_secrets() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("safe.log");
        let mut c = Capture::new(&path, "synthetic-device-credential".into()).unwrap();
        c.push(b"synthetic-device-");
        assert_eq!(std::fs::read(&path).unwrap(), b"");
        c.push(b"credential\ntoken='hidden value'\n");
        for _ in 0..1025 {
            c.push(&[b'x'; 8192]);
        }
        c.finish();
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() <= OUTPUT_CAP && c.truncated && c.complete);
        assert!(!String::from_utf8(bytes).unwrap().contains("hidden"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
    #[test]
    fn bounded_seek_uses_utf8_cursor_boundaries() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("safe.log");
        std::fs::write(&path, "你🧪abc").unwrap();
        assert_eq!(
            read_page(&path, 0, 4, None).unwrap(),
            ("你".into(), 3, true)
        );
        assert_eq!(
            read_page(&path, 3, 4, None).unwrap(),
            ("🧪".into(), 7, true)
        );
        assert!(read_page(&path, 1, 4, None).is_err());
        assert_eq!(
            read_page(&path, 10, 4, None).unwrap(),
            ("".into(), 10, false)
        );
        assert!(read_page(&path, 11, 4, None).is_err());
    }
}
