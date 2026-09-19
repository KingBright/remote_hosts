//! Bounded service logging independent of a desktop console.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

pub struct AgentLog {
    path: PathBuf,
    file: Option<File>,
    bytes: u64,
    limit: u64,
    backups: usize,
}
impl AgentLog {
    pub fn open(state_dir: &Path) -> io::Result<Self> {
        Self::with_limit(&state_dir.join("logs"), 4 * 1024 * 1024, 3)
    }
    fn with_limit(dir: &Path, limit: u64, backups: usize) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        if fs::symlink_metadata(dir)?.file_type().is_symlink() {
            return Err(io::Error::other(
                "agent log directory must not be a symlink",
            ));
        }
        let path = dir.join("agent.log");
        let file = Self::append(&path)?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            path,
            file: Some(file),
            bytes,
            limit,
            backups,
        })
    }
    fn append(path: &Path) -> io::Result<File> {
        if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink() || !m.is_file()) {
            return Err(io::Error::other("agent log path must be a regular file"));
        }
        let mut opts = OpenOptions::new();
        opts.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        opts.open(path)
    }
    fn rotate(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush()?;
            drop(file);
        }
        // Close the Windows handle before rename; no unbounded file inventory.
        let old = self.path.with_extension(format!("log.{}", self.backups));
        if old.exists() {
            fs::remove_file(old)?;
        }
        for index in (1..self.backups).rev() {
            let from = self.path.with_extension(format!("log.{index}"));
            if from.exists() {
                fs::rename(from, self.path.with_extension(format!("log.{}", index + 1)))?;
            }
        }
        fs::rename(&self.path, self.path.with_extension("log.1"))?;
        self.file = Some(Self::append(&self.path)?);
        self.bytes = 0;
        Ok(())
    }
}
impl Write for AgentLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.bytes >= self.limit {
            self.rotate()?;
        }
        if self.file.is_none() {
            self.file = Some(Self::append(&self.path)?);
            self.bytes = self.file.as_ref().unwrap().metadata()?.len();
        }
        let length = buf.len().min((self.limit - self.bytes) as usize);
        let written = self.file.as_mut().unwrap().write(&buf[..length])?;
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        if let Some(file) = &mut self.file {
            file.flush()
        } else {
            Ok(())
        }
    }
}

/// GUI subsystem avoids even an initial console flash. Diagnostic CLI commands
/// may attach to their caller, but keep inherited pipes/files for automation.
#[cfg(windows)]
pub fn attach_parent_console() {
    use windows_sys::Win32::{
        Foundation::INVALID_HANDLE_VALUE,
        System::Console::{
            ATTACH_PARENT_PROCESS, AttachConsole, GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE,
            STD_OUTPUT_HANDLE, SetStdHandle,
        },
    };
    // SAFETY: process-wide standard handles are sampled and restored before any
    // logger or worker starts. AttachConsole never creates a new window.
    unsafe {
        let saved = [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE]
            .map(|kind| (kind, GetStdHandle(kind)));
        if AttachConsole(ATTACH_PARENT_PROCESS) != 0 {
            for (kind, handle) in saved {
                if !handle.is_null() && handle != INVALID_HANDLE_VALUE {
                    SetStdHandle(kind, handle);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotating_agent_log_is_bounded_and_latest_bytes_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = AgentLog::with_limit(dir.path(), 64, 3).unwrap();
        for n in 0..100 {
            writeln!(log, "event {n:03} classified retrying recovered").unwrap();
        }
        log.flush().unwrap();
        drop(log);
        let files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(files.len() <= 4);
        assert!(files.iter().all(|f| f.metadata().unwrap().len() <= 64));
        let mut log = AgentLog::with_limit(dir.path(), 64, 3).unwrap();
        writeln!(log, "LATEST").unwrap();
        drop(log);
        assert!(
            fs::read_to_string(dir.path().join("agent.log"))
                .unwrap()
                .contains("LATEST")
        );
    }
    #[cfg(unix)]
    #[test]
    fn service_log_refuses_symlink_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("unrelated");
        fs::write(&target, b"preserve").unwrap();
        std::os::unix::fs::symlink(&target, dir.path().join("agent.log")).unwrap();
        assert!(AgentLog::with_limit(dir.path(), 64, 3).is_err());
        assert_eq!(fs::read(target).unwrap(), b"preserve");
    }
}
