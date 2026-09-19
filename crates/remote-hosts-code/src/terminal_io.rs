//! Bounded Unix PTY I/O. A full input queue must not pin runtime shutdown.
//! Duplicated master descriptors share O_NONBLOCK, so BOTH reader and writer
//! handle readiness explicitly. Cancellation never waits for the writer mutex.
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    poll::{PollFd, PollFlags, poll},
};
use std::{
    fs::File,
    io::{self, Read, Write},
    os::fd::{AsFd, BorrowedFd},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Default)]
pub(crate) struct Control {
    input_closed: Arc<AtomicBool>,
    reader_cancelled: Arc<AtomicBool>,
}
impl Control {
    pub(crate) fn close_input(&self) {
        self.input_closed.store(true, Ordering::Release);
    }
    pub(crate) fn stop_reader(&self) {
        self.reader_cancelled.store(true, Ordering::Release);
    }
}
fn io_error(error: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}
fn ready(file: &File, events: PollFlags, millis: u16) -> io::Result<()> {
    let mut pollfd = [PollFd::new(file.as_fd(), events)];
    match poll(&mut pollfd, millis) {
        Ok(_) => Ok(()),
        Err(nix::errno::Errno::EINTR) => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

pub(crate) struct Reader {
    file: File,
    control: Control,
}
impl Read for Reader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.control.reader_cancelled.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "pty_reader_cancelled_after_drain_deadline",
                ));
            }
            match self.file.read(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    ready(&self.file, PollFlags::POLLIN, 50)?
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => return result,
            }
        }
    }
}
pub(crate) struct Writer {
    file: File,
    control: Control,
}
impl Writer {
    fn write_until(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<usize> {
        loop {
            if self.control.input_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "pty_input_closed",
                ));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "pty_input_timeout_partial_delivery_possible_do_not_replay",
                ));
            }
            match self.file.write(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    ready(
                        &self.file,
                        PollFlags::POLLOUT,
                        remaining.as_millis().clamp(1, 50) as u16,
                    )?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => return result,
            }
        }
    }
    fn write_all_until(&mut self, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
        while !bytes.is_empty() {
            let written = self.write_until(bytes, deadline)?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "pty_input_write_zero",
                ));
            }
            bytes = &bytes[written..];
        }
        Ok(())
    }
}
impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_until(bytes, Instant::now() + Duration::from_secs(3))
    }
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        // One deadline covers the entire request, not a fresh timeout per chunk.
        self.write_all_until(bytes, Instant::now() + Duration::from_secs(3))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn pair(master: BorrowedFd<'_>) -> io::Result<(Reader, Writer, Control)> {
    let flags = OFlag::from_bits_truncate(fcntl(master, FcntlArg::F_GETFL).map_err(io_error)?);
    fcntl(master, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).map_err(io_error)?;
    let control = Control::default();
    let reader = Reader {
        file: File::from(master.try_clone_to_owned()?),
        control: control.clone(),
    };
    let writer = Writer {
        file: File::from(master.try_clone_to_owned()?),
        control: control.clone(),
    };
    Ok((reader, writer, control))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;
    fn socket_pair() -> (UnixStream, Reader, Writer, Control) {
        let (first, other) = UnixStream::pair().unwrap();
        let (reader, writer, control) = pair(first.as_fd()).unwrap();
        drop(first);
        (other, reader, writer, control)
    }
    #[test]
    fn full_input_queue_has_a_request_deadline_and_can_be_cancelled() {
        let (_other, _reader, mut writer, control) = socket_pair();
        let bytes = vec![b'x'; 8 * 1024 * 1024];
        let started = Instant::now();
        let error = writer
            .write_all_until(&bytes, started + Duration::from_millis(40))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(error.to_string().contains("partial_delivery_possible"));
        assert!(started.elapsed() < Duration::from_secs(2));
        control.close_input();
        assert_eq!(
            writer.write(b"x").unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
    #[test]
    fn pending_writer_observes_cancellation_without_locking_input() {
        let (_other, _reader, mut writer, control) = socket_pair();
        let join = std::thread::spawn(move || writer.write_all(&vec![b'x'; 8 * 1024 * 1024]));
        std::thread::sleep(Duration::from_millis(30));
        control.close_input();
        assert_eq!(
            join.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
    #[test]
    fn reader_preserves_bytes_and_can_end_a_detached_descendant_wait() {
        let (mut other, mut reader, _writer, control) = socket_pair();
        other.write_all(b"complete").unwrap();
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"complete");
        control.stop_reader();
        assert_eq!(
            reader.read(&mut bytes).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }
}
