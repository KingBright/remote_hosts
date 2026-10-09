"""Bounded SSH diagnostics; credentials remain in the owner's SSH client."""
import threading

PATTERNS = (
    ("host_identity_verification_failed", (b"Host key verification failed", b"REMOTE HOST IDENTIFICATION HAS CHANGED")),
    ("authentication_rejected", (b"Permission denied (", b"Too many authentication failures")),
    ("dns_resolution_failed", (b"Could not resolve hostname",)),
    ("connection_timed_out", (b"Connection timed out", b"Operation timed out")),
    ("connection_refused", (b"Connection refused",)),
    ("network_unreachable", (b"Network is unreachable", b"No route to host")),
    ("algorithm_negotiation_failed", (b"no matching host key type", b"no matching key exchange method", b"no matching cipher")),
    ("connection_closed", (b"Connection closed", b"Connection reset", b"closed by remote host")),
)


def classify(stderr, returncode):
    categories = [name for name, patterns in PATTERNS if any(item in stderr for item in patterns)]
    category = ("connected" if returncode == 0 else "interrupted_by_signal" if returncode < 0
                else categories[0] if categories else "ssh_failed_unclassified")
    return {"exit_code": returncode, "category": category, "matched_categories": categories,
            "raw_stderr_retained": False, "password_prompt_capture": False}


class StderrCodes:
    """Drain inherited SSH stderr, preserving only fixed codes and a short memory tail."""
    def __init__(self, pipe):
        self.pipe = pipe
        self.categories = set()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()

    def read(self):
        tail = b""
        try:
            for block in iter(lambda: self.pipe.read1(1024), b""):
                value = tail + block
                matches = [name for name, patterns in PATTERNS if any(item in value for item in patterns)]
                with self.lock:
                    self.categories.update(matches)
                tail = value[-256:]
        except (OSError, ValueError):
            pass
        finally:
            tail = b""

    def result(self, returncode):
        if returncode != 0:
            self.thread.join(timeout=1)
        with self.lock:
            categories = [name for name, _ in PATTERNS if name in self.categories]
        category = ("connected" if returncode == 0 else "interrupted_by_signal" if returncode < 0
                    else categories[0] if categories else "ssh_failed_unclassified")
        return {"exit_code": returncode, "category": category, "matched_categories": categories,
                "raw_stderr_retained": False, "password_prompt_capture": False}
