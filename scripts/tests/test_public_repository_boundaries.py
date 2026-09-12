"""Public repository must not regress into a deployment-state notebook."""
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]

# Construct known historical instance markers so this guard does not contain the
# exact forbidden strings it is designed to detect with simple source grep.
FORBIDDEN = (
    "hackerlife" + ".fun",
    "/Users/" + "jinliang",
    "MacBook-" + "M2-Max",
    "Mac-" + "Studio",
    "ba3bf113" + "-",
    "8af88e35" + "-",
    "/volume1/docker/" + "remote-hosts-code",
    "root@" + "hackerlife" + ".fun",
)

ACTIVE_DOCS = (
    ROOT / "README.md",
    ROOT / "README_EN.md",
    ROOT / "docs/README.md",
    ROOT / "docs/chatgpt-code-gateway.md",
    ROOT / "docs/code-gateway-deployment.md",
    ROOT / "docs/deployment-and-operations.md",
    ROOT / "docs/repository-content-model.md",
)
ACTIVE_DIRS = (
    ROOT / "crates",
    ROOT / "scripts",
    ROOT / "skills",
    ROOT / "docs/product",
)
TEXT_SUFFIXES = {".md", ".json", ".py", ".rs", ".sh", ".ps1", ".toml", ".caddy", ".service", ""}


def active_files():
    yielded = set()
    for path in ACTIVE_DOCS:
        if path.is_file():
            yielded.add(path)
            yield path
    for directory in ACTIVE_DIRS:
        for path in directory.rglob("*"):
            if path.is_file() and path.suffix in TEXT_SUFFIXES and path not in yielded:
                yield path


class PublicRepositoryBoundaryTests(unittest.TestCase):
    def test_active_public_surfaces_have_no_known_instance_identity(self):
        violations = []
        for path in active_files():
            try:
                text = path.read_text()
            except UnicodeDecodeError:
                continue
            for marker in FORBIDDEN:
                if marker in text:
                    violations.append(f"{path.relative_to(ROOT)} contains deployment marker {marker!r}")
        self.assertEqual(violations, [], "\n".join(violations))

    def test_public_backlog_does_not_store_live_release_targets(self):
        path = ROOT / "docs/product/backlog.json"
        if not path.is_file():
            self.skipTest("minimal release snapshot intentionally excludes public product backlog")
        text = path.read_text()
        for key in ("last_observed_agents", "selected_release_targets", "studio_policy"):
            self.assertNotIn('"' + key + '"', text)
        self.assertIn('"deployment_state_source"', text)

    def test_private_ops_scratch_paths_are_gitignored(self):
        path = ROOT / ".gitignore"
        if not path.is_file():
            self.skipTest("minimal release snapshot intentionally excludes repository ignore policy")
        ignore = path.read_text()
        for value in ("/ops/private/", "/deploy/local/", "/docs/live/", "/.local-deployment/"):
            self.assertIn(value, ignore)


if __name__ == "__main__":
    unittest.main()
