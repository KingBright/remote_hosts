"""Reuse the existing mixed Android/desktop gates in mainline Python discovery."""
import importlib.util
import pathlib
import sys


def load_tests(loader, tests, pattern):
    path = pathlib.Path(__file__).resolve().parents[1]/'test-android-fleet-scope.py'
    spec = importlib.util.spec_from_file_location('android_mainline_fleet_scope', path)
    module = importlib.util.module_from_spec(spec)
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(path.parent))
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous
    tests.addTests(loader.loadTestsFromModule(module))
    return tests
