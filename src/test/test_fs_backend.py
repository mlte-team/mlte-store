"""
Unit tests for filesystem-based backend implementation.
"""

from pathlib import Path
from typing import Any, Dict

import pytest
from deepdiff import DeepDiff

from ..server.backend.backend import Result
from ..server.backend.initialize import initialize_backend_store

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------


def equal(a: Result, b: Result) -> bool:
    """Compare results for equality."""
    return not DeepDiff(a.data, b.data, ignore_order=True)


def result_from(data: Dict[str, Any]) -> Result:
    """Generate a result with arbitrary data."""
    return Result(data=data)


# -----------------------------------------------------------------------------
# Test Cases
# -----------------------------------------------------------------------------


def test_initialize(tmp_path: Path):
    _ = initialize_backend_store(f"file://{tmp_path.as_posix()}")
    assert True


def test_write(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    r0 = result_from({"hello": "world"})
    store.write_result("m0", "v0", "r0", r0)


def test_write_read(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    r0 = result_from({"hello": "world"})
    store.write_result("m0", "v0", "r0", r0)

    r1 = store.read_result("m0", "v0", "r0")
    assert equal(r0, r1)


def test_write_read_latest(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    store.write_result("m0", "v0", "r0", result_from({"0": "0"}))
    store.write_result("m0", "v0", "r0", result_from({"1": "1"}))
    store.write_result("m0", "v0", "r0", result_from({"2": "2"}))

    e = result_from({"3": "3"})
    store.write_result("m0", "v0", "r0", e)

    r = store.read_result("m0", "v0", "r0")
    assert equal(r, e)


def test_read_nonexistent_model(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")
    with pytest.raises(RuntimeError):
        _ = store.read_result("fakemodel", "fakeversion", "fakeresult")
