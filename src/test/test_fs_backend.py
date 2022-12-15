"""
Unit tests for filesystem-based backend implementation.
"""

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
from deepdiff import DeepDiff

from ..server.backend.initialize import initialize_backend_store
from ..server.models import Result

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------


def equal(a: Result, b: Result) -> bool:
    """Compare results for equality."""
    return not DeepDiff(a.to_json(), b.to_json())


def result_from(
    identifier: str, tag: str, versions: List[Tuple[int, Dict[str, Any]]]
) -> Result:
    """Generate a result with arbitrary data."""
    return Result.from_json(
        {
            "identifier": identifier,
            "tag": tag,
            "versions": [{"version": e[0], "data": e[1]} for e in versions],
        }
    )


# -----------------------------------------------------------------------------
# Test Cases
# -----------------------------------------------------------------------------


def test_initialize(tmp_path: Path):
    _ = initialize_backend_store(f"file://{tmp_path.as_posix()}")
    assert True


def test_write(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    written = store.write_result(
        "m0", "v0", result_from("r0", "", [(0, {"0": "0"})])
    )
    assert written == 1


def test_write_read(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    r0 = result_from("r0", "", [(0, {"hello": "world"})])
    written = store.write_result("m0", "v0", r0)
    assert written == 1

    r1 = store.read_result("m0", "v0", "r0")
    assert equal(r0, r1)


def test_write_read_latest(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    store.write_result("m0", "v0", result_from("r0", "", [(0, {"1": "1"})]))

    e = result_from("r0", "", [(0, {"2": "2"})])
    store.write_result("m0", "v0", e)

    r = store.read_result("m0", "v0", "r0")
    assert equal(r, e)


def test_write_read_version(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    v0 = result_from("r0", "", [(0, {"0": "0"})])
    store.write_result("m0", "v0", v0)

    v1 = result_from("r0", "", [(0, {"1": "1"})])
    store.write_result("m0", "v0", v1)

    v2 = result_from("r0", "", [(0, {"2": "2"})])
    store.write_result("m0", "v0", v2)

    for vid, exp in zip([0, 1, 2], [v0, v1, v2]):
        r = store.read_result("m0", "v0", "r0", vid)
        assert equal(r, exp)


def test_write_read_bad_version(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))

    with pytest.raises(RuntimeError):
        store.read_result("m0", "v0", "r0", 2)


def test_read_nonexistent_model(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")
    with pytest.raises(RuntimeError):
        _ = store.read_result("fakemodel", "fakeversion", "fakeresult")


def test_write_delete_result_version(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    _ = store.read_result("m0", "v0", "r0")

    store.delete_result_version("m0", "v0", "r0", 0)

    # Reading exact version should fail
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0", 0)

    # Reading latest should fail
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0")


def test_write_delete_result(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    _ = store.read_result("m0", "v0", "r0")

    store.delete_result("m0", "v0", "r0")

    # Reading latest should fail
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0")


def test_delete_results(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    store.write_result("m0", "v0", result_from("r1", "", [(0, {"1": "1"})]))

    store.delete_results("m0", "v0")

    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0")
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r1")


def test_delete_results_with_tag(tmp_path: Path):
    store = initialize_backend_store(f"file://{tmp_path.as_posix()}")

    store.write_result("m0", "v0", result_from("r0", "t0", [(0, {"0": "0"})]))
    store.write_result("m0", "v0", result_from("r1", "t0", [(0, {"1": "1"})]))
    store.write_result("m0", "v0", result_from("r2", "", [(0, {"2": "2"})]))

    store.delete_results("m0", "v0", "t0")

    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0")
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r1")

    _ = store.read_result("m0", "v0", "r2")
