"""
Unit tests for backend implementation.
"""

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
from deepdiff import DeepDiff

from ..server.backend.initialize import initialize_backend_store
from ..server.backend.models import Result
from .support.backend import TestDefinition
from .support.backend.fs import (
    construct_uri,
    create_temporary_directory,
    delete_temporary_directory,
)

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

DEFINITIONS = [
    TestDefinition(
        "fs",
        "artifact:uri",
        {},
        [create_temporary_directory, construct_uri],
        [delete_temporary_directory],
    )
]


@pytest.fixture()
def backend(request):
    """A fixture to perform setup and teardown."""
    d: TestDefinition = request.param
    try:
        d.setup()
        yield d
    finally:
        d.teardown()


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_initialize(backend):
    d: TestDefinition = backend
    _ = initialize_backend_store(d.uri, d.environment)
    assert True


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_write(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    written = store.write_result(
        "m0", "v0", result_from("r0", "", [(0, {"0": "0"})])
    )
    assert written == 1


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_write_read(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    r0 = result_from("r0", "", [(0, {"hello": "world"})])
    written = store.write_result("m0", "v0", r0)
    assert written == 1

    r1 = store.read_result("m0", "v0", "r0")
    assert equal(r0, r1)


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_write_read_latest(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    store.write_result("m0", "v0", result_from("r0", "", [(0, {"1": "1"})]))

    e = result_from("r0", "", [(0, {"2": "2"})])
    store.write_result("m0", "v0", e)

    r = store.read_result("m0", "v0", "r0")
    assert equal(r, e)


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_write_read_version(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    v0 = result_from("r0", "", [(0, {"0": "0"})])
    store.write_result("m0", "v0", v0)

    v1 = result_from("r0", "", [(0, {"1": "1"})])
    store.write_result("m0", "v0", v1)

    v2 = result_from("r0", "", [(0, {"2": "2"})])
    store.write_result("m0", "v0", v2)

    for vid, exp in zip([0, 1, 2], [v0, v1, v2]):
        r = store.read_result("m0", "v0", "r0", vid)
        assert equal(r, exp)


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_write_read_bad_version(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))

    with pytest.raises(RuntimeError):
        store.read_result("m0", "v0", "r0", 2)


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_read_nonexistent_model(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    with pytest.raises(RuntimeError):
        _ = store.read_result("fakemodel", "fakeversion", "fakeresult")


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_write_delete_result_version(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    _ = store.read_result("m0", "v0", "r0")

    store.delete_result_version("m0", "v0", "r0", 0)

    # Reading exact version should fail
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0", 0)

    # Reading latest should fail
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0")


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_write_delete_result(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    _ = store.read_result("m0", "v0", "r0")

    store.delete_result("m0", "v0", "r0")

    # Reading latest should fail
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0")


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_delete_results(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    store.write_result("m0", "v0", result_from("r0", "", [(0, {"0": "0"})]))
    store.write_result("m0", "v0", result_from("r1", "", [(0, {"1": "1"})]))

    store.delete_results("m0", "v0")

    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0")
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r1")


@pytest.mark.parametrize("backend", deepcopy(DEFINITIONS), indirect=["backend"])
def test_delete_results_with_tag(backend):
    d: TestDefinition = backend
    store = initialize_backend_store(d.uri, d.environment)

    store.write_result("m0", "v0", result_from("r0", "t0", [(0, {"0": "0"})]))
    store.write_result("m0", "v0", result_from("r1", "t0", [(0, {"1": "1"})]))
    store.write_result("m0", "v0", result_from("r2", "", [(0, {"2": "2"})]))

    store.delete_results("m0", "v0", "t0")

    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r0")
    with pytest.raises(RuntimeError):
        _ = store.read_result("m0", "v0", "r1")

    _ = store.read_result("m0", "v0", "r2")
