"""
Test the HTTP interface for storage server(s).
"""

from copy import deepcopy

import pytest

from .support.http import TestDefinition, get
from .support.http.fs import (
    create_store_uri,
    create_temporary_directory,
    delete_temporary_directory,
)

"""
This list contains the global collection of test definitions that
are executed for each test. Adding a new backend consists of 
adding a new TestDefinition to this collection as appropriate.
"""
DEFINITIONS = [
    TestDefinition(
        "fs",
        ["--backend-store-uri", "artifact:uri"],
        {},
        [create_temporary_directory, create_store_uri],
        [delete_temporary_directory],
    )
]


@pytest.fixture()
def server(request):
    """A fixture to perform setup and teardown."""
    d: TestDefinition = request.param
    try:
        d.setup()
        yield d
    finally:
        d.teardown()


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_init(server):
    """Ensure that server can initialize."""
    d: TestDefinition = server
    d.start()

    res = get("/healthcheck")
    assert res.status_code == 200
