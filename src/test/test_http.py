"""
Test the HTTP interface for storage server(s).
"""

import os
import subprocess
import sys
import requests
from requests.exceptions import ConnectionError
from pathlib import Path
from typing import Any, Callable, Dict, List

import pytest

from ..server.backend import BackendStore
from .support_fs import (
    create_store_uri,
    create_temporary_directory,
    delete_temporary_directory,
)

# The host on which the server runs for tests
SERVER_HOST = "localhost"
# The port on which the server listens
SERVER_PORT = 8080 

def get(route: str):
    """Perform a GET request on `route`."""
    return requests.get(f"http://{SERVER_HOST}:{SERVER_PORT}{route}")

class TestConfig:
    """Encapsulates all data required to parametrize a test."""

    def __init__(
        self,
        args: List[str],
        environment: Dict[str, Any],
        setup_fns: List[Callable[[Dict[str, Any]], None]],
        teardown_fns: List[Callable[[Dict[str, Any]], None]],
    ):
        # Commandline arguments passed to the server on startup
        self.args = args
        # The environment provided during server initialization
        self.environment = environment
        # The collection of setup callbacks
        self.setup_fns = setup_fns
        # The collection of teardown callbacks
        self.teardown_fns = teardown_fns
        # A key/value store into which initialization can emit artifacts
        self.artifacts = {}

        # The python executable
        self.interpreter = os.path.abspath(sys.executable)
        # The path to the server entrypoint
        self.program = self._find_program()

    def setup(self) -> None:
        """Run setup functions."""
        for fn in self.setup_fns:
            fn(self.artifacts)

    def teardown(self):
        """Run teardown functions."""
        for fn in self.teardown_fns:
            fn(self.artifacts)

    def start(self) -> BackendStore:
        """Start storage server process."""
        # Process arguments with information from k/v
        cmd = [
            self.interpreter,
            self.program,
            *self._process_arguments(self.args),
        ]
        # Construct subprocess environment
        env = {**os.environ.copy(), **self.environment}

        # Start the server process
        process = subprocess.Popen(
            cmd, stdout=None, stderr=None, stdin=None, env=env
        )

        exitcode = process.poll()
        if exitcode is not None:
            raise RuntimeError(f"Failed to start server process: {exitcode}")

        while True:
            try:
                _ = get("/healthcheck")
            except ConnectionError:
                continue
            break


        # Defer termination of the server process
        self.teardown_fns.insert(0, lambda _: process.kill())

    def _process_arguments(self, args: List[str]) -> List[str]:
        """Process arguments with textual replacement."""
        results = []
        for arg in args:
            if not arg.startswith("artifact:"):
                results.append(arg)
                continue

            assert len(arg.split(":")) == 2, "Corrupt argument."

            key = arg.split(":")[1]
            if key not in self.artifacts:
                raise RuntimeError(f"Artifact with key {key} not found.")
            results.append(self.artifacts[key])

        assert len(results) == len(args), "Broken postcondition."
        return results

    def _find_program(self) -> str:
        """Locate the storage server application entry."""
        path = Path(__file__).parent.parent / "server" / "main.py"
        assert path.exists(), "Failed to locate storage server program."
        return path.resolve().as_posix()



CONFIGS = [
    TestConfig(
        ["--backend-store-uri", "artifact:uri"],
        {},
        [create_temporary_directory, create_store_uri],
        [delete_temporary_directory],
    )
]



@pytest.mark.parametrize("config", CONFIGS)
def test_init(config: TestConfig):
    """Ensure that server can initialize."""
    config.setup()
    try:
        config.start()
        input("enter to continue")
        # res = get("/healthcheck")
        # assert res.status_code == 200
    finally:
        config.teardown()
