"""
Implementation of the filesystem-based backend store.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .backend import BackendStore, BackendStoreURI, Result

# A sentinel value to indicate that the latest version should be read
LATEST_VERSION = -1

"""
The overall structure for the directory hierarchy looks like:

root/
  model_identifier0/
    model_version0/
      result_identifier0.json

The data for an individual result is then stored within a JSON file.
The structure of this JSON file looks like:

{
    "tag": "...",
    "versions": {
        "0": {},
        "1": {}
        ...
    }
}
"""


def _parse_root_path(uri: BackendStoreURI) -> Path:
    """
    Parse the root path for the backend from the URI.
    :param uri: The URI
    :type uri: BackendStoreURI
    :return: The parsed path
    :rtype: Path
    """
    assert uri.uri.startswith("file://"), "Broken precondition."
    return Path(uri.uri[len("file://") :])


def _available_results(version_path: Path) -> List[Path]:
    """
    Get all available results for a particular model version.
    :param version_path: The path to model version directory
    :type version_path: Path
    :return: A collection of paths for all results
    :rtype: List[Path]
    """
    return [x for x in version_path.glob("*") if x.is_file()]


def _available_versions(result_path: Path) -> Set[int]:
    """
    Get the available versions for a result.
    :param result_path: The path to the result
    :type result_path: Path
    :return: The available versions for the result
    :rtype: Set[int]
    """
    with open(result_path.as_posix(), "r") as f:
        document = json.load(f)
        versions = dict(document["versions"])
        return set(int(k) for k in versions.keys())


def _read_tag(result_path: Path) -> str:
    """
    Read the tag for an individual result.
    :param result_path: The path to the result
    :type result_path: Path
    :return: The tag
    :rtype: str
    """
    with result_path.open("r") as f:
        document = json.load(f)
        return document["tag"]


def _read_result(result_path: Path, version: int) -> Result:
    """
    Read the data for an individual result.
    :param result_path: The path to the result
    :type result_path: Path
    :param version: The version identifier
    :type version: int
    :return: The read result
    :rtype: Result
    """
    with result_path.open("r") as f:
        document = json.load(f)
        versions = dict(document["versions"])
        # Compute the latest version, if necessary
        version = (
            max(int(k) for k in versions.keys())
            if version == LATEST_VERSION
            else version
        )
        assert version in set(
            int(k) for k in versions.keys()
        ), "Broken invariant."
        return Result.from_json(versions[f"{version}"])


def _write_result(result_path: Path, result: Result, tag: Optional[str]):
    """
    Write a result to the file at `result_path`.
    :param result_path: The path to the result
    :type result_path: Path
    :param result: The result
    :type result: Result
    """
    if result_path.exists():
        new_version = max(_available_versions(result_path)) + 1

        # Read existing document
        with result_path.open("r") as f:
            document = json.load(f)

        # Update tag
        document["tag"] = tag if tag is not None else ""

        # Update result version
        versions = dict(document["versions"])
        versions[f"{new_version}"] = result.to_json()
        document["versions"] = versions

        # Persist updates
        with result_path.open("w") as f:
            json.dump(document, f)
    else:
        with result_path.open("w") as f:
            document = {
                "tag": tag if tag is not None else "",
                "versions": {"0": result.to_json()},
            }
            json.dump(document, f)


class FilesystemBackendStore(BackendStore):
    """
    The implementation of the filesystem -based backend store.
    """

    def __init__(self, *, uri: BackendStoreURI):
        """
        Initialize a new FilesystemBackendStore instance.
        :param uri: The URI that defines where the backend will store data
        :type uri: BackendStoreURI
        """
        # The URI for the backend
        self.uri = uri
        # The root path to the data storage location
        self.root = _parse_root_path(uri)
        if not self.root.exists():
            raise RuntimeError(
                f"Root data storage location does not exist: {self.root}."
            )

    def read_result(
        self,
        model_identifier: str,
        model_version: str,
        result_identifier: str,
        result_version: Optional[int] = None,
    ) -> Result:
        assert self.root.exists(), "Broken precondition."
        version_path = self.root / model_identifier / model_version
        if not version_path.exists():
            raise RuntimeError(
                f"Failed to read result, no results available for model identifier and version."
            )

        result_path = (version_path / result_identifier).with_suffix(".json")
        if not result_path.exists():
            raise RuntimeError(
                f"Failed to read result, result with identifier {result_identifier} not found."
            )

        if (
            result_version is not None
            and result_version not in _available_versions(result_path)
        ):
            raise RuntimeError(
                f"Failed to read result, requested version {result_version} not found."
            )

        requested_version = (
            result_version if result_version is not None else LATEST_VERSION
        )
        return _read_result(result_path, requested_version)

    def read_results(
        self, model_identifier: str, model_version: str, tag: Optional[str]
    ) -> List[Result]:
        assert self.root.exists(), "Broken precondition."
        version_path = self.root / model_identifier / model_version
        if not version_path.exists():
            raise RuntimeError(
                f"Failed to read result, no results available for model identifier and version."
            )

        available_results = _available_results(version_path)

        # Filter by tag, if applicable
        if tag is not None:
            available_results = [
                p for p in available_results if _read_tag(p) == tag
            ]

        return [_read_result(p, LATEST_VERSION) for p in available_results]

    def write_result(
        self,
        model_identifier: str,
        model_version: str,
        result_identifier: str,
        result: Result,
        tag: Optional[str] = None,
    ) -> None:
        assert self.root.exists(), "Broken precondition."
        # Create model directory
        model_path = self.root / model_identifier
        if not model_path.exists():
            model_path.mkdir()
        # Create version directory
        version_path = model_path / model_version
        if not version_path.exists():
            version_path.mkdir()

        result_path = (version_path / result_identifier).with_suffix(".json")
        _write_result(result_path, result, tag)


class FilesystemBackendStoreBuilder:
    """A builder for the FilesystemBackendStore."""

    def __init__(self):
        pass

    def with_uri(self, uri: BackendStoreURI):
        self.uri = uri
        return self

    def build(self) -> FilesystemBackendStore:
        return FilesystemBackendStore(uri=self.uri)
