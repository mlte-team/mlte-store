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

# -----------------------------------------------------------------------------
# Parsing Helpers
# -----------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------
# Metadata
# -----------------------------------------------------------------------------


def _available_result_versions(result_path: Path) -> Set[int]:
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


def _available_results(version_path: Path) -> List[Path]:
    """
    Get all available results for a particular model version.
    :param version_path: The path to model version directory
    :type version_path: Path
    :return: A collection of paths for all results
    :rtype: List[Path]
    """
    return [x for x in version_path.glob("*") if x.is_file()]


def _available_model_versions(model_path: Path) -> Set[Path]:
    """
    Get all available model versions for a particular model.
    :param model_path: The path to the model directory
    :type model_path: Path
    :return: A collection of paths to model versions
    :rtype: Set[Path]
    """
    return [x for x in model_path.glob("*") if x.is_dir()]


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


# -----------------------------------------------------------------------------
# Read
# -----------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------
# Write
# -----------------------------------------------------------------------------


def _write_result(result_path: Path, result: Result, tag: Optional[str]):
    """
    Write a result to the file at `result_path`.
    :param result_path: The path to the result
    :type result_path: Path
    :param result: The result
    :type result: Result
    """
    if result_path.exists():
        new_version = max(_available_result_versions(result_path)) + 1

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


# -----------------------------------------------------------------------------
# Delete
# -----------------------------------------------------------------------------


def _delete_result_version(result_path: Path, version: int):
    """
    Delete an individual version for a result.
    :param result_path: The path to the result
    :type result_path: Path
    :param version: The target version
    :type version: int
    """
    assert result_path.exists(), "Broken precondition."
    with result_path.open("r") as f:
        document = json.load(f)

    # Update versions data
    versions = dict(document["versions"])
    assert version in set(int(k) for k in versions.keys()), "Broken invariant."
    versions = {k: v for k, v in versions.items() if k != f"{version}"}

    # If no versions remain, delete this result
    if len(versions) == 0:
        result_path.unlink()
        return

    # Otherwise, versions remain, write updated content
    with result_path.open("w") as f:
        document["versions"] = versions
        json.dump(document, f)


def _delete_result(result_path: Path):
    """
    Delete an individual result.
    """
    assert result_path.exists(), "Broken precondition."

    # Deleting all of the result versions implicitly deletes the result
    available_versions = _available_result_versions(result_path)
    assert len(available_versions) > 0, "Broken invariant."
    for version in available_versions:
        _delete_result_version(result_path, version)

    assert not result_path.exists(), "Broken postcondition."


def _delete_results(result_paths: List[Path]):
    """
    Delete a collection of results.
    """
    assert len(result_paths) > 0, "Broken precondition."
    for path in result_paths:
        _delete_result(path)

    assert all(not p.exists() for p in result_paths), "Broken postcondition."


def _propagate_deleted_result(model_path: Path, model_version: str):
    """
    Propagate the deletion of one or more results to higher-level structures.
    :param model_path: The path to the model directory
    :type model_path: Path
    :param model_version: The string identifier for model version
    :type model_version: str
    """
    assert model_path.exists(), "Broken precondition."

    # If all results are deleted, remove the model version
    version_path = model_path / model_version
    if len(_available_results(version_path)) == 0:
        version_path.rmdir()

    # If all model versions are deleted, remove the model
    if len(_available_model_versions(model_path)) == 0:
        model_path.rmdir()


# -----------------------------------------------------------------------------
# FilesystemBackendStore Interface
# -----------------------------------------------------------------------------


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
        self._check_exists(model_identifier, model_version)

        version_path = self.root / model_identifier / model_version
        assert version_path.exists(), "Broken invariant."

        result_path = (version_path / result_identifier).with_suffix(".json")
        if not result_path.exists():
            raise RuntimeError(
                f"Failed to read result, result with identifier {result_identifier} not found."
            )

        if (
            result_version is not None
            and result_version not in _available_result_versions(result_path)
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
        self._check_exists(model_identifier, model_version)

        version_path = self.root / model_identifier / model_version
        assert version_path.exists(), "Broken invariant."

        # Query results and filter by tag, if applicable
        available_results = _available_results(version_path)
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

    def delete_result_version(
        self,
        model_identifier: str,
        model_version: str,
        result_identifier: str,
        result_version: int,
    ):
        assert self.root.exists(), "Broken precondition."
        self._check_exists(model_identifier, model_version)

        version_path = self.root / model_identifier / model_version
        assert version_path.exists(), "Broken invariant."

        result_path = (version_path / result_identifier).with_suffix(".json")
        if not result_path.exists():
            raise RuntimeError(
                f"Cannot delete result version, result with identifier {result_identifier} does not exist."
            )

        available_versions = _available_result_versions(result_path)
        if result_version not in available_versions:
            raise RuntimeError(
                f"Cannot delete result version, version {result_version} does not exist."
            )

        _delete_result_version(result_path, result_version)

        # Version deletion may have deleted the result entirety;
        # check to determine if this deletion must propagate
        _propagate_deleted_result(self.root / model_identifier, model_version)

    def delete_result(
        self, model_identifier: str, model_version: str, result_identifier: str
    ):
        assert self.root.exists(), "Broken precondition."
        self._check_exists(model_identifier, model_version)

        version_path = self.root / model_identifier / model_version
        assert version_path.exists(), "Broken invariant."

        result_path = (version_path / result_identifier).with_suffix(".json")
        if not result_path.exists():
            raise RuntimeError(
                f"Cannot delete result version, result with identifier {result_identifier} does not exist."
            )

        _delete_result(result_path)

        # Result deletion may have removed last result in version
        _propagate_deleted_result(self.root / model_identifier, model_version)

    def delete_results(
        self,
        model_identifier: str,
        model_version: str,
        result_tag: Optional[str] = None,
    ):
        assert self.root.exists(), "Broken precondition."
        self._check_exists(model_identifier, model_version)

        version_path = self.root / model_identifier / model_version
        assert version_path.exists(), "Broken invariant."

        # Query all available results
        result_paths = _available_results(version_path)
        # Filter by tag, if applicable
        result_paths = (
            [p for p in result_paths if _read_tag(p) == result_tag]
            if result_tag is not None
            else result_paths
        )

        _delete_results(result_paths)

        # Result deletion may result in removal of model version
        _propagate_deleted_result(self.root / model_identifier, model_version)

    def delete_model_version(self, model_identifier: str, model_version: str):
        self._check_exists(model_identifier, model_version)

        version_path = self.root / model_identifier / model_version
        assert version_path.exists(), "Broken invariant."

        # Remove all results
        result_paths = _available_results(version_path)
        for path in result_paths:
            _delete_result(path)

        # Delete the version
        version_path.rmdir()

        assert not version_path.exists(), "Broken postcondition."

    def delete_model(self, model_identifier: str):
        self._check_exists(model_identifier, None)

        model_path = self.root / model_identifier
        assert model_path.exists(), "Broken invariant."

        version_paths = _available_model_versions(model_path)
        for version_path in version_paths:
            result_paths = _available_results(version_path)
            for result_path in result_paths:
                _delete_result(result_path)

            version_path.rmdir()

        model_path.rmdir()

        assert not model_path.exists(), "Broken postcondition."

    def _check_exists(
        self, model_identifier: str, model_version: Optional[str] = None
    ):
        """
        Check if data is available for a particular model and version.
        :param model_identifier: The model identifier
        :type model_identifier: str
        :param model_version: The model version
        :type model_version: Optional[str]
        """
        model_path = self.root / model_identifier
        if not model_path.exists():
            raise RuntimeError(
                f"Model with identifier {model_identifier} not found."
            )

        if model_version is None:
            return

        version_path = model_path / model_version
        if not version_path.exists():
            raise RuntimeError(
                f"Model version {model_version} for model {model_identifier} not found."
            )


class FilesystemBackendStoreBuilder:
    """A builder for the FilesystemBackendStore."""

    def __init__(self):
        pass

    def with_uri(self, uri: BackendStoreURI):
        self.uri = uri
        return self

    def build(self) -> FilesystemBackendStore:
        return FilesystemBackendStore(uri=self.uri)
