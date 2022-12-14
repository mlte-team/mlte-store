"""
Data model implementation.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class ResultVersion(BaseModel):
    """Represents an individual result version."""

    # The version identifier
    version: str
    # The result payload
    data: Dict[str, Any]

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON object."""
        return {"version": self.version, "data": self.data}

    @staticmethod
    def from_json(json: Dict[str, Any]):
        """Deserialize from JSON object."""
        return ResultVersion(json["version"], json["data"])


class Result(BaseModel):
    """Represents an individual result (a collection of versions)."""

    # The identifier for the result
    identifier: str
    # The tag associated with the result
    tag: Optional[str] = None
    # A collection of result versions
    versions: List[ResultVersion]

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON object."""
        return {
            "identifier": self.identifier,
            "tag": self.tag if self.tag is not None else "",
            "versions": [v.to_json() for v in self.versions],
        }

    @staticmethod
    def from_json(json: Dict[str, Any]):
        """Deserialize from JSON object."""
        return Result(
            identifier=json["identifier"],
            tag=None if json["tag"] == "" else json["tag"],
            versions=[ResultVersion.from_json(v) for v in json["versions"]],
        )


class ModelIdentifier:
    """Represents a model identifier string."""

    def __init__(self, identifier: str):
        self.identifier = identifier

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON object."""
        return self.identifier


class ModelVersion:
    """Represents a model version string."""

    def __init__(self, version: str):
        self.version = version

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON object."""
        return self.version


class ModelMetadata:
    """Represents the metadata returned for a model."""

    def __init__(
        self, identifier: ModelIdentifier, versions: List[ModelVersion]
    ):
        self.identifier = identifier
        self.versions = versions

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON object."""
        # TODO(Kyle): Sort by creation timestamp?
        return {
            "identifier": self.identifier.to_json(),
            "versions": [v.to_json() for v in sorted(self.versions)],
        }
