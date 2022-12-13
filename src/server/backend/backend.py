"""
Backend storage interface.
"""

from enum import Enum
from typing import Any, Dict, List, Optional


class StoreType(Enum):
    """Represents the type of backend store."""

    # Use the local filesystem as the backend store
    FS = 0


class BackendStoreURI:
    """Represents the URI for a backend store."""

    def __init__(self, uri: str, type: StoreType):
        """
        Initialize a BackendStoreURI instance.
        :param uri: The URI
        :type uri: str
        :param type: The type of the backend store
        :type type: StoreType
        """
        self.uri = uri
        self.type = type

    @staticmethod
    def from_string(uri: str):
        """
        Parse a BackendStoreURI from a string.
        :param uri: The URI
        :type uri: str
        :return: The parsed BackendStoreURI
        :rtype: BackendStoreURI
        """
        if uri.startswith("file://"):
            return BackendStoreURI(uri, StoreType.FS)
        raise RuntimeError(f"Unrecognized backend store URI: {uri}.")


class Result:
    """Represents an individual result that is written to and read from the backend store."""

    def __init__(self, *, data: Dict[str, Any]):
        """
        Initialize a new Result instance.
        :param data: The result data
        :type data: Dict[str, Any]
        """
        self.data = data

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON document."""
        return {"data": self.data}

    @staticmethod
    def from_json(json: Dict[str, Any]):
        """Deserialize from JSON document."""
        return Result(data=json["data"])


class BackendStore:
    """Represents an abstract backend store."""

    def read_result(
        self,
        model_identifier: str,
        model_version: str,
        result_identifier: str,
        result_version: Optional[int] = None,
    ) -> Result:
        """
        Read an individual result from the backend store.
        :param model_identifier: The identifier for the model of interest
        :type model_identifier: str
        :param model_version: The model version string
        :type model_version: str
        :param result_identifier: The identifier for the result of interest
        :type result_identifier: str
        :param result_version: The (optional) version for the result
        :type result_version: Optional[int]
        :return: The result
        :rtype: Result
        """
        raise NotImplementedError(
            "Cannot invoke method on abstract BackendStore."
        )

    def read_results(
        self, model_identifier: str, model_version: str, tag: Optional[str]
    ) -> List[Result]:
        """
        Read a collection of results from the backend store.
        :param model_identifier: The identifier for the model of interest
        :type model_identifier: str
        :param model_version: The model version string
        :type model_version: str
        :param tag: The (optional) result tag to limit returned results
        :type tag: Optional[int]
        :return: A collection of the results matching the query
        :rtype: List[Result]
        """
        raise NotImplementedError(
            "Cannot invoke method on abstract BackendStore."
        )

    def write_result(
        self,
        model_identifier: str,
        model_version: str,
        result_identifier: str,
        result: Result,
        tag: Optional[str],
    ) -> None:
        """
        Write an individual result to the backend store.
        :param model_identifier: The identifier for the model of interest
        :type model_identifier: str
        :param model_version: The model version string
        :type model_version: str
        :param result_identifier: The identifier for the result of interest
        :type result_identifier: str
        :param result: The result to be written
        :type result: Result
        :param tag: The (optional) tag to apply
        :type tag: Optional[str]
        """
        raise NotImplementedError(
            "Cannot invoke method on abstract BackendStore."
        )

    def delete_result_version(
        self,
        model_identifier: str,
        model_version: str,
        result_identifier: str,
        result_version: int,
    ):
        """
        Delete an individual result version.
        :param model_identifier: The identifier for the model of interest
        :type model_identifier: str
        :param model_version: The model version string
        :type model_version: str
        :param result_identifier: The identifier for the result of interest
        :type result_identifier: str
        :param result_version: The version for the result
        :type result_version: int
        """
        raise NotImplementedError(
            "Cannot invoke method on abstract BackendStore."
        )

    def delete_result(
        self, model_identifier: str, model_version: str, result_identifier: str
    ):
        """
        Delete all versions for a result.
        :param model_identifier: The identifier for the model of interest
        :type model_identifier: str
        :param model_version: The model version string
        :type model_version: str
        :param result_identifier: The identifier for the result of interest
        :type result_identifier: str
        """
        raise NotImplementedError(
            "Cannot invoke method on abstract BackendStore."
        )

    def delete_results(
        self,
        model_identifier: str,
        model_version: str,
        result_tag: Optional[str] = None,
    ):
        """
        Delete a collection of results.
        :param model_identifier: The identifier for the model of interest
        :type model_identifier: str
        :param model_version: The model version string
        :type model_version: str
        :param result_tag: An (optional) tag to filter results that are deleted
        :type result_tag: Optional[str]
        """
        raise NotImplementedError(
            "Cannot invoke method on abstract BackendStore."
        )

    def delete_model_version(self, model_identifier: str, model_version: str):
        """
        Delete all results for a model version.
        :param model_identifier: The identifier for the model of interest
        :type model_identifier: str
        :param model_version: The model version string
        :type model_version: str
        """
        raise NotImplementedError(
            "Cannot invoke method on abstract BackendStore."
        )

    def delete_model(self, model_identifier: str):
        """
        Delete all results for a model (all versions).
        :param model_identifier: The identifier for the model of interest
        :type model_identifier: str
        """
        raise NotImplementedError(
            "Cannot invoke method on abstract BackendStore."
        )
