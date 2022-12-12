"""
Implementation of the MongoDB-based backend store.
"""

"""
THIS BACKEND IS CURRENTLY NOT IMPLEMENTED.
"""

from typing import Any, Dict, Iterable, List, Optional

from pymongo import MongoClient

# The default protocol, host, and port for MongoDB instances
DEFAULT_DB_URI = "mongodb://localhost:27017/"
# The default database name for a MongoDB instance
DEFAULT_DB_NAME = "test"

# The name of the collection to which results are persisted
COLLECTION_NAME = "results"


class MongodbStorageClient:
    """
    A storage client for MongoDB.
    """

    def __init__(
        self,
        *,
        uri: Optional[str] = DEFAULT_DB_URI,
        db_name: Optional[str] = DEFAULT_DB_NAME,
    ):
        """
        Initialize a MongodbStorageClient instance.
        :param uri: The URI used to access the database instance
        :type uri: Optional[str]
        :param db_name: The name of the database context
        :type db_name: Optional[str]
        """
        # Initialize the instance connection
        self.client = MongoClient(uri)
        # Change the context to the database of interest
        self.db = self.client[db_name]


class StorageModelResult:
    """Represents an individual Result type in the underlying storage model."""


# -----------------------------------------------------------------------------
# READ
# -----------------------------------------------------------------------------


class ReadPoint:
    """Represents a point read query."""

    def __init__(
        self,
        *,
        model_identifier: str,
        model_version: str,
        measurement_name: str,
        measurement_identifier: str,
        version: Optional[int] = None,
    ):
        assert all(
            p is not None
            for p in [
                model_identifier,
                model_version,
                measurement_name,
                measurement_identifier,
            ]
        ), "Broken precondition."
        self.model_identifier = model_identifier
        self.model_version = model_version
        self.measurement_name = measurement_name
        self.measurement_identifier = measurement_identifier
        self.version = version


class ReadRange:
    """Represents a range read query."""

    def __init__(self, *, model_identifier: str, model_version: str, tag: str):
        pass


def _find_if(collection: Iterable[Any], predicate) -> Optional[Any]:
    """
    Find an element in `collection` by predicate.
    :param collection: The collection
    :type collection: Iterable[Any]
    :param predicate: The predicate
    :type predicate:
    :return The first item satisfying `predicate`, or `None`
    """
    for item in collection:
        if predicate(item):
            return item
    return None


def read_result(client: MongodbStorageClient, query: ReadPoint) -> List:
    """
    Read an individual result from the database.
    :param client: The database client
    :type client: MongodbStorageClient
    :param query: The query data
    :type query: ReadPoint
    """
    results = client.db[COLLECTION_NAME]

    documents = results.find_one(
        {
            "model_identifier": query.model_identifier,
            "model_version": query.model_version,
            "measurement_name": query.measurement_name,
            "measurement_identifier": query.measurement_identifier,
        }
    )
    assert len(documents) < 2, "Broken invariant."

    if len(documents) == 0:
        return []

    # Find requested version, or latest
    result_versions = documents[0]["versions"]
    result_versions = [
        (
            _find_if(result_versions, lambda o: o["version"] == query.version)
            if query.version is not None
            else sorted(result_versions, key=lambda o: o["version"])[-1]
        )
    ]
    return [o for o in result_versions if o is not None]


def read_results(client: MongodbStorageClient) -> List:
    pass


# -----------------------------------------------------------------------------
# WRITE
# -----------------------------------------------------------------------------


class WritePoint:
    """Represents a point write query."""

    def __init__(
        self,
        *,
        model_identifier: str,
        model_version: str,
        measurement_name: str,
        measurement_identifier: str,
        tag: str,
        data: Dict[str, Any],
        multiversion: bool,
    ):
        assert all(
            p is not None
            for p in [
                model_identifier,
                model_version,
                measurement_name,
                measurement_identifier,
                tag,
                data,
                multiversion,
            ]
        )
        self.model_identifier = model_identifier
        self.model_version = model_version
        self.measurement_name = measurement_name
        self.measurement_identifier = measurement_identifier
        self.tag = tag
        self.data = data
        self.multiversion = multiversion


class WriteRange:
    pass


def write_result(client: MongodbStorageClient, query: WritePoint):
    results = client.db[COLLECTION_NAME]

    # Determine if (some version of) the result exists
    filter = {
        "model_identifier": query.model_identifier,
        "model_version": query.model_version,
        "measurement_name": query.measurement_name,
        "measurement_identifier": query.measurement_identifier,
    }
    exists = results.count_documents(filter) > 0
    if not exists:
        results.insert_one(
            {
                "model_identifier": query.model_identifier,
                "model_version": query.model_version,
                "measurement_name": query.measurement_name,
                "measurement_identifier": query.measurement_identifier,
                "tag": query.tag,
                "result_versions": [{"version": 0, "data": query.data}],
            }
        )
        return

    documents = results.find_one(filter)
    assert len(documents) == 1, "Broken invariant."
    document = documents[0]

    # Extract existing versions
    versions = document["versions"]
    assert len(versions) > 0, "Broken invariant."

    # Determine the latest version
    latest = max(o["version"] for o in versions)
    if query.multiversion:
        # If the result exists and multiversioning is requested,
        # we must update the document with a new version
        new_versions = versions + [{"version": latest + 1, "data": query.data}]
    else:
        # If the result exists and multiversioning is not requested,
        # we must replace the existing latest with the new version
        new_versions = [o for o in versions if o["version"] != latest]
        new_versions.append({"version": latest, "data": query.data})

    results.update_one(
        filter,
        {
            "model_identifier": query.model_identifier,
            "model_version": query.model_version,
            "measurement_name": query.measurement_name,
            "measurement_identifier": query.measurement_identifier,
            "tag": query.tag,
            "result_versions": new_versions,
        },
    )


def write_results(client: MongodbStorageClient):
    pass
