"""
Backend initialization.
"""

from typing import Any, Dict, Optional

from .backend import BackendStore, BackendStoreURI, StoreType
from .fs import FilesystemBackendStoreBuilder


def _unreachable():
    assert False, "Unreachable."


def _parse_uri(uri: str) -> BackendStoreURI:
    """
    Parse the URI for a backend store.
    :param uri: The URI for the backend store
    :type uri: str
    :return: The parsed BackendStoreURI
    :rtype: BackendStoreURI
    """
    return BackendStoreURI.from_string(uri)


def initialize_backend_store(
    uri: str, environment: Optional[Dict[str, Any]] = None
) -> BackendStore:
    """
    Prepare the backend store for use.
    :param uri: The URI for the backend store
    :type uri: str
    :param environment: The environment used to initialize the store
    :type: environment: Optional[Dict[str, Any]]
    :return: The prepared backend store
    :rtype: BackendStore
    """
    uri = _parse_uri(uri)
    if uri.type == StoreType.FS:
        return FilesystemBackendStoreBuilder().with_uri(uri).build()
    _unreachable()
