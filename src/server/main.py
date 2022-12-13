"""
Application entry point.
"""

import argparse
import logging
import sys
from typing import Any, Dict, Optional

import uvicorn
from backend import BackendStore, Result, initialize_backend_store
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Application exit codes
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

# The deafult host address to which the server binds
DEFAULT_HOST = "localhost"
# The default port on which the server listens
DEFAULT_PORT = 8080

# Identifies the 'default' measurement, by name, for a particular model context
DEFAULT_MEASUREMENT_IDENTIFIER = ""

# The default tag applied to individual results
DEFAULT_TAG = ""

# -----------------------------------------------------------------------------
# Global State
# -----------------------------------------------------------------------------

# The global FastAPI application
g_app = FastAPI()

# The global backend
g_store: BackendStore = None

# -----------------------------------------------------------------------------
# Argument Parsing
# -----------------------------------------------------------------------------


def parse_arguments():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_HOST,
        help=f"The host address to which the server binds (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"The port on which the server listens (default: {DEFAULT_PORT})",
    )
    # TODO(Kyle): Set a reasonable default
    parser.add_argument(
        "--backend-store-uri",
        type=str,
        required=True,
        help="The URI for the backend store.",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose output."
    )
    args = parser.parse_args()
    return args.host, args.port, args.backend_store_uri, args.verbose


# -----------------------------------------------------------------------------
# Data Model
# -----------------------------------------------------------------------------

# TODO(Kyle): Move data model elsewhere
class RequestModelResult(BaseModel):
    """A representation of an individual result in the request model."""

    # Identifies the model (project) of interest
    model_identifier: str
    # Identifies the model version of interest (e.g. within a project)
    model_version: str
    # Disambiguates multiple instances of the same measurement
    result_identifier: str
    # Allows arbitrary grouping of results within a model context
    result_tag: Optional[str] = None
    # The result data
    data: Dict[str, Any]


# -----------------------------------------------------------------------------
# Routes: Read Results
# -----------------------------------------------------------------------------


@g_app.get("/result")
async def get_result(
    model_identifier: str,
    model_version: str,
    result_identifier: str,
    result_version: Optional[int] = None,
):
    """
    Get an individual result.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param result_identifier: The identifier for the result of interest
    :type result_identifier: str
    :param result_version: The version identifier for the result of interest
    :type result_version: Optional[int]
    """
    try:
        # Result the result from the store
        result = g_store.read_result(
            model_identifier, model_version, result_identifier, result_version
        )
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")
    return {"results": [result.to_json()]}


@g_app.get("/results")
async def get_results(
    model_identifier: str,
    model_version: str,
    result_tag: Optional[str] = None,
):
    """
    Get a result or a collection of results.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param result_tag: The tag for the result of interest
    :type result_tag: Optional[str]
    """
    try:
        results = g_store.read_results(
            model_identifier, model_version, result_tag
        )
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")
    return {"results": [r.to_json() for r in results]}


# -----------------------------------------------------------------------------
# Routes: Write Results
# -----------------------------------------------------------------------------


@g_app.post("/result")
async def post_result(result: RequestModelResult):
    """
    Post a result or collection of results.
    :param result: The result to write
    :type result: RequestModelResult
    """
    try:
        # Write the result to the backend
        g_store.write_result(
            result.model_identifier,
            result.model_version,
            result.result_identifier,
            Result(data=result.data),
            result.result_tag,
        )
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")


# -----------------------------------------------------------------------------
# Routes: Delete Results
# -----------------------------------------------------------------------------


@g_app.delete("/result")
async def delete_result(
    model_identifier: str,
    model_version: str,
    result_identifier: str,
    result_version: Optional[int],
):
    """
    Delete an individual result.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param result_identifier: The identifier for the result of interest
    :type result_identifier: str
    :param result_version: The (optional) version identifier for the result
    :type result_version: Optional[int]
    """
    fn = (
        g_store.delete_result_version
        if result_version is not None
        else g_store.delete_result
    )
    try:
        args = [
            model_identifier,
            model_version,
            result_identifier,
            result_version,
        ]
        fn(*[args for arg in args if arg is not None])
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")


@g_app.delete("/results")
async def delete_results(
    model_identifier: str, model_version: str, result_tag: Optional[str]
):
    """
    Delete a collection of results.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param result_tag: The (optional) tag that identifies results of interest
    :type result_tag: Optional[str]
    """
    try:
        g_store.delete_results(model_identifier, model_version, result_tag)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")


@g_app.delete("/model")
async def delete_model(model_identifier: str, model_version: Optional[str]):
    """
    Delete a model (or a model version).
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The (optional) version string for the model of interest
    :type model_version: Optional[str]
    """
    fn = (
        g_store.delete_model_version
        if model_version is not None
        else g_store.delete_model
    )
    try:
        args = [model_identifier, model_version]
        fn(*[arg for arg in args if arg is not None])
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> int:
    global g_store

    host, port, backend_store_uri, verbose = parse_arguments()
    logging.basicConfig(level=logging.INFO if verbose else logging.ERROR)

    # Initialize the backend store
    try:
        g_store = initialize_backend_store(backend_store_uri)
    except RuntimeError as e:
        logging.error(f"{e}")
        return EXIT_FAILURE

    uvicorn.run(g_app, host=host, port=port)
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
