"""
Application entry point.
"""

import argparse
import logging
import sys
from typing import List, Optional

import uvicorn
from backend import BackendStore, initialize_backend_store
from backend.models import ModelMetadata, Result
from fastapi import FastAPI, HTTPException

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
# Routes: Read Metadata
# -----------------------------------------------------------------------------


@g_app.get("/metadata/model")
async def get_models():
    """Get metadata for all existing models."""
    try:
        models: List[ModelMetadata] = g_store.read_model_metadata()
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")

    return {"models": [m.to_json() for m in models]}


@g_app.get("/metadata/model/{model_identifier}")
async def get_model(model_identifier: str):
    """
    Get metadata for a single model.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    """
    try:
        models: List[ModelMetadata] = g_store.read_model_metadata(
            model_identifier
        )
        assert len(models) == 1, "Broken invariant."
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")
    return {"models": [m.to_json() for m in models]}


# -----------------------------------------------------------------------------
# Routes: Read Results
# -----------------------------------------------------------------------------


@g_app.get(
    "/result/{model_identifier}/{model_version}/{result_identifier}/{result_version}"
)
async def get_result_version(
    model_identifier: str,
    model_version: str,
    result_identifier: str,
    result_version: int,
):
    """
    Get an individual result version.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param result_identifier: The identifier for the result of interest
    :type result_identifier: str
    :param result_version: The version identifier for the result of interest
    :type result_version: int
    """
    try:
        # Read the result from the store
        result: Result = g_store.read_result(
            model_identifier, model_version, result_identifier, result_version
        )
        assert result is not None, "Broken invariant."
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")
    return {"results": [result.to_json()]}


@g_app.get("/result/{model_identifier}/{model_version}/{result_identifier}")
async def get_result(
    model_identifier: str,
    model_version: str,
    result_identifier: str,
):
    """
    Get an individual result.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param result_identifier: The identifier for the result of interest
    :type result_identifier: str
    """
    try:
        # Result the result from the store
        result: Result = g_store.read_result(
            model_identifier, model_version, result_identifier
        )
        assert result is not None, "Broken invariant."
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
        results: List[Result] = g_store.read_results(
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


@g_app.post("/result/{model_identifier}/{model_version}")
async def post_result(
    model_identifier: str, model_version: str, result: Result
):
    """
    Post a result or collection of results.
    :param result: The result to write
    :type result: RequestModelResult
    """
    try:
        # Write the result to the backend
        written = g_store.write_result(
            model_identifier,
            model_version,
            result,
        )
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")

    return {"written": written}


# -----------------------------------------------------------------------------
# Routes: Delete Results
# -----------------------------------------------------------------------------


@g_app.delete(
    "/result/{model_identifier}/{model_version}/{result_identifier}/{result_version}"
)
async def delete_result_version(
    model_identifier: str,
    model_version: str,
    result_identifier: str,
    result_version: int,
):
    """
    Delete an individual result version.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param result_identifier: The identifier for the result of interest
    :type result_identifier: str
    :param result_version: The version identifier for the result
    :type result_version: int
    """
    try:
        deleted = g_store.delete_result_version(
            model_identifier, model_version, result_identifier, result_version
        )
        assert deleted == 1, "Broken invariant."
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")

    return {"deleted": deleted}


@g_app.delete("/result/{model_identifier}/{model_version}/{result_identifier}")
async def delete_result_version(
    model_identifier: str,
    model_version: str,
    result_identifier: str,
    result_version: int,
):
    """
    Delete an individual result.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param result_identifier: The identifier for the result of interest
    :type result_identifier: str
    """
    try:
        deleted = g_store.delete_result_version(
            model_identifier, model_version, result_identifier, result_version
        )
        assert deleted == 1, "Broken invariant."
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")

    return {"deleted": deleted}


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
        deleted = g_store.delete_results(
            model_identifier, model_version, result_tag
        )
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error.")

    return {"deleted": deleted}


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
