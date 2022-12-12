"""
Application entry point.
"""

import argparse
import logging
import random
import string
import time
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, Request
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

app = FastAPI()

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
        require=True,
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


class RequestModelResult(BaseModel):
    """A representation of an individual result in the request model."""

    # Identifies the model (project) of interest
    model_identifier: str
    # Identifies the model version of interest (e.g. within a project)
    model_version: str
    # Identifies the name of the measurement that produced the result
    measurement_name: str
    # Disambiguates multiple instances of the same measurement
    measurement_identifier: Optional[str] = DEFAULT_MEASUREMENT_IDENTIFIER
    # Allows arbitrary grouping of results within a model context
    tag: Optional[str] = DEFAULT_TAG
    # The result data
    data: Dict[str, Any]


# -----------------------------------------------------------------------------
# Middleware
# -----------------------------------------------------------------------------


@app.middleware("http")
async def log_requests(request: Request, call_next: Any):
    """
    Log basic information for each request.
    :param request: The incoming request
    :type request: Request
    :param call_next: The next middleware in the call chain
    :type call_next: Any
    """
    id = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    logging.info(f"rid={id} start request path={request.url.path}")
    start_time = time.time()

    response = await call_next(request)

    process_time = (time.time() - start_time) * 1000
    formatted_process_time = "{0:.2f}".format(process_time)
    logging.info(
        f"rid={id} completed_in={formatted_process_time}ms status_code={response.status_code}"
    )

    return response


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------


@app.get("/results")
async def get_results(
    model_identifier: str,
    model_version: str,
    measurement_name: Optional[str] = None,
    measurement_identifier: Optional[str] = None,
    result_version: Optional[int] = None,
    tag: Optional[str] = None,
):
    """
    Get a result or a collection of results.
    :param model_identifier: The identifier for the model of interest
    :type model_identifier: str
    :param model_version: The version string for the model of interest
    :type model_version: str
    :param measurement_name: The name of the measurement of interest
    :type measurement_name: Optional[str]
    :param measurement_identifier: The identifier for the measurement of interest
    :type measurement_identifier: Optional[str]
    :param result_version: The version identifier for the result of interest
    :type result_version: Optional[int]
    :param tag: The tag for the result of interest
    :type tag: Optional[str]
    """
    # Determine if this is a range or point query
    is_point_query = measurement_name is not None

    results = []
    return {"results": results}


@app.post("/results")
async def post_results(
    results: List[RequestModelResult], multiversion: Optional[bool] = False
):
    """
    Post a result or collection of results.
    :param results: The collection of results to post (1 or more)
    :type results: List[RequestModelResult]
    :param multiversion: Enable multiversioning for individual results
    :type multiversion: Optional[bool]
    """
    pass


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> int:
    host, port, backend_store_uri, verbose = parse_arguments()
    logging.basicConfig(level=logging.INFO if verbose else logging.ERROR)

    uvicorn.run(app, host=host, port=port)
    return EXIT_SUCCESS
