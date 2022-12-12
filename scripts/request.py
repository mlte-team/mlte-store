"""
Generate requests.
"""

import sys

import requests

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

URL = "http://localhost:8080"


def main() -> int:
    # POST single
    # data = {
    #     "model_identifier": "m0",
    #     "model_version": "v0",
    #     "result_identifier": "r0",
    #     "data": {"foo": "bar"},
    # }
    # res = requests.post(f"{URL}/result")

    # GET single
    # query = f"?model_identifier=m0&model_version=v0&result_identifier=r0"
    # res = requests.get(f"{URL}/result{query}")

    # GET collection
    query = f"?model_identifier=m0&model_version=v0"
    res = requests.get(f"{URL}/results{query}")

    print(res.status_code)
    print(res.json())
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
