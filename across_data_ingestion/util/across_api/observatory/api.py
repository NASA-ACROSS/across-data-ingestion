from urllib.parse import urlencode

import httpx

from ....core.config import config  # type: ignore[import-untyped]
from ....core.exceptions import AcrossHTTPException  # type: ignore[import-untyped]

OBSERVATORY_URL: str = f"{config.across_server_url()}/observatory/"


def get(params: dict = {}) -> list[dict]:
    """
    Method to utilize the ACROSS web api to access many observatories

    Parameters
    -----------
    params: dict
        Dictionary of valid query parameters to be converted into a url query string.

    -----------
    Raises
    -----------
    AcrossHTTPException
        If the return status code is not a 200
    """
    query_string = urlencode(params)

    query_url = f"{OBSERVATORY_URL}?{query_string}"

    response = httpx.request("GET", url=query_url)

    if response.status_code == 200:
        return response.json()

    raise AcrossHTTPException(response.status_code, response.text, {})
