import httpx
import structlog

from ....core.config import config  # type: ignore[import-untyped]
from ....core.exceptions import AcrossHTTPException  # type: ignore[import-untyped]

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


TLE_URL: str = f"{config.across_server_url()}/tle/"


def post(data: dict = {}) -> None:
    """
    Method to utilize the ACROSS web API to post a TLE

    Parameters
    -----------
    data: dict
        Dictionary of valid TLE information to be posted to the ACROSS Server.

    -----------
    Raises
    -----------
    AcrossHTTPException
        If the return status code is not a 201 or 409
    """
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {config.ACROSS_SERVER_SECRET}",
        "Content-Type": "application/json",
    }

    response = httpx.request("POST", url=TLE_URL, json=data, headers=headers)

    if response.status_code == 201:
        return
    elif response.status_code == 409:
        logger.warn(f"409: {response.text}")
    else:
        raise AcrossHTTPException(response.status_code, response.text, {})
