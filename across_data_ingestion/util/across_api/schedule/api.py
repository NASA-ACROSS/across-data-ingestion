import logging

import httpx

from ....core.config import config  # type: ignore[import-untyped]
from ....core.exceptions import AcrossHTTPException  # type: ignore[import-untyped]
from ....tasks.schedules.types import AcrossSchedule

logger = logging.getLogger("uvicorn.error")

SCHEDULE_URL: str = f"{config.across_server_url()}/schedule/"


def post(data: dict | AcrossSchedule = {}) -> None:
    """
    Method to utilize the ACROSS web api to post a schedule

    Parameters
    -----------
    data: dict
        Dictionary of valid schedule information to be posted to the ACROSS Server.

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

    response = httpx.request("POST", url=SCHEDULE_URL, json=data, headers=headers)

    if response.status_code == 201:
        logger.info(f"Schedule Created with id: {response.text}")
    elif response.status_code == 409:
        logger.info(response.text)
    else:
        raise AcrossHTTPException(response.status_code, response.text, {})
