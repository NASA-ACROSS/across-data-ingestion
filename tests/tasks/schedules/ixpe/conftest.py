from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from across_data_ingestion.util.across_server import sdk

from .mocks import mock_ixpe_query, sample_html_string


## SET DATA FROM TOP-LEVEL FIXTURES ##
@pytest.fixture()
def set_mock_res_text(fake_httpx_response: MagicMock) -> None:
    fake_httpx_response.text = sample_html_string.html


@pytest.fixture(autouse=True)
def set_ixpe_telescope(
    mock_telescope_api: MagicMock, fake_ixpe_telescope: sdk.Telescope
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_ixpe_telescope]


## MOCK BEHAVIOR ##
@pytest.fixture
def mock_logger() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.ixpe.low_fidelity_planned.logger"
    ) as mock_logger:
        yield mock_logger


## FAKE DATA ##
@pytest.fixture
def fake_ixpe_telescope(fake_telescope: sdk.Telescope) -> sdk.Telescope:
    fake_telescope.id = "ixpe_telescope_id"

    for i in fake_telescope.instruments or []:
        i.id = "ixpe_instrument_id"

    return fake_telescope


@pytest.fixture
def fake_ixpe_schedule_df() -> pd.DataFrame:
    return pd.DataFrame(mock_ixpe_query.result)
