from collections.abc import Generator
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from astropy.table import Table  # type: ignore[import-untyped]

from across_data_ingestion.util.across_server import sdk


@pytest.fixture
def mock_logger() -> Generator[MagicMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.logger"
    ) as mock_logger:
        yield mock_logger


@pytest.fixture
def fake_telescope() -> sdk.Telescope:
    return sdk.Telescope(
        id="telescope_uuid",
        name="Chandra",
        short_name="chandra",
        created_on=datetime.now(),
        instruments=[
            sdk.IDNameSchema(
                id="instrument_uuid",
                name="CHANDRA ACIS",
                short_name="ACIS",
            )
        ],
    )


@pytest.fixture(autouse=True)
def set_sdk_data(mock_telescope_api: MagicMock, fake_telescope: sdk.Telescope) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_telescope]


@pytest.fixture
def fake_observation_data() -> dict:
    return {
        "obsid": 28845,
        "start_date": "2025-06-30T22:23:23",
        "ra": "39.96041666666667",
        "dec": "-1.5856000000000001",
        "instrument": "ACIS-I",
        "grating": "NONE",
        "exposure_mode": "NONE",
    }


@pytest.fixture
def fake_observation_table(
    fake_observation_data: dict,
    request: pytest.FixtureRequest,
) -> Table:
    param = getattr(request, "param", "default")

    if param is None:
        return None
    elif param == "default":
        return Table([fake_observation_data])
    else:
        return Table([fake_observation_data])


@pytest.fixture
def fake_exposure_times_data() -> dict:
    return {
        "obs_id": "28845",
        "target_name": "Abell 370",
        "t_plan_exptime": 20000.0,
    }


@pytest.fixture
def fake_exposure_times_table(
    fake_exposure_times_data: dict,
    request: pytest.FixtureRequest,
) -> Table:
    param = getattr(request, "param", "default")

    if param is None:
        return None
    elif param == "default":
        return Table([fake_exposure_times_data])
    else:
        return Table([fake_exposure_times_data])


@pytest.fixture
def mock_vo_service_query(
    fake_observation_table: Table, fake_exposure_times_table: Table
) -> AsyncMock:
    mock = AsyncMock()
    # ingest process queries for observations, then exposure times
    mock.side_effect = [fake_observation_table, fake_exposure_times_table]

    return mock


@pytest.fixture
def mock_vo_service(mock_vo_service_query: AsyncMock) -> AsyncMock:
    mock_instance = AsyncMock()
    mock_instance.query = mock_vo_service_query
    # mock the context management so it actually returns the expected instance
    mock_instance.__aenter__.return_value = mock_instance

    return mock_instance


@pytest.fixture(autouse=True)
def mock_vo_service_cls(mock_vo_service: AsyncMock) -> Generator[AsyncMock]:
    with patch(
        "across_data_ingestion.tasks.schedules.chandra.high_fidelity_planned.VOService",
        return_value=mock_vo_service,
    ) as mock_vo_service_cls:
        yield mock_vo_service_cls


@pytest.fixture
def fake_instruments() -> list[sdk.IDNameSchema]:
    instruments = [
        {
            "name": "Instrument Name - ACIS",
            "short_name": "ACIS",
            "id": "acis-mock-id",
        },
        {
            "name": "Instrument Name - ACIS-HETG",
            "short_name": "ACIS-HETG",
            "id": "acis-hetg-mock-id",
        },
        {
            "name": "Instrument Name - ACIS-LETG",
            "short_name": "ACIS-LETG",
            "id": "acis-letg-mock-id",
        },
        {
            "name": "Instrument Name - ACIS-CC",
            "short_name": "ACIS-CC",
            "id": "acis-cc-mock-id",
        },
        {
            "name": "Instrument Name - HRC",
            "short_name": "HRC",
            "id": "hrc-mock-id",
        },
        {
            "name": "Instrument Name - HRC-HETG",
            "short_name": "HRC-HETG",
            "id": "hrc-hetg-mock-id",
        },
        {
            "name": "Instrument Name - HRC-LETG",
            "short_name": "HRC-LETG",
            "id": "hrc-letg-mock-id",
        },
        {
            "name": "Instrument Name - HRC-Timing",
            "short_name": "HRC-Timing",
            "id": "hrc-timing-mock-id",
        },
    ]

    return [sdk.IDNameSchema(**info) for info in instruments]


@pytest.fixture
def fake_instruments_by_short_name(
    fake_instruments: list[sdk.IDNameSchema],
) -> dict[str, sdk.IDNameSchema]:
    return {
        (instrument.short_name or ""): instrument for instrument in fake_instruments
    }
