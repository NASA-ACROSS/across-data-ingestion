from datetime import datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from across_data_ingestion.util.across_server import sdk


def mock_repeat_every(func):
    return func


def mock_repeat_at(func):
    return func


# MUST MOCK DECORATOR BEFORE THE UNIT UNDER TEST GETS IMPORTED!
patch(
    "fastapi_utils.tasks.repeat_every", lambda *args, **kwargs: mock_repeat_every
).start()

patch("fastapi_utilities.repeat_at", lambda *args, **kwargs: mock_repeat_at).start()


@pytest.fixture
def fake_httpx_response() -> MagicMock:
    mock = MagicMock(spec=httpx.Response)
    mock.text = "mock response text"

    return mock


@pytest.fixture(autouse=True)
def mock_httpx_get(
    monkeypatch: pytest.MonkeyPatch, fake_httpx_response: MagicMock
) -> MagicMock:
    mock_get = MagicMock(return_value=fake_httpx_response)
    monkeypatch.setattr(httpx, "get", mock_get)
    return mock_get


## ACROSS SERVER SDK ##


### INSTRUMENT ###
@pytest.fixture
def fake_instrument() -> sdk.Instrument:
    return sdk.Instrument(
        id="instrument-id",
        created_on=datetime.now(),
        name="Test instrument",
        short_name="tt",
        filters=[
            sdk.Filter(
                id="filter-id",
                created_on=datetime.now(),
                name="fake filter",
                instrument_id="instrument-id",
                peak_wavelength=50,
                min_wavelength=0,
                max_wavelength=100,
                is_operational=True,
                sensitivity_depth=None,
                sensitivity_depth_unit=None,
                sensitivity_time_seconds=None,
                reference_url=None,
            )
        ],
    )


@pytest.fixture
def mock_instrument_api(fake_instrument: sdk.Instrument) -> MagicMock:
    mock = MagicMock()
    mock.get_instruments = MagicMock(return_value=[fake_instrument])

    return mock


@pytest.fixture
def mock_instrument_api_cls(mock_instrument_api: MagicMock) -> MagicMock:
    mock_cls = MagicMock(return_value=mock_instrument_api)
    return mock_cls


### END INSTRUMENT ###


### TELESCOPE ###
@pytest.fixture
def fake_telescope() -> sdk.Telescope:
    return sdk.Telescope(
        id="test-telescope-id",
        created_on=datetime.now(),
        name="Test Telescope",
        short_name="tt",
        instruments=[
            sdk.TelescopeInstrument(
                id="test-instrument-id",
                name="Test Instrument",
                short_name="ti",
                created_on=datetime.now(),
            )
        ],
    )


@pytest.fixture
def mock_telescope_api(fake_telescope: sdk.Telescope) -> MagicMock:
    mock = MagicMock()
    mock.get_telescopes = MagicMock(return_value=[fake_telescope])

    return mock


@pytest.fixture
def mock_telescope_api_cls(mock_telescope_api: MagicMock) -> MagicMock:
    mock_cls = MagicMock(return_value=mock_telescope_api)
    return mock_cls


### END TELESCOPE ###


### OBSERVATORY ###
@pytest.fixture
def fake_observatory() -> sdk.Observatory:
    return sdk.Observatory(
        id="uuid",
        created_on=datetime.fromisoformat("2025-07-15T00:00:00"),
        name="Treedome Space Observatory",
        short_name="MT",
        type=sdk.ObservatoryType.SPACE_BASED,
        reference_url=None,
        operational=sdk.NullableDateRange(
            begin=datetime.fromisoformat("2025-07-15T00:00:00"), end=None
        ),
        telescopes=[
            sdk.IDNameSchema(
                id="uuid",
                name="Treedome Telescope",
                short_name="tree",
            )
        ],
        ephemeris_types=[
            sdk.ObservatoryEphemerisType(
                ephemeris_type=sdk.EphemerisType.TLE,
                priority=1,
                parameters=sdk.Parameters(
                    sdk.TLEParameters(norad_id=123456, norad_satellite_name="MOCK")
                ),
            )
        ],
    )


@pytest.fixture
def mock_observatory_api(fake_observatory: sdk.Telescope) -> MagicMock:
    mock = MagicMock()
    mock.get_observatories = MagicMock(return_value=[fake_observatory])

    return mock


@pytest.fixture
def mock_observatory_api_cls(mock_observatory_api: MagicMock) -> MagicMock:
    mock_cls = MagicMock(return_value=mock_observatory_api)
    return mock_cls


### END OBSERVATORY ###


### SCHEDULE ###
@pytest.fixture
def mock_schedule_api() -> MagicMock:
    mock = MagicMock()
    mock.create_schedule = MagicMock()
    mock.create_many_schedules = MagicMock()

    return mock


@pytest.fixture
def mock_schedule_api_cls(mock_schedule_api: MagicMock) -> MagicMock:
    mock_cls = MagicMock(return_value=mock_schedule_api)
    return mock_cls


### END SCHEDULE ###


### TLE ###
@pytest.fixture
def mock_tle_api() -> MagicMock:
    mock = MagicMock()
    mock.create_tle = MagicMock()

    return mock


@pytest.fixture
def mock_tle_api_cls(mock_tle_api: MagicMock) -> MagicMock:
    mock_cls = MagicMock(return_value=mock_tle_api)
    return mock_cls


### END TLE ###


### PATCH SDK ###
@pytest.fixture(autouse=True)
def patch_sdk(
    monkeypatch: pytest.MonkeyPatch,
    mock_telescope_api_cls: MagicMock,
    mock_schedule_api_cls: MagicMock,
    mock_instrument_api_cls: MagicMock,
    mock_observatory_api_cls: MagicMock,
    mock_tle_api_cls: MagicMock,
) -> None:
    monkeypatch.setattr(sdk, "TelescopeApi", mock_telescope_api_cls)
    monkeypatch.setattr(sdk, "ScheduleApi", mock_schedule_api_cls)
    monkeypatch.setattr(sdk, "InstrumentApi", mock_instrument_api_cls)
    monkeypatch.setattr(sdk, "ObservatoryApi", mock_observatory_api_cls)
    monkeypatch.setattr(sdk, "TLEApi", mock_tle_api_cls)


### END PATCH SDK ###

## END ACROSS SERVER SDK ##
