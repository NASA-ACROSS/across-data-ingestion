from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
import structlog

import across_data_ingestion.tasks.schedules.xmm_newton.low_fidelity_planned as task
from across_data_ingestion.util.across_server import sdk


@pytest.fixture
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    # must be patched because it is set at runtime when the file is imported.
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task, "logger", mock)
    return mock


@pytest.fixture(autouse=True)
def set_sdk_data(
    mock_telescope_api: MagicMock,
    fake_telescope: sdk.Telescope,
) -> None:
    mock_telescope_api.get_telescopes.return_value = [fake_telescope]


@pytest.fixture
def fake_instrument_id() -> str:
    return "fake_instrument_id"


@pytest.fixture
def fake_telescope() -> sdk.Telescope:
    return sdk.Telescope(
        id="telescope_uuid",
        name="X-ray Multi-Mirror Mission",
        short_name="XMM-Newton",
        created_on=datetime.now(),
        instruments=[
            sdk.TelescopeInstrument(
                id="epic-mos_instrument_uuid",
                name="European Photon Imaging Camera - MOS",
                short_name="EPIC-MOS",
                created_on=datetime.now(),
            ),
            sdk.TelescopeInstrument(
                id="epic-pn_instrument_uuid",
                name="European Photon Imaging Camera - pn",
                short_name="EPIC-PN",
                created_on=datetime.now(),
            ),
            sdk.TelescopeInstrument(
                id="rgs_instrument_uuid",
                name="Reflection Grating Spectrometer",
                short_name="RGS",
                created_on=datetime.now(),
            ),
            sdk.TelescopeInstrument(
                id="om_instrument_uuid",
                name="Optical Monitor",
                short_name="OM",
                created_on=datetime.now(),
            ),
        ],
    )


@pytest.fixture
def fake_instrument(
    fake_instrument_id: str,
    fake_telescope: sdk.Telescope,
    fake_filters: list[sdk.Filter],
) -> sdk.Instrument:
    return sdk.Instrument(
        id=fake_instrument_id,
        name="FAKE XMM INSTRUMENT",
        short_name="XMM_FAKE",
        created_on=datetime.now(),
        filters=fake_filters,
        telescope=sdk.IDNameSchema(
            id=fake_telescope.id,
            name=fake_telescope.name,
            short_name=fake_telescope.short_name,
        ),
    )


@pytest.fixture
def fake_filters(fake_instrument_id: str) -> list[sdk.Filter]:
    filters = [
        {
            "id": "fake_filter_id_1",
            "name": "FAKE XMM FILTER",
            "short_name": "XMM FAKE ABCD",
            "min_wavelength": 2000.0,
            "max_wavelength": 4000.0,
            "instrument_id": fake_instrument_id,
            "created_on": datetime.now(),
            "peak_wavelength": None,
            "reference_url": None,
            "sensitivity_depth": None,
            "sensitivity_depth_unit": None,
            "sensitivity_time_seconds": None,
            "is_operational": True,
        },
        {
            "id": "fake_filter_id_2",
            "name": "FAKE HST FILTER 2",
            "short_name": "HST FAKE WXYZ",
            "min_wavelength": 3500.0,
            "max_wavelength": 5500.0,
            "instrument_id": fake_instrument_id,
            "created_on": datetime.now(),
            "peak_wavelength": None,
            "reference_url": None,
            "sensitivity_depth": None,
            "sensitivity_depth_unit": None,
            "sensitivity_time_seconds": None,
            "is_operational": True,
        },
    ]
    return [sdk.Filter.model_validate(f) for f in filters]


@pytest.fixture
def mock_planned_schedule_table() -> pd.DataFrame:
    return pd.read_csv(
        "tests/tasks/schedules/xmm_newton/mocks/planned_schedule_table.csv"
    )


@pytest.fixture
def mock_revolution_timeline_file() -> pd.DataFrame:
    return pd.read_csv(
        "tests/tasks/schedules/xmm_newton/mocks/revolution_timeline_file.csv"
    )


@pytest.fixture(autouse=True)
def mock_read_planned_schedule_table(
    monkeypatch: pytest.MonkeyPatch,
    mock_planned_schedule_table: pd.DataFrame,
) -> MagicMock:
    mock = MagicMock(return_value=mock_planned_schedule_table)
    monkeypatch.setattr(task, "read_planned_schedule_table", mock)
    return mock


@pytest.fixture(autouse=True)
def mock_read_revolution_timeline_file(
    monkeypatch: pytest.MonkeyPatch,
    mock_revolution_timeline_file: pd.DataFrame,
) -> MagicMock:
    mock = MagicMock(return_value=mock_revolution_timeline_file)
    monkeypatch.setattr(task, "read_revolution_timeline_file", mock)
    return mock
