from unittest.mock import MagicMock

import pytest
import structlog
from across.tools import tle
from across.tools.core.schemas.tle import TLE

import across_data_ingestion.tasks.tles.tle_ingestion as task
from across_data_ingestion.util.across_server import sdk


## MOCK BEHAVIOR ##
@pytest.fixture
def mock_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    # must be patched because it is set at runtime when the file is imported.
    mock = MagicMock(spec=structlog.stdlib.BoundLogger)
    monkeypatch.setattr(task, "logger", mock)
    return mock


@pytest.fixture
def fake_spacetrack_tle() -> TLE:
    return TLE(
        norad_id=123456,
        satellite_name="MOCK",
        tle1="1 25544U 98067A   08264.51782528 -.00002182  00000-0 -11606-4 0  2927",
        tle2="2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537",
    )


@pytest.fixture
def fake_tle_params() -> sdk.TLEParameters:
    return sdk.TLEParameters(norad_id=123456, norad_satellite_name="MOCK-1")


@pytest.fixture
def fake_tle_ephemeris_type(
    fake_tle_params: sdk.TLEParameters,
) -> sdk.ObservatoryEphemerisType:
    return sdk.ObservatoryEphemerisType(
        ephemeris_type=sdk.EphemerisType.TLE,
        priority=1,
        parameters=sdk.Parameters(fake_tle_params),
    )


@pytest.fixture
def fake_observatories(
    fake_observatory: sdk.Observatory,
    fake_tle_ephemeris_type: sdk.ObservatoryEphemerisType,
    fake_tle_params: sdk.TLEParameters,
) -> list[sdk.Observatory]:
    return [
        fake_observatory.model_copy(
            update={
                "name": "Space Obs 1",
                "ephemeris_types": [
                    fake_tle_ephemeris_type.model_copy(
                        update={
                            "parameters": sdk.Parameters(fake_tle_params),
                        }
                    )
                ],
            }
        ),
        fake_observatory.model_copy(
            update={
                "name": "Space Obs 2",
                "ephemeris_types": [
                    fake_tle_ephemeris_type.model_copy(
                        update={
                            "parameters": sdk.Parameters(
                                fake_tle_params.model_copy(
                                    update={
                                        "norad_id": 654321,
                                        "norad_satellite_name": "MOCK-2",
                                    }
                                )
                            )
                        }
                    )
                ],
            }
        ),
    ]


@pytest.fixture(autouse=True)
def mock_get_tle(
    monkeypatch: pytest.MonkeyPatch, fake_spacetrack_tle: TLE
) -> MagicMock:
    mock = MagicMock(return_value=fake_spacetrack_tle)

    monkeypatch.setattr(tle, "get_tle", mock)

    return mock


@pytest.fixture(autouse=True)
def set_observatories(
    mock_observatory_api: MagicMock, fake_observatories: list[sdk.Observatory]
) -> None:
    mock_observatory_api.get_observatories.return_value = fake_observatories
