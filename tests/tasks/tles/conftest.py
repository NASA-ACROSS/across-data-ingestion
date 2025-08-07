import pytest


@pytest.fixture
def mock_observatory_info() -> list[dict]:
    return [
        {
            "id": "uuid",
            "created_on": "2025-07-15T00:00:00",
            "name": "Mock Telescope",
            "short_name": "MT",
            "type": "SPACE_BASED",
            "telescopes": [
                {
                    "id": "uuid",
                    "name": "Mock Telescope",
                }
            ],
            "ephemeris_types": [
                {
                    "ephemeris_type": "tle",
                    "priority": 1,
                    "parameters": {"norad_id": 123456, "norad_satellite_name": "MOCK"},
                }
            ],
        }
    ]


@pytest.fixture
def mock_spacetrack_tle_response() -> dict:
    return {
        "norad_id": 123456,
        "satellite_name": "MOCK",
        "tle1": "1 25544U 98067A   08264.51782528 -.00002182  00000-0 -11606-4 0  2927",
        "tle2": "2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537",
    }


class MockTLEClass:
    def __init__(self, tle):
        self.tle = tle

    def model_dump(self):
        return self.tle


@pytest.fixture
def mock_tle(mock_spacetrack_tle_response) -> MockTLEClass:
    return MockTLEClass(mock_spacetrack_tle_response)
