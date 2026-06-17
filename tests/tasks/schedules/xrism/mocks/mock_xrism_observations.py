import datetime

from across_data_ingestion.util.across_server import sdk

mock_xrism_observations: list[dict] = [
    {
        "instrument_id": "resolve_instrument_uuid",
        "object_name": "Cygnus X-1",
        "pointing_position": {
            "ra": 299.590,
            "dec": 35.201,
        },
        "object_position": {
            "ra": 299.590,
            "dec": 35.201,
        },
        "date_range": {
            "begin": datetime.datetime(2026, 6, 4, 2, 21, 0),
            "end": datetime.datetime(2026, 6, 4, 16, 14, 20),
        },
        "external_observation_id": "12345",
        "type": "spectroscopy",
        "status": "planned",
        "pointing_angle": 0.0,
        "exposure_time": 50000,
        "bandpass": sdk.Bandpass(
            sdk.EnergyBandpass.model_validate(
                {
                    "filter_name": "XRISM Resolve",
                    "min": 1.7,
                    "max": 12.0,
                    "unit": sdk.EnergyUnit.KEV,
                }
            )
        ),
    },
]
