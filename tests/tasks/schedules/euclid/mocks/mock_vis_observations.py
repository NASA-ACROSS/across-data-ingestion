import datetime

from across_data_ingestion.util.across_server import sdk

mock_vis_observations: list[dict] = [
    {
        "instrument_id": "vis_instrument_uuid",
        "object_name": "WIDE",
        "pointing_position": {
            "ra": 123.45,
            "dec": -54.321,
        },
        "date_range": {
            "begin": datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc),
            "end": datetime.datetime(
                2024, 6, 1, 12, 9, 26, tzinfo=datetime.timezone.utc
            ),
        },
        "external_observation_id": "1",
        "type": "imaging",
        "status": "planned",
        "pointing_angle": 45.0,
        "exposure_time": 566,
        "bandpass": sdk.Bandpass(
            sdk.WavelengthBandpass.model_validate(
                {
                    "filter_name": "Euclid VIS",
                    "min": 550.0,
                    "max": 990.0,
                    "type": "WAVELENGTH",
                    "unit": "nm",
                },
            )
        ),
    },
]
