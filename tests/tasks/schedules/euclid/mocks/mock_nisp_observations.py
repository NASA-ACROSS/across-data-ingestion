import datetime

from across_data_ingestion.util.across_server import sdk

mock_nisp_observations: list[dict] = [
    {
        "instrument_id": "nisp_instrument_uuid",
        "object_name": "WIDE",
        "pointing_position": {"ra": 123.45, "dec": -54.321},
        "date_range": {
            "begin": datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc),
            "end": datetime.datetime(
                2024, 6, 1, 12, 9, 34, tzinfo=datetime.timezone.utc
            ),
        },
        "external_observation_id": "1",
        "type": "spectroscopy",
        "status": "planned",
        "pointing_angle": 45.0,
        "exposure_time": 574,
        "bandpass": sdk.Bandpass(
            sdk.WavelengthBandpass.model_validate(
                {
                    "filter_name": "Euclid NISP Red Grism",
                    "min": 1250.0,
                    "max": 1850.0,
                    "type": "WAVELENGTH",
                    "unit": "nm",
                },
            )
        ),
    },
    {
        "instrument_id": "nisp_instrument_uuid",
        "object_name": "WIDE",
        "pointing_position": {"ra": 123.45, "dec": -54.321},
        "date_range": {
            "begin": datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc),
            "end": datetime.datetime(
                2024, 6, 1, 12, 1, 52, tzinfo=datetime.timezone.utc
            ),
        },
        "external_observation_id": "1",
        "type": "imaging",
        "status": "planned",
        "pointing_angle": 45.0,
        "exposure_time": 112,
        "bandpass": sdk.Bandpass(
            sdk.WavelengthBandpass.model_validate(
                {
                    "filter_name": "Euclid NISP Y",
                    "min": 920,
                    "max": 1146,
                    "type": "WAVELENGTH",
                    "unit": "nm",
                },
            )
        ),
    },
    {
        "instrument_id": "nisp_instrument_uuid",
        "object_name": "WIDE",
        "pointing_position": {"ra": 123.45, "dec": -54.321},
        "date_range": {
            "begin": datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc),
            "end": datetime.datetime(
                2024, 6, 1, 12, 1, 52, tzinfo=datetime.timezone.utc
            ),
        },
        "external_observation_id": "1",
        "type": "imaging",
        "status": "planned",
        "pointing_angle": 45.0,
        "exposure_time": 112,
        "bandpass": sdk.Bandpass(
            sdk.WavelengthBandpass.model_validate(
                {
                    "filter_name": "Euclid NISP J",
                    "min": 1146,
                    "max": 1372,
                    "type": "WAVELENGTH",
                    "unit": "nm",
                }
            )
        ),
    },
    {
        "instrument_id": "nisp_instrument_uuid",
        "object_name": "WIDE",
        "pointing_position": {"ra": 123.45, "dec": -54.321},
        "date_range": {
            "begin": datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc),
            "end": datetime.datetime(
                2024, 6, 1, 12, 1, 52, tzinfo=datetime.timezone.utc
            ),
        },
        "external_observation_id": "1",
        "type": "imaging",
        "status": "planned",
        "pointing_angle": 45.0,
        "exposure_time": 112,
        "bandpass": sdk.Bandpass(
            sdk.WavelengthBandpass.model_validate(
                {
                    "filter_name": "Euclid NISP H",
                    "min": 1372,
                    "max": 2000,
                    "type": "WAVELENGTH",
                    "unit": "nm",
                },
            )
        ),
    },
]
