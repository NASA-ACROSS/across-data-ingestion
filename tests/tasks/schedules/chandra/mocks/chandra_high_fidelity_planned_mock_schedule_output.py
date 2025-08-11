chandra_planned_schedule: dict = {
    "telescope_id": "chandra-mock-telescope-id",
    "name": "chandra_high_fidelity_planned_2025-06-30_2025-06-30",
    "date_range": {"begin": "2025-06-30T22:23:23", "end": "2025-06-30T22:23:23"},
    "status": "scheduled",
    "fidelity": "high",
    "observations": [
        {
            "instrument_id": "acis-mock-id",
            "object_name": "Abell 370",
            "pointing_position": {
                "ra": "39.96041666666667",
                "dec": "-1.5856000000000001",
            },
            "object_position": {
                "ra": "39.96041666666667",
                "dec": "-1.5856000000000001",
            },
            "date_range": {
                "begin": "2025-06-30T22:23:23",
                "end": "2025-07-01T03:56:43.000",
            },
            "external_observation_id": "28845",
            "type": "imaging",
            "status": "scheduled",
            "pointing_angle": 0.0,
            "exposure_time": 20000.0,
            "bandpass": {
                "filter_name": "Chandra ACIS",
                "min": 0.1,
                "max": 10.0,
                "type": "ENERGY",
                "unit": "keV",
            },
        }
    ],
}
