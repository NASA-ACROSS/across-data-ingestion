from across_data_ingestion.util.across_server import sdk

xmm_newton_planned_schedule: dict = {
    "name": "XMM_Newton_planned_2025-09-04_2025-09-05",
    "telescope_id": "telescope_uuid",
    "status": "planned",
    "fidelity": "low",
    "date_range": {"begin": "2025-09-04 08:40:22", "end": "2025-09-05 21:46:16"},
    "observations": [
        {
            "instrument_id": "epic-mos_instrument_uuid",
            "object_name": "Crab",
            "external_observation_id": "0811026501",
            "pointing_position": {"ra": 83.63333333333331, "dec": 22.014444444444443},
            "object_position": {"ra": 83.63333333333331, "dec": 22.014444444444443},
            "pointing_angle": 85.49,
            "date_range": {
                "begin": "2025-09-04 13:36:52",
                "end": "2025-09-04 16:33:32",
            },
            "exposure_time": 10600.0,
            "status": "planned",
            "type": "imaging",
            "bandpass": sdk.Bandpass(
                sdk.EnergyBandpass.model_validate(
                    {
                        "filter_name": "XMM-Newton EPIC",
                        "min": 0.3,
                        "max": 12.0,
                        "type": "ENERGY",
                        "unit": "keV",
                    },
                )
            ),
        },
        {
            "instrument_id": "epic-pn_instrument_uuid",
            "object_name": "Crab",
            "external_observation_id": "0811026501",
            "pointing_position": {"ra": 83.63333333333331, "dec": 22.014444444444443},
            "object_position": {"ra": 83.63333333333331, "dec": 22.014444444444443},
            "pointing_angle": 85.49,
            "date_range": {
                "begin": "2025-09-04 13:36:52",
                "end": "2025-09-04 16:00:12",
            },
            "exposure_time": 8600.0,
            "status": "planned",
            "type": "imaging",
            "bandpass": sdk.Bandpass(
                sdk.EnergyBandpass.model_validate(
                    {
                        "filter_name": "XMM-Newton EPIC",
                        "min": 0.3,
                        "max": 12.0,
                        "type": "ENERGY",
                        "unit": "keV",
                    },
                )
            ),
        },
        {
            "instrument_id": "rgs_instrument_uuid",
            "object_name": "Crab",
            "external_observation_id": "0811026501",
            "pointing_position": {"ra": 83.63333333333331, "dec": 22.014444444444443},
            "object_position": {"ra": 83.63333333333331, "dec": 22.014444444444443},
            "pointing_angle": 85.49,
            "date_range": {
                "begin": "2025-09-04 13:36:52",
                "end": "2025-09-04 16:40:12",
            },
            "exposure_time": 11000.0,
            "status": "planned",
            "type": "spectroscopy",
            "bandpass": sdk.Bandpass(
                sdk.EnergyBandpass.model_validate(
                    {
                        "filter_name": "XMM-Newton RGS",
                        "min": 0.35,
                        "max": 2.5,
                        "type": "ENERGY",
                        "unit": "keV",
                    },
                )
            ),
        },
        {
            "instrument_id": "om_instrument_uuid",
            "object_name": "Crab",
            "external_observation_id": "0811026501",
            "pointing_position": {"ra": 83.63333333333331, "dec": 22.014444444444443},
            "object_position": {"ra": 83.63333333333331, "dec": 22.014444444444443},
            "pointing_angle": 85.49,
            "date_range": {
                "begin": "2025-09-04 13:55:25",
                "end": "2025-09-04 16:06:33",
            },
            "exposure_time": 7868,
            "status": "planned",
            "type": "imaging",
            "bandpass": sdk.Bandpass(
                sdk.WavelengthBandpass.model_validate(
                    {
                        "filter_name": "OM UVM2",
                        "min": 207.0,
                        "max": 255.0,
                        "type": "WAVELENGTH",
                        "unit": "nm",
                    },
                )
            ),
        },
    ],
}
