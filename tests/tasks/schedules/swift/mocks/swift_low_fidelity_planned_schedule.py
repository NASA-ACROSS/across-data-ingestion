from across_data_ingestion.util.across_server import sdk

expected_xrt = sdk.ScheduleCreate.model_validate(
    {
        "telescope_id": "test-telescope-id",
        "name": "swift_xrt_low_fidelity_planned_2025-07-01_2025-07-01",
        "date_range": {
            "begin": "2025-07-01T00:05:00.000",
            "end": "2025-07-01T02:56:00.000",
        },
        "status": "planned",
        "fidelity": "low",
        "observations": [
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AU_Mic",
                "pointing_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "object_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "date_range": {
                    "begin": "2025-07-01T00:05:00.000",
                    "end": "2025-07-01T00:24:00.000",
                },
                "external_observation_id": "03400165002",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 112.349661701711,
                "exposure_time": 1140,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        }
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2021LO",
                "pointing_position": {"ra": 46.1337622543611, "dec": 4.7376256643615},
                "object_position": {"ra": 46.1337622543611, "dec": 4.7376256643615},
                "date_range": {
                    "begin": "2025-07-01T00:24:00.000",
                    "end": "2025-07-01T00:34:00.000",
                },
                "external_observation_id": "00098017005",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 71.3057956961124,
                "exposure_time": 600,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "SMDG0609099",
                "pointing_position": {"ra": 92.2998402125025, "dec": -32.4468146190973},
                "object_position": {"ra": 92.2998402125025, "dec": -32.4468146190973},
                "date_range": {
                    "begin": "2025-07-01T00:34:00.000",
                    "end": "2025-07-01T00:56:00.000",
                },
                "external_observation_id": "00098319009",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 16.8560250213805,
                "exposure_time": 1320,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2025mvn",
                "pointing_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "object_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "date_range": {
                    "begin": "2025-07-01T00:56:00.000",
                    "end": "2025-07-01T01:06:00.000",
                },
                "external_observation_id": "00019849011",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 296.0906229627,
                "exposure_time": 600,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "object_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "date_range": {
                    "begin": "2025-07-01T01:06:00.000",
                    "end": "2025-07-01T01:16:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 294.668441343903,
                "exposure_time": 600,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "1ES 2344+514",
                "pointing_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "object_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "date_range": {
                    "begin": "2025-07-01T01:39:00.000",
                    "end": "2025-07-01T01:59:00.000",
                },
                "external_observation_id": "00015413065",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 56.7223179843065,
                "exposure_time": 1200,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "PBCJ0106.8+0637",
                "pointing_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "object_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "date_range": {
                    "begin": "2025-07-01T01:59:00.000",
                    "end": "2025-07-01T02:12:00.000",
                },
                "external_observation_id": "00080011005",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 69.7798380920218,
                "exposure_time": 780,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "SRGE J095928.6+64302",
                "pointing_position": {"ra": 149.822427659297, "dec": 64.5209607143038},
                "object_position": {"ra": 149.822427659297, "dec": 64.5209607143038},
                "date_range": {
                    "begin": "2025-07-01T02:12:00.000",
                    "end": "2025-07-01T02:26:00.000",
                },
                "external_observation_id": "00098022003",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 233.538345432991,
                "exposure_time": 840,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "object_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "date_range": {
                    "begin": "2025-07-01T02:26:00.000",
                    "end": "2025-07-01T02:51:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 297.800768996191,
                "exposure_time": 1500,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "ZTF19acnskyy",
                "pointing_position": {"ra": 203.810476124553, "dec": 7.4570433418871},
                "object_position": {"ra": 203.810476124553, "dec": 7.4570433418871},
                "date_range": {
                    "begin": "2025-07-01T02:51:00.000",
                    "end": "2025-07-01T02:56:00.000",
                },
                "external_observation_id": "00019876049",
                "type": "spectroscopy",
                "status": "planned",
                "pointing_angle": 297.845176203749,
                "exposure_time": 300,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift XRT",
                            "min": 0.3,
                            "max": 10.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
        ],
    }
)

expected_bat = sdk.ScheduleCreate.model_validate(
    {
        "telescope_id": "test-telescope-id",
        "name": "swift_bat_low_fidelity_planned_2025-07-01_2025-07-01",
        "date_range": {
            "begin": "2025-07-01T00:05:00.000",
            "end": "2025-07-01T02:56:00.000",
        },
        "status": "planned",
        "fidelity": "low",
        "observations": [
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AU_Mic",
                "pointing_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "object_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "date_range": {
                    "begin": "2025-07-01T00:05:00.000",
                    "end": "2025-07-01T00:24:00.000",
                },
                "external_observation_id": "03400165002",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 112.349661701711,
                "exposure_time": 1140,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2021LO",
                "pointing_position": {"ra": 46.1337622543611, "dec": 4.7376256643615},
                "object_position": {"ra": 46.1337622543611, "dec": 4.7376256643615},
                "date_range": {
                    "begin": "2025-07-01T00:24:00.000",
                    "end": "2025-07-01T00:34:00.000",
                },
                "external_observation_id": "00098017005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 71.3057956961124,
                "exposure_time": 600,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "SMDG0609099",
                "pointing_position": {"ra": 92.2998402125025, "dec": -32.4468146190973},
                "object_position": {"ra": 92.2998402125025, "dec": -32.4468146190973},
                "date_range": {
                    "begin": "2025-07-01T00:34:00.000",
                    "end": "2025-07-01T00:56:00.000",
                },
                "external_observation_id": "00098319009",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 16.8560250213805,
                "exposure_time": 1320,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2025mvn",
                "pointing_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "object_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "date_range": {
                    "begin": "2025-07-01T00:56:00.000",
                    "end": "2025-07-01T01:06:00.000",
                },
                "external_observation_id": "00019849011",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 296.0906229627,
                "exposure_time": 600,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "object_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "date_range": {
                    "begin": "2025-07-01T01:06:00.000",
                    "end": "2025-07-01T01:16:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 294.668441343903,
                "exposure_time": 600,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "1ES 2344+514",
                "pointing_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "object_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "date_range": {
                    "begin": "2025-07-01T01:39:00.000",
                    "end": "2025-07-01T01:59:00.000",
                },
                "external_observation_id": "00015413065",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 56.7223179843065,
                "exposure_time": 1200,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "PBCJ0106.8+0637",
                "pointing_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "object_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "date_range": {
                    "begin": "2025-07-01T01:59:00.000",
                    "end": "2025-07-01T02:12:00.000",
                },
                "external_observation_id": "00080011005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 69.7798380920218,
                "exposure_time": 780,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "SRGE J095928.6+64302",
                "pointing_position": {"ra": 149.822427659297, "dec": 64.5209607143038},
                "object_position": {"ra": 149.822427659297, "dec": 64.5209607143038},
                "date_range": {
                    "begin": "2025-07-01T02:12:00.000",
                    "end": "2025-07-01T02:26:00.000",
                },
                "external_observation_id": "00098022003",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 233.538345432991,
                "exposure_time": 840,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "object_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "date_range": {
                    "begin": "2025-07-01T02:26:00.000",
                    "end": "2025-07-01T02:51:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.800768996191,
                "exposure_time": 1500,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "ZTF19acnskyy",
                "pointing_position": {"ra": 203.810476124553, "dec": 7.4570433418871},
                "object_position": {"ra": 203.810476124553, "dec": 7.4570433418871},
                "date_range": {
                    "begin": "2025-07-01T02:51:00.000",
                    "end": "2025-07-01T02:56:00.000",
                },
                "external_observation_id": "00019876049",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.845176203749,
                "exposure_time": 300,
                "bandpass": sdk.Bandpass(
                    sdk.EnergyBandpass.model_validate(
                        {
                            "filter_name": "Swift BAT",
                            "min": 15.0,
                            "max": 150.0,
                            "unit": "keV",
                        },
                    )
                ),
            },
        ],
    }
)

expected_uvot = sdk.ScheduleCreate.model_validate(
    {
        "telescope_id": "test-telescope-id",
        "name": "swift_uvot_low_fidelity_planned_2025-07-01_2025-07-01",
        "date_range": {
            "begin": "2025-07-01T00:05:00.000",
            "end": "2025-07-01T02:56:00.000",
        },
        "status": "planned",
        "fidelity": "low",
        "observations": [
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AU_Mic",
                "pointing_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "object_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "date_range": {
                    "begin": "2025-07-01T00:05:00.000",
                    "end": "2025-07-01T00:24:00.000",
                },
                "external_observation_id": "03400165002",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 112.349661701711,
                "exposure_time": 380.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw1",
                            "min": 226,
                            "max": 294,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AU_Mic",
                "pointing_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "object_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "date_range": {
                    "begin": "2025-07-01T00:05:00.000",
                    "end": "2025-07-01T00:24:00.000",
                },
                "external_observation_id": "03400165002",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 112.349661701711,
                "exposure_time": 380.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AU_Mic",
                "pointing_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "object_position": {"ra": 311.316788437311, "dec": -31.331389710256},
                "date_range": {
                    "begin": "2025-07-01T00:05:00.000",
                    "end": "2025-07-01T00:24:00.000",
                },
                "external_observation_id": "03400165002",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 112.349661701711,
                "exposure_time": 380.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvm2",
                            "min": 200,
                            "max": 249,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2021LO",
                "pointing_position": {"ra": 46.1337622543611, "dec": 4.7376256643615},
                "object_position": {"ra": 46.1337622543611, "dec": 4.7376256643615},
                "date_range": {
                    "begin": "2025-07-01T00:24:00.000",
                    "end": "2025-07-01T00:34:00.000",
                },
                "external_observation_id": "00098017005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 71.3057956961124,
                "exposure_time": 600.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "SMDG0609099",
                "pointing_position": {"ra": 92.2998402125025, "dec": -32.4468146190973},
                "object_position": {"ra": 92.2998402125025, "dec": -32.4468146190973},
                "date_range": {
                    "begin": "2025-07-01T00:34:00.000",
                    "end": "2025-07-01T00:56:00.000",
                },
                "external_observation_id": "00098319009",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 16.8560250213805,
                "exposure_time": 1320.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvm2",
                            "min": 200,
                            "max": 249,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2025mvn",
                "pointing_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "object_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "date_range": {
                    "begin": "2025-07-01T00:56:00.000",
                    "end": "2025-07-01T01:06:00.000",
                },
                "external_observation_id": "00019849011",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 296.0906229627,
                "exposure_time": 31.57894736842105,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT u",
                            "min": 308,
                            "max": 385,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2025mvn",
                "pointing_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "object_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "date_range": {
                    "begin": "2025-07-01T00:56:00.000",
                    "end": "2025-07-01T01:06:00.000",
                },
                "external_observation_id": "00019849011",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 296.0906229627,
                "exposure_time": 31.57894736842105,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT b",
                            "min": 391,
                            "max": 487,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2025mvn",
                "pointing_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "object_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "date_range": {
                    "begin": "2025-07-01T00:56:00.000",
                    "end": "2025-07-01T01:06:00.000",
                },
                "external_observation_id": "00019849011",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 296.0906229627,
                "exposure_time": 31.57894736842105,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT v",
                            "min": 509,
                            "max": 585,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2025mvn",
                "pointing_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "object_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "date_range": {
                    "begin": "2025-07-01T00:56:00.000",
                    "end": "2025-07-01T01:06:00.000",
                },
                "external_observation_id": "00019849011",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 296.0906229627,
                "exposure_time": 94.73684210526315,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw1",
                            "min": 226,
                            "max": 294,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2025mvn",
                "pointing_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "object_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "date_range": {
                    "begin": "2025-07-01T00:56:00.000",
                    "end": "2025-07-01T01:06:00.000",
                },
                "external_observation_id": "00019849011",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 296.0906229627,
                "exposure_time": 157.89473684210526,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "AT2025mvn",
                "pointing_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "object_position": {"ra": 198.338212728804, "dec": 36.5755879300934},
                "date_range": {
                    "begin": "2025-07-01T00:56:00.000",
                    "end": "2025-07-01T01:06:00.000",
                },
                "external_observation_id": "00019849011",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 296.0906229627,
                "exposure_time": 252.6315789473684,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvm2",
                            "min": 200,
                            "max": 249,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "object_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "date_range": {
                    "begin": "2025-07-01T01:06:00.000",
                    "end": "2025-07-01T01:16:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 294.668441343903,
                "exposure_time": 50.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT u",
                            "min": 308,
                            "max": 385,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "object_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "date_range": {
                    "begin": "2025-07-01T01:06:00.000",
                    "end": "2025-07-01T01:16:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 294.668441343903,
                "exposure_time": 50.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT b",
                            "min": 391,
                            "max": 487,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "object_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "date_range": {
                    "begin": "2025-07-01T01:06:00.000",
                    "end": "2025-07-01T01:16:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 294.668441343903,
                "exposure_time": 50.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT v",
                            "min": 509,
                            "max": 585,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "object_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "date_range": {
                    "begin": "2025-07-01T01:06:00.000",
                    "end": "2025-07-01T01:16:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 294.668441343903,
                "exposure_time": 100.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw1",
                            "min": 226,
                            "max": 294,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "object_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "date_range": {
                    "begin": "2025-07-01T01:06:00.000",
                    "end": "2025-07-01T01:16:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 294.668441343903,
                "exposure_time": 200.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "object_position": {"ra": 189.396153617633, "dec": 26.6971305691044},
                "date_range": {
                    "begin": "2025-07-01T01:06:00.000",
                    "end": "2025-07-01T01:16:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 294.668441343903,
                "exposure_time": 150.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvm2",
                            "min": 200,
                            "max": 249,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "1ES 2344+514",
                "pointing_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "object_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "date_range": {
                    "begin": "2025-07-01T01:39:00.000",
                    "end": "2025-07-01T01:59:00.000",
                },
                "external_observation_id": "00015413065",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 56.7223179843065,
                "exposure_time": 100.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT u",
                            "min": 308,
                            "max": 385,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "1ES 2344+514",
                "pointing_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "object_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "date_range": {
                    "begin": "2025-07-01T01:39:00.000",
                    "end": "2025-07-01T01:59:00.000",
                },
                "external_observation_id": "00015413065",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 56.7223179843065,
                "exposure_time": 100.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT b",
                            "min": 391,
                            "max": 487,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "1ES 2344+514",
                "pointing_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "object_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "date_range": {
                    "begin": "2025-07-01T01:39:00.000",
                    "end": "2025-07-01T01:59:00.000",
                },
                "external_observation_id": "00015413065",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 56.7223179843065,
                "exposure_time": 100.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT v",
                            "min": 509,
                            "max": 585,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "1ES 2344+514",
                "pointing_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "object_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "date_range": {
                    "begin": "2025-07-01T01:39:00.000",
                    "end": "2025-07-01T01:59:00.000",
                },
                "external_observation_id": "00015413065",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 56.7223179843065,
                "exposure_time": 200.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw1",
                            "min": 226,
                            "max": 294,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "1ES 2344+514",
                "pointing_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "object_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "date_range": {
                    "begin": "2025-07-01T01:39:00.000",
                    "end": "2025-07-01T01:59:00.000",
                },
                "external_observation_id": "00015413065",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 56.7223179843065,
                "exposure_time": 400.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "1ES 2344+514",
                "pointing_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "object_position": {"ra": 356.803916050696, "dec": 51.6912777433994},
                "date_range": {
                    "begin": "2025-07-01T01:39:00.000",
                    "end": "2025-07-01T01:59:00.000",
                },
                "external_observation_id": "00015413065",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 56.7223179843065,
                "exposure_time": 300.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvm2",
                            "min": 200,
                            "max": 249,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "PBCJ0106.8+0637",
                "pointing_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "object_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "date_range": {
                    "begin": "2025-07-01T01:59:00.000",
                    "end": "2025-07-01T02:12:00.000",
                },
                "external_observation_id": "00080011005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 69.7798380920218,
                "exposure_time": 65.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT u",
                            "min": 308,
                            "max": 385,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "PBCJ0106.8+0637",
                "pointing_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "object_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "date_range": {
                    "begin": "2025-07-01T01:59:00.000",
                    "end": "2025-07-01T02:12:00.000",
                },
                "external_observation_id": "00080011005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 69.7798380920218,
                "exposure_time": 65.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT b",
                            "min": 391,
                            "max": 487,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "PBCJ0106.8+0637",
                "pointing_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "object_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "date_range": {
                    "begin": "2025-07-01T01:59:00.000",
                    "end": "2025-07-01T02:12:00.000",
                },
                "external_observation_id": "00080011005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 69.7798380920218,
                "exposure_time": 65.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT v",
                            "min": 509,
                            "max": 585,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "PBCJ0106.8+0637",
                "pointing_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "object_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "date_range": {
                    "begin": "2025-07-01T01:59:00.000",
                    "end": "2025-07-01T02:12:00.000",
                },
                "external_observation_id": "00080011005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 69.7798380920218,
                "exposure_time": 130.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw1",
                            "min": 226,
                            "max": 294,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "PBCJ0106.8+0637",
                "pointing_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "object_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "date_range": {
                    "begin": "2025-07-01T01:59:00.000",
                    "end": "2025-07-01T02:12:00.000",
                },
                "external_observation_id": "00080011005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 69.7798380920218,
                "exposure_time": 260.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "PBCJ0106.8+0637",
                "pointing_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "object_position": {"ra": 16.7119499999898, "dec": 6.625247730813},
                "date_range": {
                    "begin": "2025-07-01T01:59:00.000",
                    "end": "2025-07-01T02:12:00.000",
                },
                "external_observation_id": "00080011005",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 69.7798380920218,
                "exposure_time": 195.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvm2",
                            "min": 200,
                            "max": 249,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "SRGE J095928.6+64302",
                "pointing_position": {"ra": 149.822427659297, "dec": 64.5209607143038},
                "object_position": {"ra": 149.822427659297, "dec": 64.5209607143038},
                "date_range": {
                    "begin": "2025-07-01T02:12:00.000",
                    "end": "2025-07-01T02:26:00.000",
                },
                "external_observation_id": "00098022003",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 233.538345432991,
                "exposure_time": 840.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "object_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "date_range": {
                    "begin": "2025-07-01T02:26:00.000",
                    "end": "2025-07-01T02:51:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.800768996191,
                "exposure_time": 125.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT u",
                            "min": 308,
                            "max": 385,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "object_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "date_range": {
                    "begin": "2025-07-01T02:26:00.000",
                    "end": "2025-07-01T02:51:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.800768996191,
                "exposure_time": 125.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT b",
                            "min": 391,
                            "max": 487,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "object_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "date_range": {
                    "begin": "2025-07-01T02:26:00.000",
                    "end": "2025-07-01T02:51:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.800768996191,
                "exposure_time": 125.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT v",
                            "min": 509,
                            "max": 585,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "object_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "date_range": {
                    "begin": "2025-07-01T02:26:00.000",
                    "end": "2025-07-01T02:51:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.800768996191,
                "exposure_time": 250.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw1",
                            "min": 226,
                            "max": 294,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "object_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "date_range": {
                    "begin": "2025-07-01T02:26:00.000",
                    "end": "2025-07-01T02:51:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.800768996191,
                "exposure_time": 500.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "IC 3599",
                "pointing_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "object_position": {"ra": 189.396830055153, "dec": 26.6959048902536},
                "date_range": {
                    "begin": "2025-07-01T02:26:00.000",
                    "end": "2025-07-01T02:51:00.000",
                },
                "external_observation_id": "00010375133",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.800768996191,
                "exposure_time": 375.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvm2",
                            "min": 200,
                            "max": 249,
                            "unit": "nm",
                        },
                    )
                ),
            },
            {
                "instrument_id": "test-instrument-id",
                "object_name": "ZTF19acnskyy",
                "pointing_position": {"ra": 203.810476124553, "dec": 7.4570433418871},
                "object_position": {"ra": 203.810476124553, "dec": 7.4570433418871},
                "date_range": {
                    "begin": "2025-07-01T02:51:00.000",
                    "end": "2025-07-01T02:56:00.000",
                },
                "external_observation_id": "00019876049",
                "type": "imaging",
                "status": "planned",
                "pointing_angle": 297.845176203749,
                "exposure_time": 300.0,
                "bandpass": sdk.Bandpass(
                    sdk.WavelengthBandpass.model_validate(
                        {
                            "filter_name": "Swift UVOT uvw2",
                            "min": 160,
                            "max": 225,
                            "unit": "nm",
                        },
                    )
                ),
            },
        ],
    }
)
