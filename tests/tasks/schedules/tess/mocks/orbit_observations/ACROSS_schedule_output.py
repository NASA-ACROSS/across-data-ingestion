from datetime import datetime

from across_data_ingestion.util.across_server import sdk

bandpass = sdk.Bandpass(
    sdk.WavelengthBandpass(
        min=6000,
        max=10000,
        peak_wavelength=7865,
        unit=sdk.WavelengthUnit.ANGSTROM,
        filter_name="TESS_red",
    )
)

schedules = [
    {
        "name": "TESS_sector_85",
        "telescope_id": "telescope_id",
        "status": "planned",
        "fidelity": "low",
        "date_range": sdk.DateRange(
            begin=datetime.fromisoformat("2024-10-26 00:00:00"),
            end=datetime.fromisoformat("2024-11-21 00:00:00"),
        ),
        "observations": [
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_85_obs_0_orbit_177",
                "external_observation_id": "TESS_sector_85_obs_0_orbit_177",
                "pointing_position": sdk.Coordinate(ra=5.8776, dec=66.3525),
                "pointing_angle": 45.8046,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-10-27T01:20:00"),
                    end=datetime.fromisoformat("2024-11-02T11:45:00"),
                ),
                "exposure_time": 555900,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            },
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_85_obs_1_orbit_177",
                "external_observation_id": "TESS_sector_85_obs_1_orbit_177",
                "pointing_position": sdk.Coordinate(ra=5.8776, dec=66.3525),
                "pointing_angle": 45.8046,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-11-02T16:45:00"),
                    end=datetime.fromisoformat("2024-11-08T17:45:00"),
                ),
                "exposure_time": 522000,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            },
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_85_obs_2_orbit_178",
                "external_observation_id": "TESS_sector_85_obs_2_orbit_178",
                "pointing_position": sdk.Coordinate(ra=5.8776, dec=66.3525),
                "pointing_angle": 45.8046,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-11-08T22:45:00"),
                    end=datetime.fromisoformat("2024-11-15T06:15:00"),
                ),
                "exposure_time": 545400,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            },
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_85_obs_3_orbit_178",
                "external_observation_id": "TESS_sector_85_obs_3_orbit_178",
                "pointing_position": sdk.Coordinate(ra=5.8776, dec=66.3525),
                "pointing_angle": 45.8046,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-11-15T11:15:00"),
                    end=datetime.fromisoformat("2024-11-21T13:15:00"),
                ),
                "exposure_time": 525600,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            },
        ],
    },
    {
        "name": "TESS_sector_86",
        "telescope_id": "telescope_id",
        "status": "planned",
        "fidelity": "low",
        "date_range": sdk.DateRange(
            begin=datetime.fromisoformat("2024-11-21 00:00:00"),
            end=datetime.fromisoformat("2024-12-18 00:00:00"),
        ),
        "observations": [
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_86_obs_0_orbit_179",
                "external_observation_id": "TESS_sector_86_obs_0_orbit_179",
                "pointing_position": sdk.Coordinate(ra=42.6741, dec=75.893),
                "pointing_angle": 32.5811,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-11-21T18:15:00"),
                    end=datetime.fromisoformat("2024-11-28T06:15:00"),
                ),
                "exposure_time": 561600,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            },
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_86_obs_1_orbit_179",
                "external_observation_id": "TESS_sector_86_obs_1_orbit_179",
                "pointing_position": sdk.Coordinate(ra=42.6741, dec=75.893),
                "pointing_angle": 32.5811,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-11-28T11:15:00"),
                    end=datetime.fromisoformat("2024-12-05T02:45:00"),
                ),
                "exposure_time": 574200,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            },
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_86_obs_2_orbit_180",
                "external_observation_id": "TESS_sector_86_obs_2_orbit_180",
                "pointing_position": sdk.Coordinate(ra=42.6741, dec=75.893),
                "pointing_angle": 32.5811,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-12-05T07:45:00"),
                    end=datetime.fromisoformat("2024-12-11T08:00:00"),
                ),
                "exposure_time": 519300,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            },
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_86_obs_3_orbit_180",
                "external_observation_id": "TESS_sector_86_obs_3_orbit_180",
                "pointing_position": sdk.Coordinate(ra=42.6741, dec=75.893),
                "pointing_angle": 32.5811,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-12-11T13:00:00"),
                    end=datetime.fromisoformat("2024-12-18T08:00:00"),
                ),
                "exposure_time": 586800,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            },
        ],
    },
]

expected = [sdk.ScheduleCreate.model_validate(schedule) for schedule in schedules]
