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
        "name": "TESS_sector_87",
        "telescope_id": "telescope_id",
        "status": sdk.ScheduleStatus.PLANNED,
        "fidelity": sdk.ScheduleFidelity.LOW,
        "date_range": sdk.DateRange(
            begin=datetime.fromisoformat("2024-12-18 00:00:00"),
            end=datetime.fromisoformat("2025-01-14 00:00:00"),
        ),
        "observations": [
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_87_placeholder",
                "external_observation_id": "TESS_sector_87_placeholder",
                "pointing_position": sdk.Coordinate(ra=97.9629, dec=-32.3927),
                "pointing_angle": 175.9167,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2024-12-18 00:00:00"),
                    end=datetime.fromisoformat("2025-01-14 00:00:00"),
                ),
                "exposure_time": 2332800,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            }
        ],
    },
    {
        "name": "TESS_sector_88",
        "telescope_id": "telescope_id",
        "status": sdk.ScheduleStatus.PLANNED,
        "fidelity": sdk.ScheduleFidelity.LOW,
        "date_range": sdk.DateRange(
            begin=datetime.fromisoformat("2025-01-14 00:00:00"),
            end=datetime.fromisoformat("2025-02-11 00:00:00"),
        ),
        "observations": [
            {
                "instrument_id": "instrument_id",
                "object_name": "TESS_sector_88_placeholder",
                "external_observation_id": "TESS_sector_88_placeholder",
                "pointing_position": sdk.Coordinate(ra=116.8315, dec=-35.7542),
                "pointing_angle": 163.0201,
                "date_range": sdk.DateRange(
                    begin=datetime.fromisoformat("2025-01-14 00:00:00"),
                    end=datetime.fromisoformat("2025-02-11 00:00:00"),
                ),
                "exposure_time": 2419200,
                "status": sdk.ObservationStatus.PLANNED,
                "type": sdk.ObservationType.IMAGING,
                "bandpass": bandpass,
            }
        ],
    },
]

expected = [sdk.ScheduleCreate.model_validate(schedule) for schedule in schedules]
