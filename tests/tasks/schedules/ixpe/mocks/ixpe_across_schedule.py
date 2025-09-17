from datetime import datetime

from across_data_ingestion.util.across_server import sdk

ixpe_across_schedule: sdk.ScheduleCreate = sdk.ScheduleCreate(
    telescope_id="ixpe_telescope_id",
    name="ixpe_ltp_2025-04-08_2025-09-08",
    date_range=sdk.DateRange(
        begin=datetime.fromisoformat("2025-04-08T06:00:00.000"),
        end=datetime.fromisoformat("2025-09-08T06:00:00.000"),
    ),
    status=sdk.ScheduleStatus.PLANNED,
    fidelity=sdk.ScheduleFidelity.LOW,
    observations=[
        sdk.ObservationCreate(
            instrument_id="ixpe_instrument_id",
            object_name="KES 75",
            pointing_position=sdk.Coordinate(ra=281.604, dec=-2.975),
            object_position=sdk.Coordinate(ra=281.604, dec=-2.975),
            date_range=sdk.DateRange(
                begin=datetime.fromisoformat("2025-04-08T06:00:00.000"),
                end=datetime.fromisoformat("2025-04-19T00:00:00.000"),
            ),
            external_observation_id="A_0_obs_2103",
            type=sdk.ObservationType.IMAGING,
            status=sdk.ObservationStatus.PLANNED,
            exposure_time=928800,
            bandpass=sdk.Bandpass(
                sdk.EnergyBandpass(
                    min=2.0,
                    max=8.0,
                    unit=sdk.EnergyUnit.KEV,
                    filter_name="IXPE",
                )
            ),
            pointing_angle=0.0,
        ),
        sdk.ObservationCreate(
            instrument_id="ixpe_instrument_id",
            object_name="SS 433 WEST",
            pointing_position=sdk.Coordinate(ra=287.672, dec=5.032),
            object_position=sdk.Coordinate(ra=287.672, dec=5.032),
            date_range=sdk.DateRange(
                begin=datetime.fromisoformat("2025-04-19T00:00:00.000"),
                end=datetime.fromisoformat("2025-04-24T12:00:00.000"),
            ),
            external_observation_id="A_0_obs_2022",
            type=sdk.ObservationType.IMAGING,
            status=sdk.ObservationStatus.PLANNED,
            exposure_time=475200,
            bandpass=sdk.Bandpass(
                sdk.EnergyBandpass(
                    min=2.0,
                    max=8.0,
                    unit=sdk.EnergyUnit.KEV,
                    filter_name="IXPE",
                )
            ),
            pointing_angle=0.0,
        ),
    ],
)
