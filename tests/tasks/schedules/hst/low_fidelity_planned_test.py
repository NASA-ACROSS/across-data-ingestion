import re
from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

import across_data_ingestion.tasks.schedules.hst.low_fidelity_planned as task
from across_data_ingestion.tasks.schedules.hst.low_fidelity_planned import (
    extract_instrument_info,
    extract_observation_pointing_coordinates,
    get_latest_timeline_file,
    ingest,
    read_planned_exposure_catalog,
    read_timeline_file,
    transform_to_across_observation,
)
from across_data_ingestion.tasks.schedules.types import Position
from across_data_ingestion.util.across_server import sdk


class TestHSTLowFidelityPlannedScheduleIngestionTask:
    class TestIngest:
        @pytest.fixture(autouse=True)
        def patch_read_timeline_file(
            self,
            monkeypatch: pytest.MonkeyPatch,
            fake_instrument: sdk.Instrument,
        ) -> None:
            """Should create ACROSS schedule"""
            monkeypatch.setattr(
                task,
                "extract_observation_pointing_coordinates",
                MagicMock(
                    return_value=Position(
                        ra="1:1:1",
                        dec="1:1:1",
                    )
                ),
            )

            monkeypatch.setattr(
                task,
                "extract_instrument_info",
                MagicMock(
                    return_value=task.InstrumentInfo(
                        id=fake_instrument.id,
                        bandpass=sdk.Bandpass(
                            sdk.WavelengthBandpass(
                                filter_name="fake filter",
                                min=0,
                                max=100,
                                unit=sdk.WavelengthUnit.ANGSTROM,
                            )
                        ),
                        type=sdk.ObservationType.IMAGING,
                    )
                ),
            )

        def test_should_call_across_create_schedule(
            self,
            mock_schedule_api: MagicMock,
        ) -> None:
            """Should create ACROSS schedule"""
            ingest()

            mock_schedule_api.create_schedule.assert_called_once()

        def test_should_call_across_create_schedule_with_schedule_create_instance(
            self, mock_schedule_api: MagicMock
        ) -> None:
            """Should create ACROSS schedule with ScheduleCreate schema"""
            ingest()

            args = mock_schedule_api.create_schedule.call_args[0]

            assert isinstance(args[0], sdk.ScheduleCreate)

        def test_should_call_across_create_schedule_with_observation_create_instance(
            self, mock_schedule_api: MagicMock
        ) -> None:
            """Should create ACROSS schedule with ObservationCreate schemas"""
            ingest()

            args = mock_schedule_api.create_schedule.call_args[0]

            assert isinstance(args[0].observations[0], sdk.ObservationCreate)

        def test_should_not_call_create_schedule_when_observations_are_invalid(
            self,
            mock_read_timeline_file: MagicMock,
            mock_schedule_api: MagicMock,
            fake_invalid_obs_timeline_file_df: pd.DataFrame,
        ) -> None:
            """
            should not call create schedule when observations are invalid
            """
            mock_read_timeline_file.return_value = fake_invalid_obs_timeline_file_df
            ingest()
            mock_schedule_api.create_schedule.assert_not_called()

        def test_should_return_if_cannot_read_timeline_file(
            self,
            mock_read_timeline_file: MagicMock,
            mock_schedule_api: MagicMock,
        ) -> None:
            """Should return if cannot read timeline file"""
            mock_read_timeline_file.return_value = None

            ingest()

            mock_schedule_api.create_schedule.assert_not_called()

    class TestReadPlannedExposureCatalog:
        def test_should_read_planned_exposure_catalog_as_dataframe(self) -> None:
            """Should read the planned exposure catalog file as a DataFrame"""
            exposure_df = read_planned_exposure_catalog()
            assert isinstance(exposure_df, pd.DataFrame)

    class TestGetLatestTimelineFilename:
        def test_should_get_latest_timeline_filename(self) -> None:
            """Should return the timeline filename farthest in the future"""
            mock_latest_filename = get_latest_timeline_file()
            assert mock_latest_filename == "timeline_07_28_25"

        def test_should_get_the_timeline_from_stsci(
            self, mock_httpx_get: MagicMock
        ) -> None:
            """Should get the time timeline from STSCI"""
            get_latest_timeline_file()

            mock_httpx_get.assert_called_once()

        def test_should_instantiate_beautiful_soup(
            self, mock_soup_cls: MagicMock
        ) -> None:
            """Should instantiate beautiful soup"""
            get_latest_timeline_file()

            mock_soup_cls.assert_called_once()

    class TestReadTimelineFile:
        def test_should_read_timeline_file_as_dataframe(
            self,
            fake_timeline_file_df: pd.DataFrame,
        ) -> None:
            """Should read the timeline file as a DataFrame"""
            data = read_timeline_file("timeline_07_28_25")
            pd.testing.assert_frame_equal(data, fake_timeline_file_df)

    class TestExtractObservationPointingCoordinates:
        @pytest.mark.parametrize(
            "test_param",
            [
                ("ra"),
                ("dec"),
            ],
        )
        def test_should_return_position_coord_as_formatted_string(
            self,
            test_param: str,
            fake_planned_exposure_catalog_df: pd.DataFrame,
            fake_timeline_row: dict,
        ) -> None:
            """Should return RA as formatted string"""
            fake_timeline_row["target_name"] = "FSR2007-0584"

            coord = extract_observation_pointing_coordinates(
                fake_planned_exposure_catalog_df,
                task.TimelineRow(**fake_timeline_row),
            )

            assert re.match(r"\d*:\d*:\d*", str(coord and coord.get(test_param)))

        def test_should_return_none_if_target_not_found(
            self,
            fake_planned_exposure_catalog_df: pd.DataFrame,
            fake_timeline_row: dict,
        ) -> None:
            """Should return none if the target is not found in the planned exposure catalog"""
            fake_timeline_row["target_name"] = "mock_fake_target"
            across_observation = extract_observation_pointing_coordinates(
                fake_planned_exposure_catalog_df, task.TimelineRow(**fake_timeline_row)
            )
            assert across_observation is None

    class TestTransformToAcrossObservation:
        def test_should_return_none_when_no_coords(
            self,
            monkeypatch: pytest.MonkeyPatch,
            fake_planned_exposure_catalog_df: pd.DataFrame,
            fake_timeline_row: dict,
        ) -> None:
            """Should return None when coordinates cannot be found for the observation"""
            monkeypatch.setattr(
                task,
                "extract_observation_pointing_coordinates",
                MagicMock(return_value=None),
            )

            across_observation = transform_to_across_observation(
                fake_planned_exposure_catalog_df,
                task.TimelineRow(**fake_timeline_row),
                [],
            )

            assert across_observation is None

        def test_should_return_none_when_cannot_find_instrument_info(
            self,
            monkeypatch: pytest.MonkeyPatch,
            fake_planned_exposure_catalog_df: pd.DataFrame,
            fake_timeline_row: dict,
        ) -> None:
            """Should return none when instrument info cannot be found for observation"""
            monkeypatch.setattr(
                task,
                "extract_observation_pointing_coordinates",
                MagicMock(return_value=Position(ra="1:1:1", dec="2:2:2")),
            )
            monkeypatch.setattr(
                task, "extract_instrument_info", MagicMock(return_value=None)
            )
            across_observation = transform_to_across_observation(
                fake_planned_exposure_catalog_df,
                task.TimelineRow(**fake_timeline_row),
                [],
            )

            assert across_observation is None

    class TestExtractInstrumentInfo:
        def test_should_log_warning_when_no_instrument_short_name_match(
            self,
            fake_instrument: sdk.Instrument,
            mock_logger: MagicMock,
            fake_timeline_row: dict,
        ) -> None:
            """Should log a warning when no match found for the short name"""
            fake_timeline_row["element"] = "fake-element"
            fake_timeline_row["aperture"] = "fake-aperture"
            fake_timeline_row["instrument"] = "FAKE"

            extract_instrument_info(
                task.TimelineRow(**fake_timeline_row), [fake_instrument]
            )

            mock_logger.warning.assert_called_with(
                "Could not match data to ACROSS instrument.",
                instrument=fake_timeline_row["instrument"],
            )

        def test_should_log_warning_when_no_filter_found(
            self,
            fake_instrument: sdk.Instrument,
            mock_logger: MagicMock,
            fake_timeline_row: dict,
        ) -> None:
            """Should log a warning when no filter found from obs parameters"""
            fake_timeline_row["element"] = "fake-element"
            fake_timeline_row["aperture"] = "fake-aperture"
            fake_timeline_row["instrument"] = "ACS"

            fake_instrument.short_name = "HST_ACS"
            fake_instrument.filters = []

            extract_instrument_info(
                task.TimelineRow(**fake_timeline_row), [fake_instrument]
            )

            mock_logger.warning.assert_called_with(
                "Could not find filter for instrument.",
                element=fake_timeline_row["element"],
                aperture=fake_timeline_row["aperture"],
            )

        def test_should_return_none_if_no_filter_found(
            self,
            fake_planned_exposure_catalog_df: pd.DataFrame,
            fake_instrument: sdk.Instrument,
            fake_timeline_row: dict,
        ) -> None:
            """Should return an empty dict if no filter found from obs parameters"""
            fake_timeline_row["target_name"] = (
                fake_planned_exposure_catalog_df["object_name"].values[0],
            )
            fake_timeline_row["element"] = "fake-element"
            fake_timeline_row["aperture"] = "fake-aperture"
            fake_timeline_row["instrument"] = "ACS"

            fake_instrument.filters = []
            fake_instrument.short_name = "HST_ACS"

            obs = extract_instrument_info(
                task.TimelineRow(**fake_timeline_row), [fake_instrument]
            )

            assert obs is None

        @pytest.mark.parametrize(
            "fake_observation_data, fake_instrument_data, obs_type",
            [
                (
                    {
                        "element": "F100W",
                        "aperture": "mock-aperture",
                        "instrument": "ACS",
                    },
                    {
                        "short_name": "HST_ACS",
                        "filters": [{"name": "HST TEST F100W"}],
                    },
                    sdk.ObservationType.IMAGING,
                ),
                (
                    {
                        "element": "G100",
                        "aperture": "mock-aperture",
                        "instrument": "ACS",
                    },
                    {
                        "short_name": "HST_ACS",
                        "filters": [{"name": "HST TEST G100"}],
                    },
                    sdk.ObservationType.SPECTROSCOPY,
                ),
                (
                    {
                        "element": "G100",
                        "aperture": "mock-aperture",
                        "instrument": "ACS",
                    },
                    {
                        "short_name": "HST_ACS",
                        "filters": [{"name": "HST TEST G100"}],
                    },
                    sdk.ObservationType.SPECTROSCOPY,
                ),
                (
                    {
                        "element": "P100",
                        "aperture": "mock-aperture",
                        "instrument": "ACS",
                    },
                    {
                        "short_name": "HST_ACS",
                        "filters": [{"name": "HST TEST P100"}],
                    },
                    sdk.ObservationType.SPECTROSCOPY,
                ),
                (
                    {
                        "element": "FR100",
                        "aperture": "mock-aperture",
                        "instrument": "ACS",
                    },
                    {
                        "short_name": "HST_ACS",
                        "filters": [{"name": "HST TEST FR100"}],
                    },
                    sdk.ObservationType.SPECTROSCOPY,
                ),
                (
                    {
                        "element": "F100W",
                        "aperture": "mock-aperture",
                        "instrument": "COS",
                    },
                    {
                        "short_name": "HST_COS",
                        "filters": [{"name": "HST TEST F100W"}],
                    },
                    sdk.ObservationType.SPECTROSCOPY,
                ),
            ],
        )
        def test_should_pick_correct_obs_type_from_filter_name(
            self,
            fake_observation_data: dict,
            fake_instrument_data: dict,
            fake_timeline_row: dict,
            obs_type: str,
            fake_instrument: sdk.Instrument,
            fake_filters: list[sdk.Filter],
        ) -> None:
            """Should identify correct observation type from obs parameters"""
            # set data on fake schemas
            # use the union operator to update fake_timeline_row
            observation_data = fake_timeline_row | fake_observation_data
            fake_instrument.short_name = fake_instrument_data["short_name"]
            fake_filter_data = fake_instrument_data["filters"][0]
            fake_filters[0].name = fake_filter_data["name"]

            obs = extract_instrument_info(
                task.TimelineRow(**observation_data), [fake_instrument]
            )
            assert obs and obs.type == obs_type

        @pytest.mark.parametrize(
            "fake_observation_data, expected_name",
            [
                (
                    {
                        "instrument": "ACS",
                        "element": "ACS",
                        "aperture": "",
                    },
                    "ACS_Filter",
                ),
                (
                    {
                        "instrument": "COS",
                        "element": "COS",
                        "aperture": "",
                    },
                    "COS_Filter",
                ),
                (
                    {
                        "instrument": "STIS",
                        "element": "STIS",
                        "aperture": "",
                    },
                    "STIS_Filter",
                ),
                (
                    {
                        "instrument": "WFC3/UVIS",
                        "element": "UVIS",
                        "aperture": "",
                    },
                    "UVIS_Filter",
                ),
                (
                    {
                        "instrument": "WFC3/IR",
                        "element": "IR",
                        "aperture": "",
                    },
                    "IR_Filter",
                ),
                (
                    {
                        "instrument": "WFC3/IR",
                        "element": "",
                        "aperture": "APERTURE",
                    },
                    "APERTURE_Filter",
                ),
            ],
        )
        def test_should_return_correct_bandpass_filter_name(
            self,
            fake_observation_data: dict,
            fake_timeline_row: dict,
            fake_instrument: sdk.Instrument,
            expected_name: str,
        ):
            """Should get the correct bandpass filter name given obs parameters"""
            fake_instrument_data = [
                {
                    "short_name": "HST_COS",
                    "filters": [{"name": "COS_Filter"}],
                },
                {
                    "short_name": "HST_ACS",
                    "filters": [{"name": "ACS_Filter"}],
                },
                {
                    "short_name": "HST_STIS",
                    "filters": [{"name": "STIS_Filter"}],
                },
                {
                    "short_name": "HST_WFC3_UVIS",
                    "filters": [{"name": "UVIS_Filter"}],
                },
                {
                    "short_name": "HST_WFC3_IR",
                    "filters": [{"name": "IR_Filter"}, {"name": "APERTURE_Filter"}],
                },
            ]

            def to_new_fake_instrument(data: dict) -> sdk.Instrument:
                copy = fake_instrument.model_copy(deep=True)

                copy.short_name = data["short_name"]
                copy.filters = [
                    (copy.filters or [])[0].model_copy(update=f)
                    for f in data["filters"]
                ]

                return copy

            fake_instruments = list(map(to_new_fake_instrument, fake_instrument_data))

            observation_data = fake_timeline_row | fake_observation_data

            instrument_info = extract_instrument_info(
                task.TimelineRow(**observation_data), fake_instruments
            )
            assert instrument_info and (
                cast(
                    sdk.WavelengthBandpass, instrument_info.bandpass.actual_instance
                ).filter_name
                == expected_name
            )

        def test_should_log_warning_when_multiple_filters_are_matched(
            self,
            fake_instrument: sdk.Instrument,
            fake_timeline_row: dict,
            mock_logger: MagicMock,
        ):
            """Should log a warning when multiple filters are matched"""
            fake_instrument_data: dict = {
                "short_name": "HST_WFC3_IR",
                "filters": [{"name": "IR_Filter"}, {"name": "APERTURE_Filter"}],
            }

            # set both element and aperture fields so that they both produce a match.
            fake_obs_data = fake_timeline_row | {
                "instrument": "WFC3/IR",
                "element": "IR",
                "aperture": "APERTURE",
            }

            copy = fake_instrument.model_copy(deep=True)

            copy.short_name = fake_instrument_data["short_name"]
            copy.filters = [
                (copy.filters or [])[0].model_copy(update=f)
                for f in fake_instrument_data["filters"]
            ]

            extract_instrument_info(task.TimelineRow(**fake_obs_data), [copy])

            mock_logger.warning.assert_called_with(
                "Multiple filters matched for an element/aperture combination. Selecting the first filter...",
                matches=copy.filters,
                element=fake_obs_data["element"],
                aperture=fake_obs_data["aperture"],
            )
