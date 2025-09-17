from collections.abc import Generator
from datetime import datetime
from email.message import Message
from unittest.mock import MagicMock
from urllib.error import HTTPError

import pandas as pd
import pytest
from httpx import Response

import across_data_ingestion.tasks.schedules.fermi.lat_planned as lat_planned
from across_data_ingestion.util.across_server import sdk


@pytest.fixture(autouse=True)
def patch_current_time(monkeypatch: pytest.MonkeyPatch) -> Generator[MagicMock]:
    mock = MagicMock(return_value=datetime.fromisoformat("2025-03-07T00:00:00Z"))
    monkeypatch.setattr(lat_planned, "get_current_time", mock)
    return mock


@pytest.fixture
def mock_download_pointings_data(monkeypatch: pytest.MonkeyPatch):
    mock = MagicMock()
    monkeypatch.setattr(lat_planned, "download_pointings_data", mock)
    return mock


class TestCalculateDateFromFermiWeek:
    def test_should_calculate_date_from_fermi_week_to_YYYYDDD(
        self, fake_fermi_week: int
    ):
        """Should calculate start date for a given Fermi week returned in YYYYDDD format"""
        fermi_week_start_date = lat_planned.calculate_date_from_fermi_week(
            fake_fermi_week
        )
        assert fermi_week_start_date == "2025065"


class TestGetPointingFilesHtmlLines:
    def test_should_get_html_of_file_list(self, mock_httpx: MagicMock):
        """Should make an HTTP get to pull the HTML page to scrape filenames"""
        lat_planned.get_pointing_files_html_lines()

        mock_httpx.assert_called_once()

    def test_should_return_html_lines_from_fermi_web_page(self):
        """Should return a list of html lines from the Fermi web page."""
        data = lat_planned.get_pointing_files_html_lines()
        assert len(data) > 0

    def test_should_log_an_error_when_status_code_is_gte_300(
        self, mock_httpx: MagicMock, mock_logger: MagicMock
    ):
        """Should log an error if GET request for LAT pointing files returns >= 300"""
        mock_httpx.return_value = Response(
            status_code=404,
            text=" ",
        )
        lat_planned.get_pointing_files_html_lines()

        assert "Failed to GET" in mock_logger.error.call_args.args[0]

    def test_should_return_an_empty_array_when_status_code_is_gte_300(
        self, mock_httpx: MagicMock, mock_logger: MagicMock
    ):
        """Should return an empty array when GET request for LAT pointing files returns >= 300"""
        mock_httpx.return_value = Response(
            status_code=404,
            text=" ",
        )
        lines = lat_planned.get_pointing_files_html_lines()

        assert len(lines) == 0


class TestParsePointingFiles:
    def test_should_return_schedule_files_from_html(self, fake_fermi_html: str):
        files = lat_planned.parse_pointing_files(fake_fermi_html.splitlines())

        assert len(files) > 0

    @pytest.mark.parametrize("field", lat_planned.PointingFile.model_fields)
    def test_should_return_expected_parsed_pointing_file(
        self,
        field: str,
        fake_fermi_html: str,
        fake_pointing_files: list[lat_planned.PointingFile],
    ):
        files = lat_planned.parse_pointing_files(fake_fermi_html.splitlines())

        assert getattr(files[0], field) == getattr(fake_pointing_files[0], field)

    def test_should_return_instances_of_schedule_files(self, fake_fermi_html: str):
        files = lat_planned.parse_pointing_files(fake_fermi_html.splitlines())

        assert isinstance(files[0], lat_planned.PointingFile)


class TestGetPointingsData:
    @pytest.fixture(autouse=True)
    def patch_base_url(self, monkeypatch: pytest.MonkeyPatch, mock_base_path: str):
        monkeypatch.setattr(
            lat_planned, "FERMI_LAT_POINTING_FILE_BASE_PATH", mock_base_path
        )

    @pytest.fixture()
    def fake_file_groups(
        self, fake_pointing_files: list[lat_planned.PointingFile]
    ) -> list[list[lat_planned.PointingFile]]:
        groups: dict[int, list[lat_planned.PointingFile]] = {}

        for file in fake_pointing_files:
            week = groups.get(file.week)

            if week is None:
                groups[file.week] = [file]
            else:
                week.append(file)

        return list(groups.values())

    def test_should_return_pointing_data(
        self,
        fake_file_groups: list[list[lat_planned.PointingFile]],
    ):
        pointings = lat_planned.download_pointings_data(fake_file_groups)

        assert isinstance(pointings[0], lat_planned.PointingData)

    def test_should_return_pointings_with_data(
        self,
        fake_file_groups: list[list[lat_planned.PointingFile]],
    ):
        pointings = lat_planned.download_pointings_data(fake_file_groups)

        assert len(pointings[0].df) > 0

    def test_should_return_pointings_for_each_week(
        self,
        fake_file_groups: list[list[lat_planned.PointingFile]],
    ):
        pointings = lat_planned.download_pointings_data(fake_file_groups)

        number_of_checked_weeks = 3

        assert len(pointings) == number_of_checked_weeks

    def test_should_return_files_when_some_fail_from_http_error(
        self,
        fake_pointing_files: list[lat_planned.PointingFile],
        mock_fits,
        fake_hdu,
    ):
        # use one for each group, first will fail, other will return data
        groups = [[fake_pointing_files[0]], [fake_pointing_files[1]]]

        mock_fits.open.side_effect = [
            HTTPError("file-url", 404, "", Message(), None),
            [None, fake_hdu],
        ]

        pointings = lat_planned.download_pointings_data(groups)

        assert len(pointings) == 1

    def test_should_filter_in_saa_pointings(
        self,
        fake_pointing_row: dict,
        fake_file_groups: list[list[lat_planned.PointingFile]],
        # not auto used, since not all tests need it so it has to be added but not accessed
        mock_fits: MagicMock,
    ):
        # set row to in SAA
        fake_pointing_row["IN_SAA"] = True

        # assume only one week and one file for it will be found
        fake_file_groups = fake_file_groups[0:1]
        fake_file_groups[0] = fake_file_groups[0][0:1]

        pointings = lat_planned.download_pointings_data(fake_file_groups)

        assert len(pointings[0].df) == 0

    def test_should_log_warning_when_404_file_not_found(
        self,
        fake_file_groups: list[list[lat_planned.PointingFile]],
        fake_hdu: MagicMock,
        mock_fits: MagicMock,
        mock_logger: MagicMock,
    ):
        # last group has 2 files, first should fail, second should return data
        fake_file_groups = [fake_file_groups[-1]]
        mock_fits.open.side_effect = [
            HTTPError("file-url", 404, "", Message(), None),
            [None, fake_hdu],
        ]

        lat_planned.download_pointings_data(fake_file_groups)

        mock_logger.warning.assert_called_once_with(
            "File not found, skipping.", url="file-url"
        )

    def test_should_log_exception_when_any_http_error(
        self,
        fake_file_groups: list[list[lat_planned.PointingFile]],
        fake_hdu: MagicMock,
        mock_fits: MagicMock,
        mock_logger: MagicMock,
    ):
        # last group has 2 files, first should fail, second should return data
        fake_file_groups = [fake_file_groups[-1]]
        mock_fits.open.side_effect = [
            HTTPError("file-url", 400, "", Message(), None),
            [None, fake_hdu],
        ]

        lat_planned.download_pointings_data(fake_file_groups)

        mock_logger.exception.assert_called_once_with(
            "Failed to read the file due to an HTTP error.", url="file-url"
        )

    def test_should_log_warning_when_no_files_for_a_week_found(
        self,
        fake_file_groups: list[list[lat_planned.PointingFile]],
        mock_fits: MagicMock,
        mock_logger: MagicMock,
    ):
        # check one week with one file
        file = fake_file_groups[0][0]
        groups = [[file]]
        mock_fits.open.side_effect = [
            HTTPError("file-url", 404, "", Message(), None),
        ]

        lat_planned.download_pointings_data(groups)

        mock_logger.warning.assert_called_with(
            "No pointing data found for a given week.",
            week=file.week,
            fidelity=file.fidelity,
        )

    def test_should_raise_error_when_generic_exception_thrown(
        self,
        fake_file_groups: list[list[lat_planned.PointingFile]],
        mock_fits: MagicMock,
    ):
        mock_fits.open.side_effect = Exception("oh no")

        with pytest.raises(Exception):
            lat_planned.download_pointings_data(fake_file_groups)


class TestFindFilesForWeeksAhead:
    def test_should_return_list_of_files_per_week(
        self,
        fake_pointing_files: list[lat_planned.PointingFile],
        fake_fermi_week: int,
    ):
        groups = lat_planned.find_files_for_weeks_ahead(
            fake_pointing_files, fake_fermi_week
        )

        assert len(groups[0]) > 0

    def test_should_return_groups_of_only_the_weeks_ahead(
        self,
        fake_pointing_files: list[lat_planned.PointingFile],
        fake_fermi_week: int,
    ):
        extra_files = [
            lat_planned.PointingFile(week=fake_fermi_week - 1),
            lat_planned.PointingFile(week=fake_fermi_week - 2),
            lat_planned.PointingFile(week=fake_fermi_week - 3),
        ]

        groups = lat_planned.find_files_for_weeks_ahead(
            [*fake_pointing_files, *extra_files], fake_fermi_week
        )

        # Flatten all files in all groups
        all_files = [f for group in groups for f in group]

        assert all(
            (f.week - fake_fermi_week) in lat_planned.FIDELITY_BY_WEEKS_AHEAD
            for f in all_files
        )


class TestTransformObservations:
    def test_should_transform_data_to_observations(
        self, fake_pointing_df: pd.DataFrame
    ):
        observations = lat_planned.transform_to_observations("id", 1, fake_pointing_df)

        assert isinstance(observations[0], sdk.ObservationCreate)

    @pytest.mark.parametrize(
        "field, extract_obs, extract_df",
        [
            (
                "ra",
                lambda obs: [o.pointing_position.ra for o in obs],
                lambda df: list(df["RA_SCZ"]),
            ),
            (
                "dec",
                lambda obs: [o.pointing_position.dec for o in obs],
                lambda df: list(df["DEC_SCZ"]),
            ),
            (
                "begin",
                lambda obs: [o.date_range.begin for o in obs],
                lambda df: (
                    lat_planned.FERMI_TIME_START_EPOCH
                    + df["START"].to_numpy() * lat_planned.u.second
                )
                .to_datetime()
                .tolist(),
            ),
            (
                "stop",
                lambda obs: [o.date_range.end for o in obs],
                lambda df: (
                    lat_planned.FERMI_TIME_START_EPOCH
                    + df["STOP"].to_numpy() * lat_planned.u.second
                )
                .to_datetime()
                .tolist(),
            ),
            (
                "exposure",
                lambda obs: [o.exposure_time for o in obs],
                lambda df: (df["STOP"] - df["START"]).astype(float).tolist(),
            ),
        ],
    )
    def test_should_map_over_array_data_without_changing_order(
        self, fake_pointing_df: pd.DataFrame, field, extract_obs, extract_df
    ):
        observations = lat_planned.transform_to_observations("id", 1, fake_pointing_df)

        actual = extract_obs(observations)
        expected_vals = extract_df(fake_pointing_df)

        assert actual == expected_vals


class TestIngest:
    def test_should_call_create_many_schedules(self, mock_schedule_api: MagicMock):
        lat_planned.ingest()

        mock_schedule_api.create_many_schedules.assert_called_once()

    def test_should_log_warning_when_no_pointings(
        self,
        mock_download_pointings_data: MagicMock,
        mock_logger: MagicMock,
        fake_fermi_week: int,
    ):
        mock_download_pointings_data.return_value = []

        lat_planned.ingest()

        mock_logger.warning.assert_called_once_with(
            "No pointing data to transform.", current_fermi_week=fake_fermi_week
        )

    def test_should_not_call_across_when_no_schedule_data(
        self,
        mock_download_pointings_data: MagicMock,
        mock_telescope_api_cls: MagicMock,
    ):
        mock_download_pointings_data.return_value = []

        lat_planned.ingest()

        mock_telescope_api_cls.assert_not_called()

    def test_should_get_telescope_info_from_across(
        self,
        mock_telescope_api: MagicMock,
    ):
        lat_planned.ingest()

        mock_telescope_api.get_telescopes.assert_called_once()

    def test_should_raise_exc_when_any_other_exception(
        self,
        mock_telescope_api: MagicMock,
    ):
        mock_telescope_api.get_telescopes.side_effect = Exception("oh no")

        with pytest.raises(Exception) as exc:
            lat_planned.ingest()

        assert "oh no" in str(exc.value)
