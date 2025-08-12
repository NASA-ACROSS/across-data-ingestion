from collections.abc import Generator
from email.message import Message
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pytest
from astropy.io import fits  # type: ignore[import-untyped]
from astropy.table import Table  # type: ignore[import-untyped]
from httpx import Response

import across_data_ingestion.tasks.schedules.fermi.lat_planned as lat_planned
from across_data_ingestion.util.across_server import sdk

from .conftest import BuildPathProto

MOCK_PRELIM_POINTING_FILE = "FERMI_POINTING_PRELIM_878_2025086_2025093_00.fits"
MOCK_FINAL_POINTING_FILE = "FERMI_POINTING_FINAL_875_2025065_2025072_00.fits"


class TestFermiLATPlannedScheduleIngestionTask:
    def test_should_calculate_date_from_fermi_week_to_YYYYDDD(self):
        """Should calculate start date for a given Fermi week returned in YYYYDDD format"""
        fermi_week_start_date = lat_planned.calculate_date_from_fermi_week(875)
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

    class TestGetPointingFilenames:
        def test_should_return_schedule_files_from_html(
            self, fake_fermi_html: str, fake_fermi_week: int
        ):
            files = lat_planned.get_pointing_filenames(
                fake_fermi_html.splitlines(), fake_fermi_week
            )

            assert len(files) > 0

        def test_should_return_instances_of_schedule_files(
            self, fake_fermi_html: str, fake_fermi_week
        ):
            files = lat_planned.get_pointing_filenames(
                html_lines=fake_fermi_html.splitlines(),
                current_fermi_week=fake_fermi_week,
            )

            assert isinstance(files[0], lat_planned.ScheduleFile)

        def test_should_log_warning_when_no_filenames_match(
            self, fake_fermi_html: str, mock_logger: MagicMock
        ):
            lat_planned.get_pointing_filenames(
                html_lines=fake_fermi_html.splitlines(),
                current_fermi_week=0,
            )

            assert mock_logger.warning.call_count == 3

    class TestGetScheduleFileData:
        @pytest.fixture(autouse=True)
        def patch_base_url(self, mock_base_path):
            with patch(
                "across_data_ingestion.tasks.schedules.fermi.lat_planned.FERMI_LAT_POINTING_FILE_BASE_PATH",
                mock_base_path,
            ):
                yield

        def test_should_return_file_data(
            self,
            fake_schedule_files: list[lat_planned.ScheduleFile],
        ):
            file = lat_planned.get_schedule_file_data(fake_schedule_files)

            assert isinstance(file, lat_planned.FileData)

        def test_should_return_file_data_with_data(
            self,
            fake_schedule_files: list[lat_planned.ScheduleFile],
        ):
            file = lat_planned.get_schedule_file_data(fake_schedule_files)

            assert len(file.table) > 0

        class TestErrors:
            @pytest.fixture
            def fake_http_error(self) -> HTTPError:
                return HTTPError(
                    url="test",
                    code=400,
                    msg="Error",
                    hdrs=Message(),
                    fp=None,
                )

            @pytest.fixture(autouse=True)
            def mock_fits(self, fake_http_error: HTTPError):
                with patch(
                    "across_data_ingestion.tasks.schedules.fermi.lat_planned.fits"
                ) as mock_fits:
                    mock_fits.open.side_effect = fake_http_error
                    yield mock_fits

            def test_should_only_log_warning_when_file_not_found(
                self,
                fake_schedule_files: list[lat_planned.ScheduleFile],
                mock_logger: MagicMock,
                fake_http_error: HTTPError,
            ):
                fake_http_error.code = 404

                lat_planned.get_schedule_file_data(fake_schedule_files)

                # error will raise for each file when trying to open it
                assert mock_logger.warning.call_count == len(fake_schedule_files)

            def test_should_log_error_when_file_cannot_be_opened(
                self,
                fake_schedule_files: list[lat_planned.ScheduleFile],
                mock_logger: MagicMock,
            ):
                lat_planned.get_schedule_file_data(fake_schedule_files)

                # error will raise for each file when trying to open it
                assert mock_logger.error.call_count == len(fake_schedule_files)

            def test_should_log_error_when_generic_exception(
                self,
                fake_schedule_files: list[lat_planned.ScheduleFile],
                mock_fits: MagicMock,
            ):
                mock_fits.open.side_effect = Exception("boom")

                with pytest.raises(Exception) as exc:
                    lat_planned.get_schedule_file_data(fake_schedule_files)

                assert "boom" in str(exc.value)

            def test_should_return_empty_file_data_when_no_data_exists(
                self,
                fake_schedule_files: list[lat_planned.ScheduleFile],
            ):
                file = lat_planned.get_schedule_file_data(fake_schedule_files)

                assert len(file.table) == 0

    class TestGetPlannedScheduleData:
        @pytest.fixture(autouse=True)
        def mock_get_schedule_file_data(self) -> Generator[MagicMock]:
            with (
                patch(
                    "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_pointing_files_html_lines"
                ),
                patch(
                    "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_pointing_filenames"
                ),
                patch(
                    "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_schedule_file_data"
                ) as mock,
            ):
                yield mock

        def test_should_return_file_data(
            self, mock_get_schedule_file_data: MagicMock, fake_fermi_week: int
        ):
            fake_table = Table(
                names=("a", "b", "IN_SAA"),
                rows=[(1, 2, True), (3, 4, False)],
            )
            mock_get_schedule_file_data.return_value = lat_planned.FileData(
                table=fake_table, file=lat_planned.ScheduleFile()
            )

            data = lat_planned.get_planned_schedule_data(fake_fermi_week)

            assert isinstance(data, lat_planned.FileData)

        def test_should_filter_saa_data(
            self, mock_get_schedule_file_data: MagicMock, fake_fermi_week: int
        ):
            fake_table = Table(
                names=("a", "b", "IN_SAA"),
                rows=[(1, 2, True), (3, 4, False)],
            )

            mock_get_schedule_file_data.return_value = lat_planned.FileData(
                table=fake_table, file=lat_planned.ScheduleFile()
            )

            data = lat_planned.get_planned_schedule_data(fake_fermi_week)

            # data.table["IN_SAA"] is array-like
            # True if no row is inside SAA
            assert not data.table["IN_SAA"].any()

    class TestIngest:
        @pytest.fixture(autouse=True)
        def mock_get_schedule_file_data(
            self, build_path: BuildPathProto
        ) -> Generator[MagicMock]:
            with (
                patch(
                    "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_pointing_files_html_lines"
                ),
                patch(
                    "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_pointing_filenames"
                ),
                patch(
                    "across_data_ingestion.tasks.schedules.fermi.lat_planned.get_schedule_file_data"
                ) as mock_get_data,
            ):
                hdu = fits.open(build_path(MOCK_PRELIM_POINTING_FILE))
                data = Table(hdu[1].data)

                mock_get_data.return_value = lat_planned.FileData(table=data)
                yield mock_get_data

        def test_should_log_warning_when_no_schedule_data(
            self,
            mock_logger: MagicMock,
            mock_get_schedule_file_data: MagicMock,
        ):
            mock_get_schedule_file_data.return_value = lat_planned.FileData()

            lat_planned.ingest()

            mock_logger.warning.assert_called_once_with(
                "No schedule data to transform."
            )

        def test_should_return_not_call_across_when_no_schedule_data(
            self,
            mock_get_schedule_file_data: MagicMock,
            mock_telescope_api_cls: MagicMock,
        ):
            mock_get_schedule_file_data.return_value = lat_planned.FileData()

            lat_planned.ingest()

            mock_telescope_api_cls.assert_not_called()

        def test_should_get_telescope_info_from_across(
            self,
            mock_telescope_api: MagicMock,
        ):
            lat_planned.ingest()

            mock_telescope_api.get_telescopes.assert_called_once()

        def test_should_create_schedule_in_across(
            self,
            mock_schedule_api: MagicMock,
        ):
            lat_planned.ingest()

            mock_schedule_api.create_schedule.assert_called_once()

        def test_should_log_info_when_across_returns_409_conflict(
            self,
            mock_logger: MagicMock,
            mock_schedule_api: MagicMock,
        ):
            mock_schedule_api.create_schedule.side_effect = sdk.ApiException(
                status=409, reason="Conflict"
            )

            lat_planned.ingest()

            mock_logger.info.assert_called_once()

        def test_should_raise_exc_when_any_other_exception(
            self,
            mock_telescope_api: MagicMock,
        ):
            mock_telescope_api.get_telescopes.side_effect = Exception("oh no")

            with pytest.raises(Exception) as exc:
                lat_planned.ingest()

            assert "oh no" in str(exc.value)
