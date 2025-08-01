from unittest.mock import patch

import pytest

from across_data_ingestion.core.exceptions import AcrossHTTPException
from across_data_ingestion.util.across_api import observatory, schedule, telescope, tle


class mock_response:
    """
    Dummy class that mocks the used functionality of the requests library.
        All that is used (so far) in api repository are:

        Fields:
            status_code: int
            text: str
        Methods:
            json
    """

    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

    def json(self):
        return [{}]


class TestScheduleApi:
    """Class that is used for our implementation of the Across-Server API"""

    class TestPost:
        def test_should_create_201(self):
            """Should log a successful posted schedule"""
            response = mock_response(status_code=201, text="test_id")

            with patch(
                "across_data_ingestion.util.across_api.schedule.api.logger"
            ) as log_mock, patch("httpx.request", return_value=response):
                schedule.post(data={})
                assert (
                    f"Schedule Created with id: {response.text}"
                    in log_mock.info.call_args.args[0]
                )

        def test_should_be_duplicate_409(self):
            """Should log a duplicate schedule POST"""
            response = mock_response(
                status_code=409,
                text="Duplicate Schedule detected with id test_id already exists.",
            )

            with patch(
                "across_data_ingestion.util.across_api.schedule.api.logger"
            ) as log_mock, patch("httpx.request", return_value=response):
                schedule.post(data={})
                assert response.text in log_mock.info.call_args.args[0]

        def test_should_raise_exception(self):
            """Should raise an exception in POST"""
            response = mock_response(
                status_code=400,
                text="Duplicate Schedule detected with id test_id already exists.",
            )

            with patch("httpx.request", return_value=response), patch(
                "logging.error", return_value=None
            ):
                with pytest.raises(AcrossHTTPException):
                    schedule.post(data={})


class TestTelescopeApi:
    class TestGet:
        def test_should_be_succesful_on_200(self):
            """Should return a successful GET"""
            response = mock_response(status_code=200, text="response_text")

            with patch("httpx.request", return_value=response):
                value = telescope.get(params={})
                assert isinstance(value, list)

        def test_should_raise_exception(self):
            """Should raise an exception on telescope get with non-200 status code"""
            response = mock_response(status_code=400, text="response_text")

            with patch("httpx.request", return_value=response), patch(
                "logging.error", return_value=None
            ):
                with pytest.raises(AcrossHTTPException):
                    telescope.get(params={})


class TestObservatoryApi:
    class TestGet:
        def test_should_be_successful_on_200(self):
            """Should return a successful GET"""
            response = mock_response(status_code=200, text="response text")

            with patch("httpx.request", return_value=response):
                value = observatory.get(params={})
                assert isinstance(value, list)

        def test_should_raise_exception(self):
            """Should raise an exception on observatory get with non-200 status code"""
            response = mock_response(status_code=400, text="response_text")

            with patch("httpx.request", return_value=response), patch(
                "logging.error", return_value=None
            ):
                with pytest.raises(AcrossHTTPException):
                    observatory.get(params={})


class TestTLEApi:
    class TestPost:
        def test_should_create_201(self):
            """Should return on a 201"""
            response = mock_response(status_code=201, text="test_tle_params")

            with patch("httpx.request", return_value=response):
                tle_post_return = tle.post(data={})
                assert tle_post_return is None

        def test_should_be_duplicate_409(self):
            """Should log a duplicate TLE POST"""
            response = mock_response(
                status_code=409,
                text="TLE with norad_id and epoch already exists",
            )

            with patch(
                "across_data_ingestion.util.across_api.tle.api.logger"
            ) as log_mock, patch("httpx.request", return_value=response):
                tle.post(data={})
                assert response.text in log_mock.warn.call_args.args[0]

        def test_should_raise_exception(self):
            """Should raise an exception in POST with non-201 or 409 status code"""
            response = mock_response(
                status_code=400,
                text="TLE with norad_id and epoch already exists",
            )

            with patch("httpx.request", return_value=response), patch(
                "logging.error", return_value=None
            ):
                with pytest.raises(AcrossHTTPException):
                    tle.post(data={})
