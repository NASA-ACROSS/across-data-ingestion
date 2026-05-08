from unittest.mock import MagicMock

import pytest
from across.sdk.v1 import ObservationFootprintCreate
from across.tools import Coordinate
from across.tools.footprint import Footprint

from across_data_ingestion.util.footprint_util import (
    _convert,
    project_footprint,
)


@pytest.fixture()
def mock_converted_footprint(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock_fp = MagicMock()
    mock_fp.project.return_value.detectors = []

    monkeypatch.setattr(
        "across_data_ingestion.util.footprint_util._convert",
        lambda _: mock_fp,
    )

    return mock_fp


class TestSdkFootprintsToTools:
    def test_returns_footprint_instance(self):
        result = _convert([])
        assert isinstance(result, Footprint)

    def test_empty_input_returns_no_detectors(self):
        result = _convert([])
        assert result.detectors == []


class TestGenerateObservationFootprint:
    def test_returns_list(self, mock_converted_footprint):
        result = project_footprint([], 0, 0, 0)
        assert isinstance(result, list)

    def test_calls_project(self, mock_converted_footprint):
        project_footprint([], 1, 2, 3)
        assert mock_converted_footprint.project.called

    def test_project_called_with_correct_ra(self, mock_converted_footprint):
        project_footprint([], 10, 20, 30)
        assert mock_converted_footprint.project.call_args.kwargs["coordinate"].ra == 10

    def test_project_called_with_correct_dec(self, mock_converted_footprint):
        project_footprint([], 10, 20, 30)
        assert mock_converted_footprint.project.call_args.kwargs["coordinate"].dec == 20

    def test_project_called_with_correct_roll(self, mock_converted_footprint):
        project_footprint([], 10, 20, 30)
        assert mock_converted_footprint.project.call_args.kwargs["roll_angle"] == 30

    def test_returns_empty_when_no_detectors(self, mock_converted_footprint):
        result = project_footprint([], 0, 0, 0)
        assert result == []

    def test_output_length_matches_detectors(self, mock_converted_footprint):
        mock_detector = MagicMock()
        mock_detector.coordinates = [Coordinate(ra=1, dec=2)]

        mock_converted_footprint.project.return_value.detectors = [
            mock_detector,
            mock_detector,
        ]

        result = project_footprint([], 0, 0, 0)
        assert len(result) == 2

    def test_output_type_is_observation_footprint_create(
        self, mock_converted_footprint
    ):
        mock_detector = MagicMock()
        mock_detector.coordinates = [Coordinate(ra=1, dec=2)]

        mock_converted_footprint.project.return_value.detectors = [mock_detector]

        result = project_footprint([], 0, 0, 0)
        assert isinstance(result[0], ObservationFootprintCreate)

    def test_polygon_point_ra_mapping(self, mock_converted_footprint):
        mock_detector = MagicMock()
        mock_detector.coordinates = [Coordinate(ra=5, dec=6)]

        mock_converted_footprint.project.return_value.detectors = [mock_detector]

        result = project_footprint([], 0, 0, 0)
        assert result[0].polygon[0].x == 5

    def test_polygon_point_dec_mapping(self, mock_converted_footprint):
        mock_detector = MagicMock()
        mock_detector.coordinates = [Coordinate(ra=5, dec=6)]

        mock_converted_footprint.project.return_value.detectors = [mock_detector]

        result = project_footprint([], 0, 0, 0)
        assert result[0].polygon[0].y == 6
