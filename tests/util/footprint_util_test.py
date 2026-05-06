from unittest.mock import MagicMock

from across.sdk.v1 import ObservationFootprintCreate
from across.tools import Coordinate
from across.tools.footprint import Footprint

from across_data_ingestion.util.footprint_util import (
    _convert,
    project_footprint,
)


class TestSdkFootprintsToTools:
    def test_returns_footprint_instance(self):
        result = _convert([])
        assert isinstance(result, Footprint)

    def test_empty_input_returns_no_detectors(self):
        result = _convert([])
        assert result.detectors == []


class TestGenerateObservationFootprint:
    def test_returns_list(self, monkeypatch):
        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = []

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        result = project_footprint([], 0, 0, 0)
        assert isinstance(result, list)

    def test_calls_project(self, monkeypatch):
        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = []

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        project_footprint([], 1, 2, 3)
        assert mock_fp.project.called

    def test_project_called_with_correct_ra(self, monkeypatch):
        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = []

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        project_footprint([], 10, 20, 30)
        assert mock_fp.project.call_args.kwargs["coordinate"].ra == 10

    def test_project_called_with_correct_dec(self, monkeypatch):
        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = []

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        project_footprint([], 10, 20, 30)
        assert mock_fp.project.call_args.kwargs["coordinate"].dec == 20

    def test_project_called_with_correct_roll(self, monkeypatch):
        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = []

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        project_footprint([], 10, 20, 30)
        assert mock_fp.project.call_args.kwargs["roll_angle"] == 30

    def test_returns_empty_when_no_detectors(self, monkeypatch):
        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = []

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        result = project_footprint([], 0, 0, 0)
        assert result == []

    def test_output_length_matches_detectors(self, monkeypatch):
        mock_detector = MagicMock()
        mock_detector.coordinates = [Coordinate(ra=1, dec=2)]

        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = [mock_detector, mock_detector]

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        result = project_footprint([], 0, 0, 0)
        assert len(result) == 2

    def test_output_type_is_observation_footprint_create(self, monkeypatch):
        mock_detector = MagicMock()
        mock_detector.coordinates = [Coordinate(ra=1, dec=2)]

        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = [mock_detector]

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        result = project_footprint([], 0, 0, 0)
        assert isinstance(result[0], ObservationFootprintCreate)

    def test_polygon_point_ra_mapping(self, monkeypatch):
        mock_detector = MagicMock()
        mock_detector.coordinates = [Coordinate(ra=5, dec=6)]

        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = [mock_detector]

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        result = project_footprint([], 0, 0, 0)
        assert result[0].polygon[0].x == 5

    def test_polygon_point_dec_mapping(self, monkeypatch):
        mock_detector = MagicMock()
        mock_detector.coordinates = [Coordinate(ra=5, dec=6)]

        mock_fp = MagicMock()
        mock_fp.project.return_value.detectors = [mock_detector]

        monkeypatch.setattr(
            "across_data_ingestion.util.footprint_util._convert",
            lambda _: mock_fp,
        )

        result = project_footprint([], 0, 0, 0)
        assert result[0].polygon[0].y == 6
