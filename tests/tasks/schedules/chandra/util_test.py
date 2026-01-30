import pytest

from across_data_ingestion.tasks.schedules.chandra.util import (
    match_instrument_from_tap_observation,
)
from across_data_ingestion.util.across_server import sdk


class TestUtil:
    @pytest.mark.parametrize(
        "mock_tap_row, expected_instrument_short_name",
        [
            (
                {"instrument": "ACIS", "grating": "NONE", "exposure_mode": "NONE"},
                "ACIS",
            ),
            (
                {"instrument": "ACIS", "grating": "HETG", "exposure_mode": "NONE"},
                "ACIS-HETG",
            ),
            (
                {"instrument": "ACIS", "grating": "LETG", "exposure_mode": "NONE"},
                "ACIS-LETG",
            ),
            (
                {"instrument": "ACIS", "grating": "NONE", "exposure_mode": "CC"},
                "ACIS-CC",
            ),
            (
                {"instrument": "HRC", "grating": "NONE", "exposure_mode": ""},
                "HRC",
            ),
            (
                {"instrument": "HRC", "grating": "HETG", "exposure_mode": ""},
                "HRC-HETG",
            ),
            (
                {"instrument": "HRC", "grating": "LETG", "exposure_mode": ""},
                "HRC-LETG",
            ),
            (
                {"instrument": "HRC", "grating": "NONE", "exposure_mode": "TIMING"},
                "HRC-Timing",
            ),
            (
                {"instrument": "ACIS", "grating": "BAD_GRATING", "exposure_mode": ""},
                "",
            ),
            (
                {"instrument": "HRC", "grating": "BAD_GRATING", "exposure_mode": ""},
                "",
            ),
            (
                {"instrument": "BAD_INSTRUMENT", "grating": "", "exposure_mode": ""},
                "",
            ),
        ],
    )
    def test_should_match_instrument_from_tap_observation(
        self,
        mock_tap_row: dict,
        expected_instrument_short_name: str,
        fake_instruments_by_short_name: dict[str, sdk.TelescopeInstrument],
    ) -> None:
        """Should return correct instrument name and id from observation row"""
        instrument = match_instrument_from_tap_observation(
            fake_instruments_by_short_name, mock_tap_row
        )

        assert instrument.short_name == expected_instrument_short_name
