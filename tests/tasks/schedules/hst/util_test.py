import pytest

from across_data_ingestion.tasks.schedules.hst.util import (
    get_instrument_short_name_from_observation,
    get_obs_type,
)
from across_data_ingestion.util.across_server import sdk


class TestGetObsType:
    @pytest.mark.parametrize(
        "fake_instrument_data, obs_type",
        [
            (
                {
                    "short_name": "HST_ACS",
                    "filters": [{"name": "HST TEST F100W"}],
                },
                sdk.ObservationType.IMAGING,
            ),
            (
                {
                    "short_name": "HST_ACS",
                    "filters": [{"name": "HST TEST G100"}],
                },
                sdk.ObservationType.SPECTROSCOPY,
            ),
            (
                {
                    "short_name": "HST_ACS",
                    "filters": [{"name": "HST TEST G100"}],
                },
                sdk.ObservationType.SPECTROSCOPY,
            ),
            (
                {
                    "short_name": "HST_ACS",
                    "filters": [{"name": "HST TEST P100"}],
                },
                sdk.ObservationType.SPECTROSCOPY,
            ),
            (
                {
                    "short_name": "HST_ACS",
                    "filters": [{"name": "HST TEST FR100"}],
                },
                sdk.ObservationType.SPECTROSCOPY,
            ),
            (
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
        fake_instrument_data: dict,
        obs_type: str,
        fake_instrument: sdk.Instrument,
        fake_filters: list[sdk.Filter],
    ) -> None:
        """Should identify correct observation type from obs parameters"""
        # set data on fake schemas
        fake_instrument.short_name = fake_instrument_data["short_name"]
        fake_filter_data = fake_instrument_data["filters"][0]
        fake_filters[0].name = fake_filter_data["name"]
        fake_filter = fake_filters[0]

        obs = get_obs_type(fake_filter, fake_instrument)
        assert obs and obs.value == obs_type

    @pytest.mark.parametrize(
        "fake_observation_instrument_name, expected_short_name",
        [
            (
                "HST ACS",
                "HST_ACS",
            ),
            (
                "HST COS",
                "HST_COS",
            ),
            (
                "HST STIS MAMA",
                "HST_STIS",
            ),
            (
                "HST WFC3/UVIS",
                "HST_WFC3_UVIS",
            ),
            (
                "HST WFC3/IR",
                "HST_WFC3_IR",
            ),
            (
                "HST BAD NAME",
                None,
            ),
        ],
    )
    def test_should_pick_correct_instrument_short_name(
        self,
        fake_observation_instrument_name: str,
        expected_short_name: str,
    ) -> None:
        """Should identify correct instrument short name from observation instrument name"""
        # set data on fake schemas
        instrument_short_name = get_instrument_short_name_from_observation(
            fake_observation_instrument_name
        )
        assert instrument_short_name == expected_short_name
