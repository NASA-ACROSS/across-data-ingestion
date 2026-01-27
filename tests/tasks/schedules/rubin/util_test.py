import pandas as pd
import pytest

from across_data_ingestion.tasks.schedules.rubin.util import (
    designate_filter_name_key,
)


class TestUtil:
    @pytest.mark.parametrize(
        "em_min, em_max, expected_filter",
        [
            # rubin_u filter (midpoint between 3492.5 and 3955.5 angstroms)
            (3492.5e-10, 3955.5e-10, "rubin_u"),
            (3500e-10, 3900e-10, "rubin_u"),
            # rubin_g filter (midpoint between 4064.5 and 5549.5 angstroms)
            (4064.5e-10, 5549.5e-10, "rubin_g"),
            (4500e-10, 5000e-10, "rubin_g"),
            # rubin_r filter (midpoint between 5521.5 and 6920.5 angstroms)
            (5521.5e-10, 6920.5e-10, "rubin_r"),
            (6000e-10, 6500e-10, "rubin_r"),
            # rubin_i filter (midpoint between 6916 and 8202 angstroms)
            (6916e-10, 8202e-10, "rubin_i"),
            (7000e-10, 8000e-10, "rubin_i"),
            # rubin_z filter (midpoint between 8160 and 9200 angstroms)
            (8160e-10, 9200e-10, "rubin_z"),
            (8500e-10, 9000e-10, "rubin_z"),
            # rubin_y filter (midpoint between 9322 and 10184 angstroms)
            (9322e-10, 10184e-10, "rubin_y"),
            (9500e-10, 10000e-10, "rubin_y"),
            # unknown filter (outside all ranges)
            (3.0e-10, 3.4e-10, "unknown"),
        ],
    )
    def test_designate_filter_name_key(
        self, em_min: float, em_max: float, expected_filter: str
    ) -> None:
        """Should correctly designate filter name from em_min and em_max values"""

        row = pd.Series({"em_min": em_min, "em_max": em_max})
        result = designate_filter_name_key(row)

        assert result == expected_filter
