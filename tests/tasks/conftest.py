from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def mock_pandas(
    monkeypatch: pytest.MonkeyPatch,
) -> MagicMock:
    mock = MagicMock()

    mock.read_csv = MagicMock(side_effect=pd.read_csv)
    mock.read_fwf = MagicMock(side_effect=pd.read_fwf)

    monkeypatch.setattr(pd, "read_csv", mock.read_csv)
    monkeypatch.setattr(pd, "read_fwf", mock.read_fwf)

    return mock
