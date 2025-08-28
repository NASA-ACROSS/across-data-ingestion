from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pydantic
import pytest
from swifttools import swift_too  # type:ignore

import across_data_ingestion.tasks.schedules.swift.low_fidelity_planned as task

from .mocks import fake_swift_plan


class FakeUVOTModeEntry(pydantic.BaseModel):
    """Attempt to match the UVOT entry object when iterating over uvot modes from Swift"""

    filter_name: str
    weight: int


class FakeUVOTMode(pydantic.BaseModel):
    entries: list[FakeUVOTModeEntry]


class FakePPSTEntry(pydantic.BaseModel):
    """Attempt to match the ppst entry object when iterating over SwiftPPST"""

    obsid: str
    targname: str
    ra: float
    dec: float
    begin: datetime
    end: datetime
    exposure: timedelta
    roll: float
    uvot: str
    bat: str
    xrt: str
    fom: float
    segment: int
    target_id: int


@pytest.fixture
def fake_uvot_mode_entries():
    return {
        "0x30ed": [
            {"filter_name": "u", "weight": 750},
            {"filter_name": "b", "weight": 750},
            {"filter_name": "v", "weight": 750},
            {"filter_name": "uvw1", "weight": 1500},
            {"filter_name": "uvw2", "weight": 3000},
            {"filter_name": "uvm2", "weight": 2250},
        ],
        "0x223f": [
            {"filter_name": "u", "weight": 200},
            {"filter_name": "b", "weight": 200},
            {"filter_name": "v", "weight": 200},
            {"filter_name": "uvw1", "weight": 600},
            {"filter_name": "uvw2", "weight": 1000},
            {"filter_name": "uvm2", "weight": 1600},
        ],
        "0x015a": [{"filter_name": "uvm2", "weight": 3000}],
        "0x011e": [{"filter_name": "uvw2", "weight": 3000}],
        "0x308f": [
            {"filter_name": "uvw1", "weight": 3000},
            {"filter_name": "uvw2", "weight": 3000},
            {"filter_name": "uvm2", "weight": 3000},
        ],
    }


@pytest.fixture
def fake_swift_ppst_entries() -> list[FakePPSTEntry]:
    """
    Gets the mock swift plan
    """
    return [FakePPSTEntry(**entry) for entry in fake_swift_plan.entries]


@pytest.fixture
def fake_swift_obs_entries(
    fake_swift_ppst_entries: list[FakePPSTEntry],
) -> list[task.CustomSwiftObsEntry]:
    return [
        task.CustomSwiftObsEntry.from_entry(entry) for entry in fake_swift_ppst_entries
    ]


@pytest.fixture
def mock_uvot_mode_cls(fake_uvot_mode_entries: dict) -> MagicMock:
    mock_instance = MagicMock()
    mock_instance.entries = []

    def mock_init(mode: str) -> MagicMock:
        # set the entries and return the instance, when
        # there is a mode that dne return an empty list.
        try:
            entries = fake_uvot_mode_entries[mode]
        except KeyError:
            entries = []

        mock_instance.entries = [
            FakeUVOTModeEntry.model_validate(entry) for entry in entries
        ]
        return mock_instance

    return MagicMock(spec=FakeUVOTMode, side_effect=mock_init)


@pytest.fixture(autouse=True)
def mock_swift_too(
    monkeypatch: pytest.MonkeyPatch,
    fake_swift_ppst_entries: list[FakePPSTEntry],
    mock_uvot_mode_cls: MagicMock,
):
    mock_too = MagicMock()
    mock_too.PlanQuery = MagicMock(return_value=fake_swift_ppst_entries)
    mock_too.UVOTMode = mock_uvot_mode_cls

    monkeypatch.setattr(swift_too, "PlanQuery", mock_too.PlanQuery)
    monkeypatch.setattr(swift_too, "UVOTMode", mock_too.UVOTMode)

    return mock_too
