from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

import across_data_ingestion.tasks.alerts.lvk.util as task
from across_data_ingestion.util.across_server import sdk


class TestIngestLvkAlert:
    def test_should_raise_value_error_for_test_event(
        self,
        fake_alert: dict[str, Any],
        mock_process_contours: MagicMock,
        mock_broker_alert_api: MagicMock,
    ) -> None:
        """Should raise and skip ingestion for MDC/test events (superevent_id prefixed with MS)"""
        fake_alert["superevent_id"] = "MS240101a"

        with pytest.raises(ValueError, match="MS240101a"):
            task.ingest_lvk_alert(fake_alert)

        mock_process_contours.assert_not_called()
        mock_broker_alert_api.create_broker_alert.assert_not_called()

    def test_should_raise_value_error_for_unknown_alert_type(
        self,
        fake_alert: dict[str, Any],
        mock_process_contours: MagicMock,
        mock_broker_alert_api: MagicMock,
    ) -> None:
        """Should raise and skip ingestion if alert_type is not a known BrokerAlertStatus"""
        fake_alert["alert_type"] = "not_a_real_status"

        with pytest.raises(ValueError, match="not_a_real_status"):
            task.ingest_lvk_alert(fake_alert)

        mock_process_contours.assert_not_called()
        mock_broker_alert_api.create_broker_alert.assert_not_called()

    def test_should_call_process_contours_with_skymap(
        self, fake_alert: dict[str, Any], mock_process_contours: MagicMock
    ) -> None:
        """Should decode and process the skymap pulled from the alert event"""
        skymap_str = fake_alert["event"]["skymap"]

        task.ingest_lvk_alert(fake_alert)

        mock_process_contours.assert_called_once_with(skymap_str)

    def test_should_remove_skymap_from_event_payload(
        self, fake_alert: dict[str, Any]
    ) -> None:
        """Should pop the skymap out of the alert before storing it as the payload"""
        task.ingest_lvk_alert(fake_alert)

        assert "skymap" not in fake_alert["event"]

    def test_should_create_broker_alert_with_expected_fields(
        self,
        fake_alert: dict[str, Any],
        fake_localizations: list[sdk.LocalizationCreate],
        fake_broker_received_on: datetime,
        mock_broker_alert_api: MagicMock,
    ) -> None:
        """Should build the BrokerAlertCreate payload from the alert and localizations"""
        expected_alert = sdk.BrokerAlertCreate(
            status=sdk.BrokerAlertStatus.PRELIMINARY,
            broker_name="GCN Kafka LVK",
            data_source=sdk.BrokerAlertDataSource.IGWN,
            broker_event_type=sdk.BrokerEventType.GW,
            broker_event_name="S240101a",
            broker_received_on=fake_broker_received_on,
            broker_event_datetime=fake_alert["event"]["time"],
            payload=fake_alert,
            localizations=fake_localizations,
        )

        task.ingest_lvk_alert(fake_alert)

        mock_broker_alert_api.create_broker_alert.assert_called_once_with(
            expected_alert
        )

    def test_should_call_create_broker_alert(
        self, fake_alert: dict[str, Any], mock_broker_alert_api: MagicMock
    ) -> None:
        """Should submit the newly created broker alert to ACROSS server"""
        task.ingest_lvk_alert(fake_alert)

        mock_broker_alert_api.create_broker_alert.assert_called_once()

    def test_should_log_created_broker_alert(
        self, fake_alert: dict[str, Any], mock_logger: MagicMock
    ) -> None:
        """Should log an info message with the superevent id once created"""
        task.ingest_lvk_alert(fake_alert)

        mock_logger.info.assert_called_with(
            "Created new Broker Alert", broker_event_name="S240101a"
        )
