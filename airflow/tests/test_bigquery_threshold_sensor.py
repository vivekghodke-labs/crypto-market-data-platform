"""
Unit tests for BigQueryThresholdSensor.

Strategy:
  - BigQueryHook is mocked — no GCP credentials or live BigQuery required.
  - Tests cover all ThresholdOperator variants.
  - AirflowException is asserted on threshold breach with fail_on_breach=True.
  - Breach callback invocation is verified via mock.
  - XCom push content is validated.
  - Edge cases: empty result set, None value, zero value.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))
sys.path.insert(0, str(Path(__file__).parent.parent / "plugins"))

from airflow.exceptions import AirflowException
from operators.bigquery_threshold_sensor import (
    BigQueryThresholdSensor,
    ThresholdOperator,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_context():
    ti = MagicMock()
    dag = MagicMock()
    dag.dag_id = "pipeline_health_monitor"
    return {
        "task_instance": ti,
        "dag": dag,
        "run_id": "scheduled__2025-01-01T00:00:00+00:00",
    }


def _make_sensor(
    sql: str = "SELECT 5",
    threshold: int = 10,
    operator: ThresholdOperator = ThresholdOperator.LESS_THAN,
    fail_on_breach: bool = True,
    on_breach_callback=None,
    label: str = "test_check",
) -> BigQueryThresholdSensor:
    return BigQueryThresholdSensor(
        task_id="test_sensor",
        sql=sql,
        threshold=threshold,
        threshold_operator=operator,
        fail_on_breach=fail_on_breach,
        on_breach_callback=on_breach_callback,
        label=label,
        poke_interval=1,
        timeout=10,
    )


# ─── ThresholdOperator Evaluation Tests ──────────────────────────────────────

class TestThresholdOperatorEvaluation:

    def test_less_than_passes_when_actual_below_threshold(self) -> None:
        sensor = _make_sensor(threshold=100, operator=ThresholdOperator.LESS_THAN)
        assert sensor._evaluate(50.0) is True

    def test_less_than_fails_when_actual_equals_threshold(self) -> None:
        sensor = _make_sensor(threshold=100, operator=ThresholdOperator.LESS_THAN)
        assert sensor._evaluate(100.0) is False

    def test_less_than_fails_when_actual_above_threshold(self) -> None:
        sensor = _make_sensor(threshold=100, operator=ThresholdOperator.LESS_THAN)
        assert sensor._evaluate(101.0) is False

    def test_less_than_or_equal_passes_when_equal(self) -> None:
        sensor = _make_sensor(threshold=100, operator=ThresholdOperator.LESS_THAN_OR_EQUAL)
        assert sensor._evaluate(100.0) is True

    def test_greater_than_passes_when_actual_above_threshold(self) -> None:
        sensor = _make_sensor(threshold=50, operator=ThresholdOperator.GREATER_THAN)
        assert sensor._evaluate(51.0) is True

    def test_greater_than_fails_when_actual_equals_threshold(self) -> None:
        sensor = _make_sensor(threshold=50, operator=ThresholdOperator.GREATER_THAN)
        assert sensor._evaluate(50.0) is False

    def test_greater_than_or_equal_passes_when_equal(self) -> None:
        sensor = _make_sensor(threshold=50, operator=ThresholdOperator.GREATER_THAN_OR_EQUAL)
        assert sensor._evaluate(50.0) is True

    def test_equal_passes_when_values_match(self) -> None:
        sensor = _make_sensor(threshold=42, operator=ThresholdOperator.EQUAL)
        assert sensor._evaluate(42.0) is True

    def test_equal_fails_when_values_differ(self) -> None:
        sensor = _make_sensor(threshold=42, operator=ThresholdOperator.EQUAL)
        assert sensor._evaluate(43.0) is False


# ─── Poke — Threshold Passed Tests ───────────────────────────────────────────

class TestBigQueryThresholdSensorPoke:

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_poke_returns_true_when_condition_met(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = [[50]]
        sensor = _make_sensor(threshold=100, operator=ThresholdOperator.LESS_THAN)
        result = sensor.poke(mock_context)
        assert result is True

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_poke_pushes_xcom_on_success(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = [[75]]
        sensor = _make_sensor(threshold=100, label="dead_letter_count")
        sensor.poke(mock_context)

        mock_context["task_instance"].xcom_push.assert_called_once_with(
            key="threshold_check_result",
            value={
                "label": "dead_letter_count",
                "actual_value": 75.0,
                "threshold": 100,
                "operator": "lt",
                "breached": False,
            },
        )

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_poke_raises_on_breach_when_fail_on_breach_true(
        self, mock_hook_class, mock_context
    ) -> None:
        # actual=150 > threshold=100 → LESS_THAN violated
        mock_hook_class.return_value.get_records.return_value = [[150]]
        sensor = _make_sensor(
            threshold=100,
            operator=ThresholdOperator.LESS_THAN,
            fail_on_breach=True,
        )
        with pytest.raises(AirflowException) as exc_info:
            sensor.poke(mock_context)
        assert "Threshold breach" in str(exc_info.value)
        assert "150" in str(exc_info.value)

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_poke_returns_false_on_breach_when_fail_on_breach_false(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = [[150]]
        sensor = _make_sensor(
            threshold=100,
            operator=ThresholdOperator.LESS_THAN,
            fail_on_breach=False,
        )
        result = sensor.poke(mock_context)
        assert result is False

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_breach_callback_invoked_on_threshold_violation(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = [[200]]
        breach_callback = MagicMock()

        sensor = _make_sensor(
            threshold=100,
            operator=ThresholdOperator.LESS_THAN,
            fail_on_breach=False,
            on_breach_callback=breach_callback,
            label="dead_letter_test",
        )
        sensor.poke(mock_context)

        breach_callback.assert_called_once_with(
            dag_id="pipeline_health_monitor",
            run_id="scheduled__2025-01-01T00:00:00+00:00",
            table="dead_letter_test",
            count=200,
            threshold=100,
        )

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_breach_callback_not_invoked_when_condition_met(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = [[50]]
        breach_callback = MagicMock()

        sensor = _make_sensor(
            threshold=100,
            on_breach_callback=breach_callback,
        )
        sensor.poke(mock_context)
        breach_callback.assert_not_called()


# ─── Edge Case Tests ──────────────────────────────────────────────────────────

class TestBigQueryThresholdSensorEdgeCases:

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_empty_result_set_treated_as_zero(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = []
        # Zero is less than 100 → condition met
        sensor = _make_sensor(
            threshold=100, operator=ThresholdOperator.LESS_THAN
        )
        result = sensor.poke(mock_context)
        assert result is True

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_none_result_treated_as_zero(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = [[None]]
        sensor = _make_sensor(
            threshold=100, operator=ThresholdOperator.LESS_THAN
        )
        result = sensor.poke(mock_context)
        assert result is True

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_zero_actual_value_evaluated_correctly(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = [[0]]
        sensor = _make_sensor(
            threshold=100, operator=ThresholdOperator.LESS_THAN
        )
        result = sensor.poke(mock_context)
        assert result is True

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_xcom_breached_true_on_violation(
        self, mock_hook_class, mock_context
    ) -> None:
        mock_hook_class.return_value.get_records.return_value = [[999]]
        sensor = _make_sensor(
            threshold=100,
            operator=ThresholdOperator.LESS_THAN,
            fail_on_breach=False,
            label="test_label",
        )
        sensor.poke(mock_context)

        xcom_value = mock_context["task_instance"].xcom_push.call_args[1]["value"]
        assert xcom_value["breached"] is True
        assert xcom_value["actual_value"] == 999.0

    @patch("operators.bigquery_threshold_sensor.BigQueryHook")
    def test_breach_callback_failure_does_not_mask_breach_exception(
        self, mock_hook_class, mock_context
    ) -> None:
        """If breach callback raises, the AirflowException must still propagate."""
        mock_hook_class.return_value.get_records.return_value = [[200]]
        failing_callback = MagicMock(side_effect=RuntimeError("callback failed"))

        sensor = _make_sensor(
            threshold=100,
            operator=ThresholdOperator.LESS_THAN,
            fail_on_breach=True,
            on_breach_callback=failing_callback,
        )
        # AirflowException must still be raised even though callback failed
        with pytest.raises(AirflowException):
            sensor.poke(mock_context)