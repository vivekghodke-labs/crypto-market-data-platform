"""
Unit tests for DbtRunOperator and DbtTestOperator.

Strategy:
  - subprocess.Popen is mocked — no dbt installation required in CI.
  - Airflow context is constructed manually using MagicMock.
  - XCom push/pull is verified via mock assertions.
  - AirflowException is asserted on non-zero exit codes and test failures.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))
sys.path.insert(0, str(Path(__file__).parent.parent / "plugins"))

from airflow.exceptions import AirflowException
from operators.dbt_operator import DbtRunOperator, DbtTestOperator


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_context():
    """Minimal Airflow task context mock."""
    ti = MagicMock()
    dag_run = MagicMock()
    dag_run.run_id = "scheduled__2025-01-01T00:00:00+00:00"
    context = {
        "task_instance": ti,
        "dag_run": dag_run,
        "execution_date": "2025-01-01T00:00:00+00:00",
        "dag": MagicMock(dag_id="dbt_transformation_pipeline"),
    }
    return context


def _make_process_mock(
    returncode: int = 0,
    stdout_lines: list[str] | None = None,
    stderr: str = "",
):
    """Creates a mock Popen process with configurable output."""
    stdout_lines = stdout_lines or [
        json.dumps({"info": {"level": "info", "msg": "Running dbt..."}}),
        json.dumps({"info": {"level": "info", "msg": "Completed successfully"}}),
    ]
    mock_process = MagicMock()
    mock_process.returncode = returncode
    mock_process.stdout = iter(stdout_lines)
    mock_process.communicate.return_value = ("", stderr)
    return mock_process


# ─── DbtRunOperator Tests ─────────────────────────────────────────────────────

class TestDbtRunOperator:

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_successful_run_returns_result_dict(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=0)

        op = DbtRunOperator(
            task_id="test_run",
            models=["silver_deduped_trades"],
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
            target="dev",
        )
        result = op.execute(mock_context)

        assert result["exit_code"] == 0
        assert result["models"] == ["silver_deduped_trades"]
        assert result["target"] == "dev"
        assert "duration_seconds" in result

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_failed_run_raises_airflow_exception(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=1, stderr="dbt error")

        op = DbtRunOperator(
            task_id="test_run",
            models=["silver_deduped_trades"],
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        with pytest.raises(AirflowException) as exc_info:
            op.execute(mock_context)

        assert "dbt run failed" in str(exc_info.value)
        assert "silver_deduped_trades" in str(exc_info.value)

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_full_refresh_flag_included_in_command(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=0)

        op = DbtRunOperator(
            task_id="test_run",
            models=["gold_daily_ohlcv"],
            full_refresh=True,
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        op.execute(mock_context)

        cmd = mock_popen.call_args[0][0]
        assert "--full-refresh" in cmd

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_multiple_models_joined_in_select(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=0)

        op = DbtRunOperator(
            task_id="test_run",
            models=["model_a", "model_b", "model_c"],
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        op.execute(mock_context)

        cmd = " ".join(mock_popen.call_args[0][0])
        assert "model_a model_b model_c" in cmd

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_xcom_pushed_on_success(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=0)

        op = DbtRunOperator(
            task_id="test_run",
            models=["silver_deduped_trades"],
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        op.execute(mock_context)

        mock_context["task_instance"].xcom_push.assert_called_once_with(
            key="dbt_run_result",
            value=pytest.approx({"exit_code": 0, "models": ["silver_deduped_trades"],
                                  "target": "prod", "full_refresh": False,
                                  "duration_seconds": pytest.approx(0, abs=5)},
                                abs=1e-1),
        )

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_dbt_vars_serialised_as_json(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=0)

        op = DbtRunOperator(
            task_id="test_run",
            models=["model_a"],
            dbt_vars={"run_date": "2025-01-01"},
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        op.execute(mock_context)

        cmd = mock_popen.call_args[0][0]
        assert "--vars" in cmd
        vars_idx = cmd.index("--vars")
        assert json.loads(cmd[vars_idx + 1]) == {"run_date": "2025-01-01"}

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_json_log_format_flag_always_present(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=0)
        op = DbtRunOperator(task_id="t", models=["m"], project_dir="/p", profiles_dir="/p")
        op.execute(mock_context)
        cmd = mock_popen.call_args[0][0]
        assert "--log-format" in cmd
        assert "json" in cmd


# ─── DbtTestOperator Tests ────────────────────────────────────────────────────

class TestDbtTestOperator:

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_successful_tests_return_result_dict(self, mock_popen, mock_context) -> None:
        stdout = [
            json.dumps({"info": {"level": "info", "msg": "PASS: not_null_silver_trade_id"}}),
            json.dumps({"info": {"level": "info", "msg": "PASS: unique_silver_trade_id"}}),
        ]
        mock_popen.return_value = _make_process_mock(returncode=0, stdout_lines=stdout)

        op = DbtTestOperator(
            task_id="test_silver",
            select="silver",
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        result = op.execute(mock_context)

        assert result["exit_code"] == 0
        assert result["failed_count"] == 0
        assert result["passed_count"] == 2

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_failed_tests_raise_airflow_exception(self, mock_popen, mock_context) -> None:
        stdout = [
            json.dumps({"info": {"level": "info", "msg": "FAIL: assert_ohlcv_high_gte_low"}}),
        ]
        mock_popen.return_value = _make_process_mock(returncode=1, stdout_lines=stdout)

        op = DbtTestOperator(
            task_id="test_silver",
            select="silver",
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        with pytest.raises(AirflowException) as exc_info:
            op.execute(mock_context)

        assert "FAILED" in str(exc_info.value)
        assert "assert_ohlcv_high_gte_low" in str(exc_info.value)

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_zero_exit_code_with_fail_message_still_raises(
        self, mock_popen, mock_context
    ) -> None:
        """Guards against dbt exit 0 but FAIL in output — should still raise."""
        stdout = [
            json.dumps({"info": {"level": "info", "msg": "FAIL: some_test"}}),
        ]
        mock_popen.return_value = _make_process_mock(returncode=0, stdout_lines=stdout)

        op = DbtTestOperator(
            task_id="test_gold",
            select="gold",
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        with pytest.raises(AirflowException):
            op.execute(mock_context)

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_store_failures_flag_included(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=0)

        op = DbtTestOperator(
            task_id="t",
            select="silver",
            store_failures=True,
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        op.execute(mock_context)

        cmd = mock_popen.call_args[0][0]
        assert "--store-failures" in cmd

    @patch("operators.dbt_operator.subprocess.Popen")
    def test_xcom_pushed_with_test_result(self, mock_popen, mock_context) -> None:
        mock_popen.return_value = _make_process_mock(returncode=0)

        op = DbtTestOperator(
            task_id="t",
            select="silver",
            project_dir="/tmp/dbt",
            profiles_dir="/tmp/dbt",
        )
        op.execute(mock_context)

        mock_context["task_instance"].xcom_push.assert_called_once()
        call_kwargs = mock_context["task_instance"].xcom_push.call_args[1]
        assert call_kwargs["key"] == "dbt_test_result"