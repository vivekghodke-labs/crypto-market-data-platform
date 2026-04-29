"""
DAG 1: dbt Silver + Gold transformation pipeline.

Schedule: Every 15 minutes.
SLA:      30 minutes (full pipeline must complete within 30 min of scheduled time).

Task dependency graph:
  check_previous_run_success
          │
          ▼
  dbt_run_silver_deduped_trades
          │
          ▼
  dbt_run_silver_ohlcv_validated
          │
          ▼
  dbt_test_silver ──► [CIRCUIT BREAKER] ── FAIL → notify_pipeline_failure
          │                                         (TriggerRule.ONE_FAILED)
          │ (ALL_SUCCESS only)
          ▼
  ┌──────────────────────────────────────────────────┐
  │  dbt_run_gold_daily_ohlcv                        │
  │  dbt_run_gold_price_stats_24h     (parallel)     │
  │  dbt_run_gold_trade_volume_hourly                │
  └──────────────────────────────────────────────────┘
          │
          ▼
  dbt_test_gold ──► [CIRCUIT BREAKER] ── FAIL → notify_pipeline_failure
          │
          ▼
  mark_pipeline_success (XCom: run summary)

Enterprise patterns:
  - Idempotent: dbt incremental merge on Silver, full-refresh on Gold.
  - Circuit breaker: TriggerRule.ALL_SUCCESS on every post-test task group.
  - SLA miss callback: fires structured CRITICAL log on 30-min breach.
  - Per-task retry: 3 retries with exponential backoff (1→2→4 min).
  - Zero hardcoded config: all paths from constants.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from dag_utils.callbacks import (
    on_task_failure,
    on_task_retry,
    on_dag_success,
    on_sla_miss,
)
from dag_utils.constants import (
    DBT_SILVER_MODELS,
    DBT_GOLD_MODELS,
    TAG_TRANSFORMATION,
    TAG_DBT,
    TAG_SILVER,
    TAG_GOLD,
    TAG_PLATFORM,
    TASK_RETRIES,
    TASK_RETRY_DELAY_MINUTES,
    TASK_EXECUTION_TIMEOUT_MINUTES,
    TRANSFORMATION_SLA_MINUTES,
)

logger = logging.getLogger(__name__)

# ─── Default Args ─────────────────────────────────────────────────────────────
# Applied to every task in the DAG unless overridden at the task level.

DEFAULT_ARGS = {
    "owner": "platform-engineering",
    "depends_on_past": False,
    "retries": TASK_RETRIES,
    "retry_delay": timedelta(minutes=TASK_RETRY_DELAY_MINUTES),
    "retry_exponential_backoff": True,      # Doubles delay on each retry: 1→2→4 min
    "max_retry_delay": timedelta(minutes=8),
    "execution_timeout": timedelta(minutes=TASK_EXECUTION_TIMEOUT_MINUTES),
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
    "email_on_failure": False,              # Handled by on_failure_callback
    "email_on_retry": False,
}


@dag(
    dag_id="dbt_transformation_pipeline",
    description=(
        "Orchestrates dbt Silver and Gold layer transformations. "
        "Runs every 15 minutes. SLA: 30 minutes."
    ),
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,                          # Do not backfill missed runs on deploy
    max_active_runs=1,                      # Prevent concurrent pipeline runs
    default_args=DEFAULT_ARGS,
    sla_miss_callback=on_sla_miss,
    on_success_callback=on_dag_success,
    tags=[TAG_TRANSFORMATION, TAG_DBT, TAG_SILVER, TAG_GOLD, TAG_PLATFORM],
    doc_md="""
## dbt Transformation Pipeline

Orchestrates the Medallion Architecture transformation layer:

1. **Silver — Deduplication**: Removes duplicate trade_ids from Bronze raw_trades
   using incremental merge strategy.
2. **Silver — OHLCV Validation**: Applies structural OHLCV invariant checks
   and deduplicates Beam window panes.
3. **Silver Test Gate**: All dbt schema + custom tests must pass before Gold runs.
   Failure halts the pipeline (circuit breaker).
4. **Gold — Parallel Execution**: Three Gold models run in parallel after Silver gate.
5. **Gold Test Gate**: Final quality validation before marking the run successful.

**SLA**: 30 minutes from scheduled start time.
**Schedule**: Every 15 minutes (UTC).
**Retry policy**: 3 retries, exponential backoff (1→2→4 minutes).
    """,
)
def dbt_transformation_pipeline():

    # ── Import operators inside DAG function ───────────────────────────────
    # Deferred import prevents Airflow scheduler parse errors if the
    # plugin is not yet installed during DAG discovery.
    from plugins.operators.dbt_operator import DbtRunOperator, DbtTestOperator

    # ── Task: Preflight check ──────────────────────────────────────────────
    # Verifies the DAG environment is correctly configured before any
    # dbt subprocess is spawned. Catches misconfiguration early.
    @task(
        task_id="check_environment",
        sla=timedelta(minutes=2),
    )
    def check_environment(**context) -> dict:
        """Validates dbt project directory and profiles exist."""
        import os
        from dag_utils.constants import DBT_PROJECT_DIR, DBT_PROFILES_DIR

        checks = {
            "dbt_project_dir_exists": os.path.isdir(DBT_PROJECT_DIR),
            "profiles_dir_exists": os.path.isdir(DBT_PROFILES_DIR),
            "dbt_project_yml_exists": os.path.isfile(
                f"{DBT_PROJECT_DIR}/dbt_project.yml"
            ),
        }

        failed = [k for k, v in checks.items() if not v]
        if failed:
            raise AirflowException(
                f"Environment preflight failed: {failed}. "
                f"Ensure the dbt project is correctly mounted in the Airflow container."
            )

        logger.info("Environment preflight passed: %s", checks)
        return checks

    # ── Task: Silver — deduped_trades ──────────────────────────────────────
    run_silver_deduped = DbtRunOperator(
        task_id="dbt_run_silver_deduped_trades",
        models=["silver_deduped_trades"],
        full_refresh=False,                 # Incremental merge — never full refresh
        sla=timedelta(minutes=10),
    )

    # ── Task: Silver — ohlcv_validated ────────────────────────────────────
    # View materialisation — near-instant, but still gated for observability.
    run_silver_ohlcv = DbtRunOperator(
        task_id="dbt_run_silver_ohlcv_validated",
        models=["silver_ohlcv_validated"],
        full_refresh=False,
        sla=timedelta(minutes=3),
    )

    # ── Task: Silver test gate (CIRCUIT BREAKER) ───────────────────────────
    # Raises AirflowException on any test failure.
    # TriggerRule.ALL_SUCCESS is Airflow default — Gold tasks only run
    # if this task succeeds.
    test_silver = DbtTestOperator(
        task_id="dbt_test_silver",
        select="silver",
        store_failures=True,
        sla=timedelta(minutes=5),
    )

    # ── Tasks: Gold — parallel execution ──────────────────────────────────
    run_gold_daily_ohlcv = DbtRunOperator(
        task_id="dbt_run_gold_daily_ohlcv",
        models=["gold_daily_ohlcv"],
        full_refresh=True,
        sla=timedelta(minutes=10),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    run_gold_price_stats = DbtRunOperator(
        task_id="dbt_run_gold_price_stats_24h",
        models=["gold_price_stats_24h"],
        full_refresh=True,
        sla=timedelta(minutes=10),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    run_gold_volume_hourly = DbtRunOperator(
        task_id="dbt_run_gold_trade_volume_hourly",
        models=["gold_trade_volume_hourly"],
        full_refresh=True,
        sla=timedelta(minutes=10),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task: Gold test gate (CIRCUIT BREAKER) ─────────────────────────────
    test_gold = DbtTestOperator(
        task_id="dbt_test_gold",
        select="gold",
        store_failures=True,
        sla=timedelta(minutes=5),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task: Pipeline success marker ─────────────────────────────────────
    @task(
        task_id="mark_pipeline_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    def mark_pipeline_success(**context) -> dict:
        """
        Records pipeline completion metadata to XCom.
        Downstream monitoring DAG can read this via ExternalTaskSensor.
        """
        ti = context["task_instance"]
        dag_run = context["dag_run"]

        # Pull XCom results from dbt tasks for audit summary
        silver_deduped_result = ti.xcom_pull(
            task_ids="dbt_run_silver_deduped_trades",
            key="dbt_run_result",
        )
        gold_stats_result = ti.xcom_pull(
            task_ids="dbt_run_gold_price_stats_24h",
            key="dbt_run_result",
        )

        summary = {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "execution_date": str(context["execution_date"]),
            "status": "success",
            "silver_deduped_duration_s": (
                silver_deduped_result.get("duration_seconds")
                if silver_deduped_result else None
            ),
            "gold_stats_duration_s": (
                gold_stats_result.get("duration_seconds")
                if gold_stats_result else None
            ),
        }

        logger.info("Pipeline completed successfully: %s", summary)
        return summary

    # ── Task: Failure notifier ─────────────────────────────────────────────
    # TriggerRule.ONE_FAILED: runs if ANY upstream task fails.
    # Does NOT block — runs in parallel with the failed branch.
    @task(
        task_id="notify_pipeline_failure",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,                          # Notification must not retry
    )
    def notify_pipeline_failure(**context) -> None:
        """
        Fires on any pipeline task failure.
        In production: wire Slack/PagerDuty provider hooks here.
        """
        dag_run = context["dag_run"]
        failed_tasks = [
            ti.task_id
            for ti in dag_run.get_task_instances()
            if ti.state == "failed"
        ]

        from dag_utils.callbacks import _emit_structured_log
        _emit_structured_log(
            severity="CRITICAL",
            event="transformation_pipeline_failed",
            payload={
                "dag_id": dag_run.dag_id,
                "run_id": dag_run.run_id,
                "failed_tasks": failed_tasks,
                "action": (
                    "Gold layer may be stale. "
                    "Investigate failed tasks and re-trigger the DAG."
                ),
            },
        )

    # ── Wire task dependencies ─────────────────────────────────────────────
    from airflow.exceptions import AirflowException  # noqa — available in task scope

    env_check = check_environment()

    (
        env_check
        >> run_silver_deduped
        >> run_silver_ohlcv
        >> test_silver
        >> [run_gold_daily_ohlcv, run_gold_price_stats, run_gold_volume_hourly]
        >> test_gold
        >> mark_pipeline_success()
    )

    # Failure notifier is triggered by any task failure — independent branch
    [
        env_check,
        run_silver_deduped,
        run_silver_ohlcv,
        test_silver,
        run_gold_daily_ohlcv,
        run_gold_price_stats,
        run_gold_volume_hourly,
        test_gold,
    ] >> notify_pipeline_failure()


dbt_transformation_pipeline()