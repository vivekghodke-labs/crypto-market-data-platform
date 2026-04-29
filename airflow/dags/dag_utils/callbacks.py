"""
Airflow DAG and task-level callback functions.

All callbacks emit structured JSON to stdout — compatible with GCP Cloud
Logging. In a production deployment with Cloud Composer, these JSON log
lines are automatically indexed and queryable via Log Analytics.

Callback types implemented:
  on_failure_callback   → fires on any task failure
  on_retry_callback     → fires on task retry
  on_success_callback   → fires on DAG-level success
  sla_miss_callback     → fires when a task misses its SLA deadline

Enterprise design:
  - All callbacks are pure functions — no side effects beyond logging.
  - Email/PagerDuty/Slack integration is wired here in production by
    adding the relevant Airflow provider hook calls.
  - Callbacks never raise exceptions — a callback failure must not
    mask the original task failure.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import TaskInstance, DagRun

logger = logging.getLogger(__name__)


# ─── Structured Log Helpers ───────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit_structured_log(severity: str, event: str, payload: dict) -> None:
    """
    Emits a single-line JSON log record to stdout.
    GCP Cloud Logging parses these automatically when running on Cloud Run
    or Cloud Composer.
    """
    record = {
        "severity": severity,
        "event": event,
        "timestamp": _now_iso(),
        "platform": "crypto-market-data",
        **payload,
    }
    print(json.dumps(record, default=str), flush=True)


def _extract_task_context(context: dict) -> dict:
    """Extracts standardised task metadata from Airflow task context."""
    ti: TaskInstance = context.get("task_instance")
    dag_run: DagRun = context.get("dag_run")

    return {
        "dag_id": context.get("dag").dag_id if context.get("dag") else "unknown",
        "task_id": ti.task_id if ti else "unknown",
        "run_id": dag_run.run_id if dag_run else "unknown",
        "execution_date": str(context.get("execution_date", "unknown")),
        "try_number": ti.try_number if ti else 0,
        "hostname": ti.hostname if ti else "unknown",
    }


# ─── Task Failure Callback ────────────────────────────────────────────────────

def on_task_failure(context: dict) -> None:
    """
    Fires on any task failure.
    Logs a structured ERROR record with full task context and exception info.

    In production: extend this to call the Airflow EmailOperator hook,
    PagerDuty provider, or Slack WebhookOperator as required by SLA policy.
    """
    try:
        task_ctx = _extract_task_context(context)
        exception = context.get("exception")

        _emit_structured_log(
            severity="ERROR",
            event="task_failed",
            payload={
                **task_ctx,
                "exception_type": type(exception).__name__ if exception else "unknown",
                "exception_message": str(exception) if exception else "unknown",
                "action": "investigate_immediately",
            },
        )
    except Exception as cb_exc:
        # Callback must never raise — log minimally and return
        logger.error("on_task_failure callback itself failed: %s", cb_exc)


# ─── Task Retry Callback ──────────────────────────────────────────────────────

def on_task_retry(context: dict) -> None:
    """
    Fires on each task retry.
    Logs a WARNING — retries are expected under transient GCP API errors
    but repeated retries indicate a systemic issue requiring investigation.
    """
    try:
        task_ctx = _extract_task_context(context)
        exception = context.get("exception")
        ti: TaskInstance = context.get("task_instance")

        _emit_structured_log(
            severity="WARNING",
            event="task_retrying",
            payload={
                **task_ctx,
                "max_retries": ti.max_tries if ti else "unknown",
                "exception_message": str(exception) if exception else "unknown",
                "action": "monitor_retry_progression",
            },
        )
    except Exception as cb_exc:
        logger.error("on_task_retry callback itself failed: %s", cb_exc)


# ─── DAG Success Callback ─────────────────────────────────────────────────────

def on_dag_success(context: dict) -> None:
    """
    Fires on successful completion of the full DAG run.
    Logs an INFO record — useful for run duration tracking and audit.
    """
    try:
        dag_run: DagRun = context.get("dag_run")
        dag = context.get("dag")

        duration_seconds: float | None = None
        if dag_run and dag_run.start_date and dag_run.end_date:
            duration_seconds = (
                dag_run.end_date - dag_run.start_date
            ).total_seconds()

        _emit_structured_log(
            severity="INFO",
            event="dag_completed_successfully",
            payload={
                "dag_id": dag.dag_id if dag else "unknown",
                "run_id": dag_run.run_id if dag_run else "unknown",
                "execution_date": str(context.get("execution_date", "unknown")),
                "duration_seconds": duration_seconds,
            },
        )
    except Exception as cb_exc:
        logger.error("on_dag_success callback itself failed: %s", cb_exc)


# ─── SLA Miss Callback ────────────────────────────────────────────────────────

def on_sla_miss(
    dag,
    task_list: str,
    blocking_task_list: str,
    slas: list,
    blocking_tis: list,
) -> None:
    """
    Fires when a task misses its declared SLA deadline.

    Airflow SLA miss callbacks have a different signature from task callbacks —
    they receive the DAG object and lists of affected tasks directly.

    An SLA miss means the pipeline is taking longer than the contractual
    window defined in the DAG. This is a severity=CRITICAL event because
    it directly impacts Gold layer freshness for downstream consumers
    (Looker Studio, API clients).
    """
    try:
        missed_tasks = [str(sla) for sla in slas]
        blocking_tasks = [str(ti) for ti in blocking_tis]

        _emit_structured_log(
            severity="CRITICAL",
            event="sla_miss_detected",
            payload={
                "dag_id": dag.dag_id,
                "missed_tasks": missed_tasks,
                "blocking_tasks": blocking_tasks,
                "task_list": task_list,
                "blocking_task_list": blocking_task_list,
                "action": "escalate_immediately — Gold layer freshness SLA breached",
            },
        )
    except Exception as cb_exc:
        logger.error("on_sla_miss callback itself failed: %s", cb_exc)


# ─── Dead Letter Alert ────────────────────────────────────────────────────────

def on_dead_letter_threshold_exceeded(
    dag_id: str,
    run_id: str,
    table: str,
    count: int,
    threshold: int,
) -> None:
    """
    Fires when dead-letter message volume exceeds the configured threshold.
    Called explicitly from the BigQueryThresholdSensor operator.

    A dead-letter spike indicates schema drift from the Binance API,
    a Beam pipeline defect, or an upstream data quality issue.
    """
    try:
        _emit_structured_log(
            severity="CRITICAL",
            event="dead_letter_threshold_exceeded",
            payload={
                "dag_id": dag_id,
                "run_id": run_id,
                "table": table,
                "message_count": count,
                "threshold": threshold,
                "excess": count - threshold,
                "action": (
                    "investigate_beam_pipeline — possible schema drift "
                    "from Binance API or Beam validation defect"
                ),
            },
        )
    except Exception as cb_exc:
        logger.error("on_dead_letter_threshold_exceeded callback failed: %s", cb_exc)


# ─── Freshness Breach Alert ───────────────────────────────────────────────────

def on_freshness_breach(
    dag_id: str,
    run_id: str,
    source: str,
    last_loaded_at: str,
    stale_minutes: float,
) -> None:
    """
    Fires when a source table freshness check detects stale data.
    Called explicitly from the freshness check task.
    """
    try:
        _emit_structured_log(
            severity="ERROR",
            event="source_freshness_breach",
            payload={
                "dag_id": dag_id,
                "run_id": run_id,
                "source": source,
                "last_loaded_at": last_loaded_at,
                "stale_minutes": stale_minutes,
                "action": (
                    "check_ingestor_and_beam — data pipeline may be interrupted"
                ),
            },
        )
    except Exception as cb_exc:
        logger.error("on_freshness_breach callback failed: %s", cb_exc)