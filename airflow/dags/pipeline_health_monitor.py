"""
DAG 2: End-to-end platform health monitoring.

Schedule: Every 5 minutes.
SLA:      10 minutes (all health checks must complete within 10 min).

Task dependency graph:
  ┌─────────────────────────────────────────────────────────────┐
  │  check_bronze_source_freshness                               │
  │  check_silver_source_freshness        (parallel)            │
  └──────────────┬──────────────────────────────────────────────┘
                 │ (ALL_SUCCESS)
                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  check_dead_letter_volume_pipeline_table                     │
  │  check_dead_letter_volume_ingestor_topic  (parallel)        │
  └──────────────┬──────────────────────────────────────────────┘
                 │ (ALL_SUCCESS)
                 ▼
  check_ingestor_health_endpoint
                 │
                 ▼
  check_silver_row_count_anomaly
                 │
                 ▼
  log_health_summary (XCom aggregation)

  notify_health_failure (TriggerRule.ONE_FAILED — parallel branch)

Enterprise patterns:
  - BigQueryThresholdSensor for metric-based assertions
  - HttpSensor for Cloud Run liveness check
  - XCom-driven health summary for external observability
  - All thresholds configurable via Airflow Variables at runtime
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.trigger_rule import TriggerRule

from dag_utils.callbacks import (
    on_task_failure,
    on_task_retry,
    on_dag_success,
    on_sla_miss,
    on_dead_letter_threshold_exceeded,
    on_freshness_breach,
)
from dag_utils.constants import (
    GCP_PROJECT_ID,
    BQ_BRONZE_DATASET,
    BQ_SILVER_DATASET,
    BQ_DEAD_LETTER_TABLE,
    BQ_RAW_TRADES_TABLE,
    BQ_OHLCV_TABLE,
    AIRFLOW_VAR_DEAD_LETTER_MAX_PER_HOUR,
    AIRFLOW_VAR_INGESTOR_HEALTH_URL,
    AIRFLOW_VAR_SILVER_MIN_ROWS_PER_HOUR,
    DEFAULT_DEAD_LETTER_MAX_PER_HOUR,
    DEFAULT_SILVER_MIN_ROWS_PER_HOUR,
    TAG_MONITORING,
    TAG_HEALTH,
    TAG_PLATFORM,
    TASK_RETRIES,
    TASK_RETRY_DELAY_MINUTES,
    HEALTH_MONITOR_SLA_MINUTES,
)

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "platform-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=4),
    "execution_timeout": timedelta(minutes=8),
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="pipeline_health_monitor",
    description=(
        "Monitors end-to-end platform health: source freshness, "
        "dead-letter volume, ingestor liveness, and Silver row count anomalies. "
        "Runs every 5 minutes. SLA: 10 minutes."
    ),
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=on_sla_miss,
    on_success_callback=on_dag_success,
    tags=[TAG_MONITORING, TAG_HEALTH, TAG_PLATFORM],
    doc_md="""
## Pipeline Health Monitor

Continuously monitors the health of all platform components:

1. **Source Freshness**: Verifies Bronze and Silver tables received data
   within the expected SLA window (warn: 2h, error: 6h).
2. **Dead Letter Volume**: Alerts if pipeline dead-letter message count
   exceeds the configured threshold (default: 100/hour).
3. **Ingestor Health**: HTTP liveness check against the Cloud Run `/health`
   endpoint — verifies the WebSocket connection is active.
4. **Silver Row Count Anomaly**: Detects abnormally low ingestion volume
   in the Silver layer — indicates Binance stream issues or Beam pipeline failures.

**SLA**: 10 minutes from scheduled start time.
**Schedule**: Every 5 minutes (UTC).
**Thresholds**: Configurable via Airflow Variables at runtime — no restart required.
    """,
)
def pipeline_health_monitor():

    from plugins.operators.bigquery_threshold_sensor import (
        BigQueryThresholdSensor,
        ThresholdOperator,
    )

    # ── Runtime threshold resolution ───────────────────────────────────────
    # Read from Airflow Variables at DAG parse time.
    # Variable.get() returns the default if the Variable is not set.
    dead_letter_threshold = int(
        Variable.get(
            AIRFLOW_VAR_DEAD_LETTER_MAX_PER_HOUR,
            default_var=DEFAULT_DEAD_LETTER_MAX_PER_HOUR,
        )
    )
    silver_min_rows = int(
        Variable.get(
            AIRFLOW_VAR_SILVER_MIN_ROWS_PER_HOUR,
            default_var=DEFAULT_SILVER_MIN_ROWS_PER_HOUR,
        )
    )

    # ── Task group 1: Source freshness ─────────────────────────────────────

    @task(
        task_id="check_bronze_source_freshness",
        sla=timedelta(minutes=3),
    )
    def check_bronze_source_freshness(**context) -> dict:
        """
        Checks that bronze_raw.raw_trades has received data in the last 2 hours.
        A stale Bronze table means the Beam pipeline or ingestor has stopped.
        """
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from dag_utils.callbacks import on_freshness_breach

        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)

        sql = f"""
            SELECT
                MAX(ingested_at)                                        AS last_loaded_at,
                TIMESTAMP_DIFF(
                    CURRENT_TIMESTAMP(), MAX(ingested_at), MINUTE
                )                                                       AS stale_minutes
            FROM `{GCP_PROJECT_ID}.{BQ_BRONZE_DATASET}.raw_trades`
        """

        records = hook.get_records(sql)

        if not records or records[0][0] is None:
            raise Exception(
                "bronze_raw.raw_trades has NO data — table is empty. "
                "The Beam pipeline has never written to this table."
            )

        last_loaded_at = str(records[0][0])
        stale_minutes = float(records[0][1])

        result = {
            "source": f"{BQ_BRONZE_DATASET}.raw_trades",
            "last_loaded_at": last_loaded_at,
            "stale_minutes": stale_minutes,
        }

        if stale_minutes > 360:  # 6 hours — error threshold
            on_freshness_breach(
                dag_id=context["dag"].dag_id,
                run_id=context["run_id"],
                source="bronze_raw.raw_trades",
                last_loaded_at=last_loaded_at,
                stale_minutes=stale_minutes,
            )
            raise Exception(
                f"FRESHNESS ERROR: bronze_raw.raw_trades is {stale_minutes:.1f} "
                f"minutes stale (threshold: 360 min). "
                f"Last record: {last_loaded_at}"
            )

        if stale_minutes > 120:  # 2 hours — warn threshold (log but don't fail)
            logger.warning(
                "FRESHNESS WARNING: bronze_raw.raw_trades is %.1f minutes stale "
                "(warn threshold: 120 min). Last: %s",
                stale_minutes, last_loaded_at,
            )

        logger.info(
            "Bronze freshness OK | stale_minutes=%.1f | last_loaded_at=%s",
            stale_minutes, last_loaded_at,
        )
        return result

    @task(
        task_id="check_silver_source_freshness",
        sla=timedelta(minutes=3),
    )
    def check_silver_source_freshness(**context) -> dict:
        """
        Checks that silver_curated.ohlcv_1min has received data recently.
        Stale Silver means Beam is not writing OHLCV windows to BigQuery.
        """
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from dag_utils.callbacks import on_freshness_breach

        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)

        sql = f"""
            SELECT
                MAX(ingested_at)                                        AS last_loaded_at,
                TIMESTAMP_DIFF(
                    CURRENT_TIMESTAMP(), MAX(ingested_at), MINUTE
                )                                                       AS stale_minutes
            FROM `{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.ohlcv_1min`
        """

        records = hook.get_records(sql)

        if not records or records[0][0] is None:
            raise Exception(
                "silver_curated.ohlcv_1min has NO data. "
                "The Beam OHLCV pipeline has never successfully written to Silver."
            )

        last_loaded_at = str(records[0][0])
        stale_minutes = float(records[0][1])

        if stale_minutes > 360:
            on_freshness_breach(
                dag_id=context["dag"].dag_id,
                run_id=context["run_id"],
                source="silver_curated.ohlcv_1min",
                last_loaded_at=last_loaded_at,
                stale_minutes=stale_minutes,
            )
            raise Exception(
                f"FRESHNESS ERROR: silver_curated.ohlcv_1min is {stale_minutes:.1f} "
                f"minutes stale. Last: {last_loaded_at}"
            )

        logger.info(
            "Silver freshness OK | stale_minutes=%.1f | last_loaded_at=%s",
            stale_minutes, last_loaded_at,
        )
        return {"source": "silver_curated.ohlcv_1min", "stale_minutes": stale_minutes}

    # ── Task group 2: Dead letter volume checks ────────────────────────────

    check_dead_letter_pipeline = BigQueryThresholdSensor(
        task_id="check_dead_letter_volume_pipeline",
        sql=f"""
            SELECT COUNT(*)
            FROM `{BQ_DEAD_LETTER_TABLE}`
            WHERE logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """,
        threshold=dead_letter_threshold,
        threshold_operator=ThresholdOperator.LESS_THAN,
        label="pipeline_dead_letter_count_last_hour",
        on_breach_callback=on_dead_letter_threshold_exceeded,
        fail_on_breach=True,
        poke_interval=30,
        timeout=120,
        sla=timedelta(minutes=4),
    )

    check_dead_letter_ingestor = BigQueryThresholdSensor(
        task_id="check_dead_letter_volume_ingestor",
        sql=f"""
            SELECT COUNT(*)
            FROM `{GCP_PROJECT_ID}.{BQ_BRONZE_DATASET}.pipeline_dead_letter`
            WHERE logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
              AND pipeline_error = 'schema_validation_failure'
        """,
        threshold=dead_letter_threshold,
        threshold_operator=ThresholdOperator.LESS_THAN,
        label="ingestor_schema_failures_last_hour",
        on_breach_callback=on_dead_letter_threshold_exceeded,
        fail_on_breach=True,
        poke_interval=30,
        timeout=120,
        sla=timedelta(minutes=4),
    )

    # ── Task: Ingestor health check ────────────────────────────────────────

    @task(
        task_id="check_ingestor_health_endpoint",
        sla=timedelta(minutes=3),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    def check_ingestor_health_endpoint(**context) -> dict:
        """
        Performs an HTTP GET to the Cloud Run ingestor /health endpoint.
        Fails if:
          - HTTP status is not 200
          - Response body stats.running != true
          - Request times out (10s)

        The Cloud Run URL is read from Airflow Variable 'ingestor_cloud_run_url'.
        """
        import urllib.request
        import urllib.error
        import json as json_lib

        health_url = Variable.get(
            AIRFLOW_VAR_INGESTOR_HEALTH_URL,
            default_var=None,
        )

        if not health_url:
            raise Exception(
                "Airflow Variable 'ingestor_cloud_run_url' is not set. "
                "Run: airflow variables set ingestor_cloud_run_url <cloud-run-url>"
            )

        endpoint = f"{health_url.rstrip('/')}/health"
        logger.info("Checking ingestor health at: %s", endpoint)

        try:
            req = urllib.request.Request(endpoint, method="GET")
            with urllib.request.urlopen(req, timeout=10) as response:
                status_code = response.status
                body = json_lib.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise Exception(
                f"Ingestor health endpoint returned HTTP {exc.code}: {endpoint}"
            )
        except Exception as exc:
            raise Exception(
                f"Ingestor health endpoint unreachable: {endpoint} — {exc}"
            )

        if status_code != 200:
            raise Exception(
                f"Ingestor health returned HTTP {status_code} (expected 200): "
                f"{endpoint}"
            )

        stats = body.get("stats", {})
        if not stats.get("running", False):
            raise Exception(
                f"Ingestor WebSocket client is NOT running. "
                f"Health response: {body}. "
                f"Cloud Run container may need restart."
            )

        result = {
            "status": body.get("status"),
            "running": stats.get("running"),
            "messages_processed": stats.get("messages_processed"),
            "messages_rejected": stats.get("messages_rejected"),
            "reconnect_attempts": stats.get("reconnect_attempts"),
        }

        logger.info("Ingestor health OK: %s", result)
        return result

    # ── Task: Silver row count anomaly detection ───────────────────────────

    check_silver_row_count = BigQueryThresholdSensor(
        task_id="check_silver_row_count_anomaly",
        sql=f"""
            SELECT COUNT(*)
            FROM `{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.ohlcv_1min`
            WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """,
        threshold=silver_min_rows,
        threshold_operator=ThresholdOperator.GREATER_THAN_OR_EQUAL,
        label="silver_ohlcv_rows_last_hour",
        fail_on_breach=True,
        poke_interval=30,
        timeout=120,
        sla=timedelta(minutes=4),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task: Health summary ───────────────────────────────────────────────

    @task(
        task_id="log_health_summary",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    def log_health_summary(**context) -> dict:
        """
        Aggregates all health check results from XCom and logs a structured
        summary. This record is queryable via GCP Log Analytics.
        """
        ti = context["task_instance"]
        dag_run = context["dag_run"]

        bronze_result = ti.xcom_pull(task_ids="check_bronze_source_freshness") or {}
        silver_result = ti.xcom_pull(task_ids="check_silver_source_freshness") or {}
        ingestor_result = ti.xcom_pull(task_ids="check_ingestor_health_endpoint") or {}
        dl_pipeline = ti.xcom_pull(
            task_ids="check_dead_letter_volume_pipeline",
            key="threshold_check_result",
        ) or {}
        silver_rows = ti.xcom_pull(
            task_ids="check_silver_row_count_anomaly",
            key="threshold_check_result",
        ) or {}

        summary = {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "execution_date": str(context["execution_date"]),
            "overall_status": "healthy",
            "bronze_stale_minutes": bronze_result.get("stale_minutes"),
            "silver_stale_minutes": silver_result.get("stale_minutes"),
            "ingestor_running": ingestor_result.get("running"),
            "ingestor_messages_processed": ingestor_result.get("messages_processed"),
            "dead_letter_count": dl_pipeline.get("actual_value"),
            "silver_rows_last_hour": silver_rows.get("actual_value"),
        }

        from dag_utils.callbacks import _emit_structured_log
        _emit_structured_log(
            severity="INFO",
            event="platform_health_check_passed",
            payload=summary,
        )

        return summary

    # ── Task: Health failure notifier ─────────────────────────────────────

    @task(
        task_id="notify_health_failure",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
    )
    def notify_health_failure(**context) -> None:
        """Fires on any health check task failure with structured CRITICAL log."""
        dag_run = context["dag_run"]
        failed_tasks = [
            ti.task_id
            for ti in dag_run.get_task_instances()
            if ti.state == "failed"
        ]

        from dag_utils.callbacks import _emit_structured_log
        _emit_structured_log(
            severity="CRITICAL",
            event="platform_health_check_failed",
            payload={
                "dag_id": dag_run.dag_id,
                "run_id": dag_run.run_id,
                "failed_checks": failed_tasks,
                "action": "investigate_pipeline_components_immediately",
            },
        )

    # ── Wire task dependencies ─────────────────────────────────────────────

    bronze_check = check_bronze_source_freshness()
    silver_check = check_silver_source_freshness()

    # Freshness checks run in parallel
    freshness_group = [bronze_check, silver_check]

    # Dead letter checks run after freshness (in parallel with each other)
    freshness_group >> check_dead_letter_pipeline
    freshness_group >> check_dead_letter_ingestor

    dead_letter_group = [check_dead_letter_pipeline, check_dead_letter_ingestor]

    # Ingestor health after dead letter checks
    ingestor_check = check_ingestor_health_endpoint()
    dead_letter_group >> ingestor_check

    # Silver anomaly after ingestor check
    ingestor_check >> check_silver_row_count

    # Summary after all checks
    check_silver_row_count >> log_health_summary()

    # Failure notifier — independent branch, fires on any failure
    [
        bronze_check,
        silver_check,
        check_dead_letter_pipeline,
        check_dead_letter_ingestor,
        ingestor_check,
        check_silver_row_count,
    ] >> notify_health_failure()


pipeline_health_monitor()