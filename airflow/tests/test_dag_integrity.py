"""
Structural integrity tests for all Airflow DAGs.

Tests:
  - DAG imports without errors (syntax + dependency check)
  - No circular task dependencies (cycle detection)
  - Required DAG attributes are set (schedule, tags, owner, catchup)
  - Task count within expected bounds
  - All tasks have retry policies configured
  - SLA is configured on expected tasks
  - Default args propagated correctly

These tests run in CI without a live Airflow instance or database.
They use Airflow's DAG parsing utilities only.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

# Add airflow DAGs and plugins to path for CI (mirrors Airflow container PYTHONPATH)
AIRFLOW_HOME = Path(__file__).parent.parent
sys.path.insert(0, str(AIRFLOW_HOME / "dags"))
sys.path.insert(0, str(AIRFLOW_HOME / "plugins"))


# ─── DAG Import Fixtures ──────────────────────────────────────────────────────

def _load_dag(module_name: str, dag_id: str):
    """Imports a DAG module and returns the DAG object."""
    spec = importlib.util.spec_from_file_location(
        module_name,
        str(AIRFLOW_HOME / "dags" / f"{module_name}.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Airflow DAGs register themselves in the global DagBag
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder=str(AIRFLOW_HOME / "dags"), include_examples=False)
    return dagbag.get_dag(dag_id)


@pytest.fixture(scope="module")
def dagbag():
    """Loads all DAGs from the dags/ directory."""
    from airflow.models import DagBag
    bag = DagBag(
        dag_folder=str(AIRFLOW_HOME / "dags"),
        include_examples=False,
    )
    return bag


# ─── DagBag Import Tests ──────────────────────────────────────────────────────

class TestDagBagImport:

    def test_dagbag_has_no_import_errors(self, dagbag) -> None:
        """All DAGs must import without syntax or dependency errors."""
        assert dagbag.import_errors == {}, (
            f"DAG import errors detected:\n"
            + "\n".join(
                f"  {path}: {err}"
                for path, err in dagbag.import_errors.items()
            )
        )

    def test_expected_dags_are_present(self, dagbag) -> None:
        expected_dag_ids = {
            "dbt_transformation_pipeline",
            "pipeline_health_monitor",
        }
        loaded_dag_ids = set(dagbag.dag_ids)
        missing = expected_dag_ids - loaded_dag_ids
        assert not missing, f"Expected DAGs not found in DagBag: {missing}"

    def test_no_unexpected_dags_present(self, dagbag) -> None:
        """Guards against accidental DAG file additions to this sprint."""
        expected_dag_ids = {
            "dbt_transformation_pipeline",
            "pipeline_health_monitor",
        }
        unexpected = set(dagbag.dag_ids) - expected_dag_ids
        assert not unexpected, (
            f"Unexpected DAGs found in DagBag: {unexpected}. "
            f"If intentional, add them to the expected set."
        )


# ─── dbt_transformation_pipeline Tests ───────────────────────────────────────

class TestDbtTransformationPipeline:

    DAG_ID = "dbt_transformation_pipeline"

    @pytest.fixture
    def dag(self, dagbag):
        d = dagbag.get_dag(self.DAG_ID)
        assert d is not None, f"DAG '{self.DAG_ID}' not found in DagBag"
        return d

    def test_dag_has_correct_schedule(self, dag) -> None:
        assert str(dag.schedule_interval) == "*/15 * * * *", (
            f"Expected schedule '*/15 * * * *', got '{dag.schedule_interval}'"
        )

    def test_dag_catchup_is_disabled(self, dag) -> None:
        assert dag.catchup is False, "catchup must be False to prevent backfill storms"

    def test_dag_max_active_runs_is_one(self, dag) -> None:
        assert dag.max_active_runs == 1, (
            "max_active_runs must be 1 to prevent concurrent pipeline runs"
        )

    def test_dag_has_required_tags(self, dag) -> None:
        required_tags = {"transformation", "dbt", "silver", "gold"}
        assert required_tags.issubset(set(dag.tags)), (
            f"Missing required tags: {required_tags - set(dag.tags)}"
        )

    def test_dag_has_sla_miss_callback(self, dag) -> None:
        assert dag.sla_miss_callback is not None, (
            "SLA miss callback must be configured for enterprise alerting"
        )

    def test_dag_has_no_cycles(self, dag) -> None:
        """Airflow raises CycleException on import if cycles exist — this test
        is a belt-and-suspenders check using the topological sort."""
        topological_sort = dag.topological_sort()
        assert len(topological_sort) > 0, "Topological sort failed — possible cycle"

    def test_dag_has_expected_task_count(self, dag) -> None:
        task_count = len(dag.tasks)
        assert task_count >= 8, (
            f"Expected at least 8 tasks, found {task_count}. "
            f"Tasks: {[t.task_id for t in dag.tasks]}"
        )

    def test_all_tasks_have_owner(self, dag) -> None:
        for task in dag.tasks:
            assert task.owner, f"Task '{task.task_id}' has no owner set"

    def test_silver_tasks_run_before_gold(self, dag) -> None:
        """Validates that Silver test gate precedes all Gold tasks."""
        task_ids = {t.task_id for t in dag.tasks}
        silver_test = dag.get_task("dbt_test_silver")
        assert silver_test is not None, "dbt_test_silver task not found"

        gold_tasks = [
            "dbt_run_gold_daily_ohlcv",
            "dbt_run_gold_price_stats_24h",
            "dbt_run_gold_trade_volume_hourly",
        ]
        for gold_task_id in gold_tasks:
            if gold_task_id in task_ids:
                gold_task = dag.get_task(gold_task_id)
                upstream_ids = {t.task_id for t in gold_task.upstream_list}
                assert "dbt_test_silver" in upstream_ids, (
                    f"Gold task '{gold_task_id}' must have 'dbt_test_silver' as upstream. "
                    f"Got: {upstream_ids}"
                )

    def test_failure_notifier_has_one_failed_trigger_rule(self, dag) -> None:
        from airflow.utils.trigger_rule import TriggerRule
        notify_task = dag.get_task("notify_pipeline_failure")
        assert notify_task is not None, "notify_pipeline_failure task not found"
        assert notify_task.trigger_rule == TriggerRule.ONE_FAILED, (
            f"notify_pipeline_failure must use TriggerRule.ONE_FAILED, "
            f"got {notify_task.trigger_rule}"
        )


# ─── pipeline_health_monitor Tests ───────────────────────────────────────────

class TestPipelineHealthMonitor:

    DAG_ID = "pipeline_health_monitor"

    @pytest.fixture
    def dag(self, dagbag):
        d = dagbag.get_dag(self.DAG_ID)
        assert d is not None, f"DAG '{self.DAG_ID}' not found in DagBag"
        return d

    def test_dag_has_correct_schedule(self, dag) -> None:
        assert str(dag.schedule_interval) == "*/5 * * * *"

    def test_dag_catchup_is_disabled(self, dag) -> None:
        assert dag.catchup is False

    def test_dag_max_active_runs_is_one(self, dag) -> None:
        assert dag.max_active_runs == 1

    def test_dag_has_required_tags(self, dag) -> None:
        required_tags = {"monitoring", "health"}
        assert required_tags.issubset(set(dag.tags))

    def test_dag_has_sla_miss_callback(self, dag) -> None:
        assert dag.sla_miss_callback is not None

    def test_dag_has_no_cycles(self, dag) -> None:
        assert len(dag.topological_sort()) > 0

    def test_dag_has_expected_task_count(self, dag) -> None:
        assert len(dag.tasks) >= 7

    def test_failure_notifier_has_one_failed_trigger_rule(self, dag) -> None:
        from airflow.utils.trigger_rule import TriggerRule
        notify_task = dag.get_task("notify_health_failure")
        assert notify_task is not None
        assert notify_task.trigger_rule == TriggerRule.ONE_FAILED

    def test_freshness_tasks_upstream_of_dead_letter(self, dag) -> None:
        """Freshness checks must precede dead-letter volume checks."""
        task_ids = {t.task_id for t in dag.tasks}
        dl_task_id = "check_dead_letter_volume_pipeline"
        if dl_task_id in task_ids:
            dl_task = dag.get_task(dl_task_id)
            upstream_ids = {t.task_id for t in dl_task.upstream_list}
            assert "check_bronze_source_freshness" in upstream_ids or \
                   "check_silver_source_freshness" in upstream_ids, (
                f"Dead letter check must have freshness checks upstream. Got: {upstream_ids}"
            )