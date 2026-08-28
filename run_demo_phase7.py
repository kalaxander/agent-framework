"""Run Phase 7 end to end: compile a Flow to Airflow DAG source, verify it's structurally
correct and syntactically valid Python, then simulate an Airflow run (via a stub XCom) and
confirm the results match SyncExecutor running the same flow directly. No external dependencies
— this verifies everything Airflow-adapter-side that doesn't require an actual JVM/Airflow
install (see integrations/camel/README.md for why the Camel piece isn't executed the same way).
Run with:
    python3 run_demo_phase7.py
"""
from __future__ import annotations

import sys

sys.path.insert(0, "sdk")
sys.path.insert(0, ".")

from agentframework.core.executor import SyncExecutor
from agentframework.integrations.airflow_adapter import compile_to_airflow_dag, run_task_for_airflow
from examples.shared_flows import simple_pipeline_flow


class StubTaskInstance:
    """Minimal stand-in for Airflow's TaskInstance — just enough of the `.xcom_pull()` surface
    that run_task_for_airflow actually uses."""

    def __init__(self, xcom: dict):
        self.xcom = xcom

    def xcom_pull(self, task_ids: str):
        return self.xcom.get(task_ids)


def demo_compile():
    print("=== Phase 7: compile a Flow to Airflow DAG source ===")
    src = compile_to_airflow_dag("examples.shared_flows", "simple_pipeline_flow",
                                  dag_id="simple-pipeline-dag")
    print(src)

    compile(src, "<generated_dag>", "exec")  # raises SyntaxError if malformed
    print("OK: generated DAG source is valid Python (checked via compile())")

    for task_name in ["classify", "fetch_docs", "draft"]:
        assert f"{task_name} = PythonOperator(" in src, f"missing PythonOperator for {task_name}"
    assert "classify >> fetch_docs" in src
    assert "fetch_docs >> draft" in src
    print("OK: task ids and dependency edges (>>) present and correctly ordered")
    print()


def demo_execution_matches_sync_executor():
    print("=== Phase 7: simulated Airflow run matches SyncExecutor's direct run ===")
    flow = simple_pipeline_flow()
    order = flow.topological_order()

    xcom: dict = {}
    ti = StubTaskInstance(xcom)
    for task_name in order:
        result = run_task_for_airflow(
            flow_module="examples.shared_flows",
            flow_factory="simple_pipeline_flow",
            task_name=task_name,
            ti=ti,
        )
        xcom[task_name] = result  # Airflow auto-pushes a PythonOperator's return under this key

    print(f"Airflow-simulated results: {xcom}")

    direct_result = SyncExecutor().run(simple_pipeline_flow(), inputs={})
    direct_subset = {k: direct_result[k] for k in order}
    print(f"SyncExecutor direct result: {direct_subset}")

    assert xcom == direct_subset, "MISMATCH between Airflow-compiled execution and SyncExecutor"
    print("OK: identical results — the compiled DAG's task logic is equivalent to running the "
          "Flow directly")
    print()


if __name__ == "__main__":
    demo_compile()
    demo_execution_matches_sync_executor()
    print("Phase 7 Airflow adapter verified end to end. See integrations/camel/README.md for "
          "the Camel route (documented, not executed — see docs/Memory.md).")
