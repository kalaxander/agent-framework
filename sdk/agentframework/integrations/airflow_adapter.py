"""Production Airflow adapter (docs/Architecture.md > Orchestrator: "optional Airflow adapter
for batch-style flows"). Two halves:

1. `compile_to_airflow_dag()` — generates the *source code* of an Airflow DAG file from a Flow.
   Runs in our process at compile time (needs to import the flow to read its task graph), but
   the generated file only imports `airflow` + this module — it has no dependency on the live
   Flow object, because Airflow re-imports DAG files in its own scheduler/worker processes where
   our in-memory objects don't exist. That's why the Flow is referenced by *import path*
   (`flow_module`/`flow_factory`) rather than passed directly, and why examples/shared_flows.py
   exists as an importable module instead of a closure in a demo script.

2. `run_task_for_airflow()` — what each generated PythonOperator actually calls at DAG-run time.
   Rebuilds the Flow, pulls upstream task results out of Airflow's XCom (Airflow's built-in
   cross-task data-passing mechanism) into the same `context` dict shape every other executor
   uses, and runs the one task through `SyncExecutor._run_task_with_retry` — so retries,
   guardrails, and the audit log all behave identically to running the flow directly.

Requires the `server`-adjacent `airflow` package at actual DAG-run time (not to *generate* the
DAG source, which is pure string building and needs nothing extra) — this sandbox has no
network access to install `apache-airflow`, so `run_task_for_airflow` is verified here by
calling it directly with a stub XCom object (see docs/Memory.md and run_demo_phase7.py) rather
than through a real Airflow scheduler.
"""
from __future__ import annotations

import importlib
from typing import Any, Optional

from agentframework.core.executor import SyncExecutor
from agentframework.tools.registry import ToolRegistry


def compile_to_airflow_dag(
    flow_module: str,
    flow_factory: str,
    dag_id: Optional[str] = None,
    schedule: Optional[str] = None,
    tool_registry_factory: Optional[str] = None,
) -> str:
    """Generate Airflow DAG file source for the Flow returned by `flow_factory()` in
    `flow_module` (both must be importable — e.g. "examples.shared_flows", "simple_pipeline_flow").
    `tool_registry_factory`, if the flow uses named tools, is an importable "module:function"
    string for a zero-arg factory returning a configured ToolRegistry (same import-path
    constraint as the flow itself, for the same reason).
    """
    module = importlib.import_module(flow_module)
    flow = getattr(module, flow_factory)()
    flow.validate()
    order = flow.topological_order()
    dag_id = dag_id or flow.name

    lines = [
        "from datetime import datetime",
        "from airflow import DAG",
        "from airflow.operators.python import PythonOperator",
        "from agentframework.integrations.airflow_adapter import run_task_for_airflow",
        "",
        f"with DAG(",
        f"    dag_id={dag_id!r},",
        f"    schedule={schedule!r},",
        f"    start_date=datetime(2024, 1, 1),",
        f"    catchup=False,",
        f") as dag:",
    ]
    for name in order:
        lines.append(
            f"    {name} = PythonOperator("
            f"task_id={name!r}, "
            f"python_callable=run_task_for_airflow, "
            f"op_kwargs={{"
            f"'flow_module': {flow_module!r}, "
            f"'flow_factory': {flow_factory!r}, "
            f"'task_name': {name!r}, "
            f"'tool_registry_factory': {tool_registry_factory!r}"
            f"}})"
        )
    for name in order:
        for dep in flow.tasks[name].depends_on:
            lines.append(f"    {dep} >> {name}")

    return "\n".join(lines) + "\n"


def run_task_for_airflow(
    flow_module: str,
    flow_factory: str,
    task_name: str,
    ti: Any,
    tool_registry_factory: Optional[str] = None,
    dag_run: Any = None,
    **_kwargs: Any,
) -> Any:
    """Entry point every generated PythonOperator calls. `ti` (Airflow's TaskInstance, or a
    stub with the same `.xcom_pull(task_ids=...)` shape for testing) supplies upstream results;
    `dag_run.conf`, if present, becomes the flow's top-level inputs. Returns the task's result —
    Airflow auto-pushes a PythonOperator's return value to XCom under "return_value", so
    downstream tasks' `ti.xcom_pull(task_ids=<this task>)` picks it up automatically.
    """
    module = importlib.import_module(flow_module)
    flow = getattr(module, flow_factory)()
    task = flow.tasks[task_name]

    inputs = getattr(dag_run, "conf", None) or {}
    context: dict[str, Any] = {"__inputs__": inputs}
    for dep in task.depends_on:
        context[dep] = ti.xcom_pull(task_ids=dep)

    tool_registry: Optional[ToolRegistry] = None
    if tool_registry_factory:
        registry_module_name, registry_fn_name = tool_registry_factory.rsplit(":", 1)
        registry_module = importlib.import_module(registry_module_name)
        tool_registry = getattr(registry_module, registry_fn_name)()

    executor = SyncExecutor(tool_registry=tool_registry)
    return executor._run_task_with_retry(flow, task, context)
