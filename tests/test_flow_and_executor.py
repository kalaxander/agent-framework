import pytest

from agentframework import Flow, Task, FlowValidationError
from agentframework.core.flow import RetryPolicy
from agentframework.core.executor import SyncExecutor


def test_topological_order_simple_chain():
    flow = Flow(name="chain")
    flow.add_task(Task(name="a", fn=lambda ctx: 1))
    flow.add_task(Task(name="b", fn=lambda ctx: 2, depends_on=["a"]))
    flow.add_task(Task(name="c", fn=lambda ctx: 3, depends_on=["b"]))
    assert flow.topological_order() == ["a", "b", "c"]


def test_validate_raises_on_missing_dependency():
    flow = Flow(name="broken")
    flow.add_task(Task(name="a", fn=lambda ctx: 1, depends_on=["ghost"]))
    with pytest.raises(FlowValidationError):
        flow.validate()


def test_validate_raises_on_cycle():
    flow = Flow(name="cyclic")
    flow.add_task(Task(name="a", fn=lambda ctx: 1, depends_on=["b"]))
    flow.add_task(Task(name="b", fn=lambda ctx: 2, depends_on=["a"]))
    with pytest.raises(FlowValidationError):
        flow.validate()


def test_executor_runs_flow_and_passes_context():
    flow = Flow(name="demo")
    flow.add_task(Task(name="classify", fn=lambda ctx: {"category": "billing"}))
    flow.add_task(
        Task(
            name="draft",
            fn=lambda ctx: f"Re: {ctx['classify']['category']}",
            depends_on=["classify"],
        )
    )
    result = SyncExecutor().run(flow, inputs={"ticket_id": 42})
    assert result["classify"] == {"category": "billing"}
    assert result["draft"] == "Re: billing"
    assert result["__inputs__"] == {"ticket_id": 42}


def test_executor_retries_then_succeeds():
    attempts = {"count": 0}

    def flaky(ctx):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("transient failure")
        return "ok"

    flow = Flow(name="retry-demo")
    flow.add_task(
        Task(
            name="flaky_task",
            fn=flaky,
            retry_policy=RetryPolicy(max_attempts=5, backoff_seconds=0.01, backoff_multiplier=1.0),
        )
    )
    result = SyncExecutor().run(flow, inputs={})
    assert result["flaky_task"] == "ok"
    assert attempts["count"] == 3


def test_executor_exhausts_retries_and_raises():
    def always_fails(ctx):
        raise RuntimeError("nope")

    flow = Flow(name="always-fails")
    flow.add_task(
        Task(
            name="t",
            fn=always_fails,
            retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0.01, backoff_multiplier=1.0),
        )
    )
    with pytest.raises(RuntimeError):
        SyncExecutor().run(flow, inputs={})


def test_audit_log_records_each_attempt():
    executor = SyncExecutor()
    flow = Flow(name="audited")
    flow.add_task(Task(name="a", fn=lambda ctx: "done"))
    executor.run(flow, inputs={})
    events = executor.audit_log.for_flow("audited")
    assert len(events) == 1
    assert events[0].event == "task_succeeded"
