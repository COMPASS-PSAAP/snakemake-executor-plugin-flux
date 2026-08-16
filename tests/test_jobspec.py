"""Unit tests for the resource translation.

These do not require flux (or a flux instance) and therefore run anywhere.
"""

from types import SimpleNamespace
from typing import Optional

import pytest
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_flux import ExecutorSettings, executor as executor_module
from snakemake_executor_plugin_flux.executor import FluxExecutor


class FakeJobspec:
    """Stand-in for flux.job.JobspecV1, recording what it was asked for."""

    def __init__(self, kind, **kwargs):
        self.kind = kind
        self.__dict__.update(kwargs)

    @classmethod
    def from_command(cls, **kwargs):
        return cls("command", **kwargs)

    @classmethod
    def from_batch_command(cls, **kwargs):
        return cls("batch", **kwargs)


@pytest.fixture(autouse=True)
def fake_jobspec(monkeypatch):
    monkeypatch.setattr(executor_module, "JobspecV1", FakeJobspec)


def make_executor(settings: Optional[ExecutorSettings] = None) -> FluxExecutor:
    # bypass __init__, which requires a live flux instance
    executor = object.__new__(FluxExecutor)
    executor.logger = SimpleNamespace(debug=lambda *args, **kwargs: None)
    executor.flux_settings = settings if settings is not None else ExecutorSettings()
    executor.format_job_exec = lambda job: "snakemake --some args"
    executor.run_namespace = "testrun"
    return executor


def make_job(threads: int = 1, **resources) -> SimpleNamespace:
    return SimpleNamespace(
        name="testjob", jobid=1, threads=threads, resources=resources
    )


def test_serial_job_is_a_plain_single_task():
    spec = make_executor()._jobspec(make_job(threads=4))
    assert spec.kind == "command"
    assert spec.num_tasks == 1
    assert spec.cores_per_task == 4  # falls back to the job's threads
    assert spec.gpus_per_task is None
    assert spec.command == ["snakemake", "--some", "args"]


def test_parallel_job_is_a_batch_allocation():
    job = make_job(threads=1, nodes=2, tasks=8, cpus_per_task=6, gpus_per_task=1)
    spec = make_executor()._jobspec(job)
    # a multi-task jobspec would run one Snakemake per task
    assert spec.kind == "batch"
    assert (spec.num_nodes, spec.num_slots) == (2, 8)
    assert (spec.cores_per_slot, spec.gpus_per_slot) == (6, 1)
    assert spec.script.startswith("#!/bin/sh\n")
    assert "snakemake --some args" in spec.script


def test_multiple_tasks_without_nodes_is_also_a_batch_allocation():
    spec = make_executor()._jobspec(make_job(tasks=4))
    assert spec.kind == "batch"
    assert spec.num_slots == 4
    assert spec.num_nodes is None


def test_cpus_per_task_overrides_threads():
    spec = make_executor()._jobspec(make_job(threads=2, cpus_per_task=8))
    assert spec.cores_per_task == 8


def test_exclusive_requires_nodes():
    executor = make_executor(ExecutorSettings(exclusive=True))
    with pytest.raises(WorkflowError, match="does not define a 'nodes' resource"):
        executor._jobspec(make_job())

    spec = executor._jobspec(make_job(nodes=2, tasks=2))
    assert spec.exclusive is True


def test_fewer_tasks_than_nodes_is_rejected():
    with pytest.raises(WorkflowError, match="at least one task per node"):
        make_executor()._jobspec(make_job(nodes=4, tasks=2))


@pytest.mark.parametrize(
    "resources,expected",
    [
        ({}, 0),
        ({"runtime": 0}, 0),
        ({"runtime": 30}, 1800.0),
        ({"runtime": 1440}, 86400.0),
    ],
)
def test_runtime_is_converted_from_minutes_to_seconds(resources, expected):
    assert FluxExecutor._duration(resources) == expected
