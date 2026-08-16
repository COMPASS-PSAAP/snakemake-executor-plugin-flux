from dataclasses import dataclass, field
from typing import Optional

from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)

from .executor import FluxExecutor as Executor  # noqa


# Optional:
# Define additional settings for the executor.
# They will occur in the Snakemake CLI as --flux-<param-name>.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    queue: Optional[str] = field(
        default=None,
        metadata={
            "help": "Flux queue to submit jobs to (e.g. pbatch). Only meaningful "
            "when submitting to an instance that defines queues.",
            "env_var": False,
            "required": False,
        },
    )
    bank: Optional[str] = field(
        default=None,
        metadata={
            "help": "Accounting bank to charge the jobs to. Required by some "
            "sites when submitting to a system instance.",
            "env_var": False,
            "required": False,
        },
    )
    exclusive: bool = field(
        default=False,
        metadata={
            "help": "Request whole nodes exclusively. Requires the jobs to define "
            "a 'nodes' resource.",
            "env_var": False,
            "required": False,
        },
    )


# Required:
# Common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    job_deploy_sources=True,
    non_local_exec=True,
    # Flux can have a shared filesystem if run in an HPC context, or not if cloud
    # so we cannot set it one way or the other.
    implies_no_shared_fs=False,
)
