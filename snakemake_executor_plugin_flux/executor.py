import os
import shlex

from typing import List, Generator
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.workflow import WorkflowExecutorInterface
from snakemake_interface_executor_plugins.logging import LoggerExecutorInterface
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError  # noqa

# Just import flux python bindings once
try:
    import flux
    import flux.job
    from flux.job import JobspecV1
except ImportError:
    flux = None
    JobspecV1 = None


class FluxExecutor(RemoteExecutor):
    def __init__(
        self,
        workflow: WorkflowExecutorInterface,
        logger: LoggerExecutorInterface,
    ):
        super().__init__(workflow, logger)

        # Attach variables for easy access
        self.workdir = os.path.realpath(os.path.dirname(self.workflow.persistence.path))
        self.envvars = list(self.workflow.envvars) or []

        # access executor specific settings
        self.flux_settings = self.workflow.executor_settings

        # Quit early if we can't access the flux api
        if not flux:
            raise WorkflowError(
                "Cannot import flux. Are Python bindings available? They are "
                "provided by your flux-core installation rather than by this "
                "plugin, so they have to be importable from the interpreter "
                "that runs Snakemake."
            )
        self._fexecutor = flux.job.FluxExecutor()

    def get_envvar_declarations(self):
        """
        Temporary workaround until:
        https://github.com/snakemake/snakemake-interface-executor-plugins/pull/31
        is able to be merged.
        """
        return " ".join(
            f"{var}={repr(os.environ[var])}"
            for var in self.workflow.remote_execution_settings.envvars or {}
        )

    def _jobspec(self, job: JobExecutorInterface):
        """Translate the job's resources into a flux jobspec.

        Recognized resources (all optional, and named after their equivalents
        in the other cluster executors):

        nodes           number of nodes to allocate
        tasks           number of tasks (e.g. MPI ranks) to launch
        cpus_per_task   cores per task, defaults to the job's threads
        gpus_per_task   GPUs per task
        runtime         walltime limit in minutes

        flux does not support mem_mb and disk_mb.

        Jobs that request neither nodes nor multiple tasks are submitted as a
        plain single task. Everything else is submitted as a batch job, i.e. as
        a nested flux instance that owns the requested allocation and runs the
        Snakemake job exactly once inside it. The rule's own command is then
        responsible for launching the parallel program into that allocation
        (e.g. with 'flux run -n {resources.tasks}'), analogous to using srun
        within an sbatch script.
        """
        command = self.format_job_exec(job)
        self.logger.debug(command)

        resources = job.resources

        # Must have at least one task and one core per task, and integer values
        num_tasks = max(1, int(resources.get("tasks") or 1))
        cores_per_task = max(1, int(resources.get("cpus_per_task") or job.threads))

        num_nodes = resources.get("nodes")
        num_nodes = int(num_nodes) if num_nodes else None

        gpus_per_task = resources.get("gpus_per_task")
        gpus_per_task = int(gpus_per_task) if gpus_per_task else None

        exclusive = bool(self.flux_settings.exclusive)

        if num_nodes is not None and num_tasks < num_nodes:
            raise WorkflowError(
                f"Job {job.name} requests {num_nodes} nodes but only "
                f"{num_tasks} tasks. Flux requires at least one task per node, "
                "please increase the 'tasks' resource."
            )
        if exclusive and num_nodes is None:
            raise WorkflowError(
                "Exclusive node allocation was requested (--flux-exclusive), but "
                f"job {job.name} does not define a 'nodes' resource."
            )

        if num_nodes is None and num_tasks == 1:
            # A plain job: the command is the task.
            return JobspecV1.from_command(
                command=shlex.split(command),
                num_tasks=1,
                cores_per_task=cores_per_task,
                gpus_per_task=gpus_per_task,
            )

        # A parallel job: allocate the requested resources as a nested instance
        # and run the command once inside it. Submitting this as a multi-task
        # job instead would launch one copy of Snakemake per task.
        return JobspecV1.from_batch_command(
            script=f"#!/bin/sh\n{command}\n",
            jobname=self.get_jobname(job),
            num_slots=num_tasks,
            cores_per_slot=cores_per_task,
            gpus_per_slot=gpus_per_task,
            num_nodes=num_nodes,
            exclusive=exclusive,
        )

    @staticmethod
    def _duration(resources) -> float:
        """Snakemake's runtime resource is in minutes, flux durations in seconds.

        A duration of zero (the default) means unlimited.
        """
        runtime = resources.get("runtime")
        return float(runtime) * 60 if runtime else 0

    def run_job(self, job: JobExecutorInterface):
        flux_logfile = job.logfile_suggestion(os.path.join(".snakemake", "flux_logs"))
        os.makedirs(os.path.dirname(flux_logfile), exist_ok=True)

        # Generate the flux job
        fluxjob = self._jobspec(job)

        fluxjob.duration = self._duration(job.resources)

        fluxjob.stderr = flux_logfile

        # Ensure the cwd is the snakemake working directory
        fluxjob.cwd = self.workdir
        fluxjob.environment = dict(os.environ)

        if self.flux_settings.queue:
            fluxjob.queue = self.flux_settings.queue
        if self.flux_settings.bank:
            fluxjob.setattr("attributes.system.bank", self.flux_settings.bank)

        flux_future = self._fexecutor.submit(fluxjob)

        # Save aux metadata
        aux = {"flux_future": flux_future, "flux_logfile": flux_logfile}

        # Record job info
        jobid = str(flux_future.jobid())
        self.report_job_submission(SubmittedJobInfo(job, external_jobid=jobid, aux=aux))

    def get_snakefile(self):
        assert os.path.exists(self.workflow.main_snakefile)
        return self.workflow.main_snakefile

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        # Loop through active jobs and act on status
        for j in active_jobs:
            jobid = j.external_jobid
            self.logger.debug(f"Checking status for job {jobid}")
            flux_future = j.aux["flux_future"]

            # Aux logs are consistently here
            aux_logs = [j.aux["flux_logfile"]]

            # Case 1: the job is done
            if flux_future.done():
                # The exit code can help us determine if the job was successful
                try:
                    exit_code = flux_future.result(0)
                except RuntimeError:
                    # job did not complete
                    msg = f"Flux job '{j.external_jobid}' failed. "
                    self.report_job_error(j, msg=msg, aux_logs=aux_logs)

                else:
                    # the job finished (but possibly with nonzero exit code)
                    if exit_code != 0:
                        msg = f"Flux job '{jobid}' finished with non-zero exit code. "
                        self.report_job_error(j, msg=msg, aux_logs=aux_logs)
                        continue

                    # Finished and success!
                    self.report_job_success(j)

            # Otherwise, we are still running
            else:
                yield j

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        """
        cancel all active jobs. This method is called when snakemake is interrupted.
        """
        for job in active_jobs:
            flux_future = job.aux["flux_future"]
            if not flux_future.done():
                # cancel() covers both jobs that are still pending in the
                # executor and jobs that are already running.
                flux_future.cancel()
        self.shutdown()
