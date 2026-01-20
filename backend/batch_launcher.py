# batch_launcher.py
"""
Launches Cloud Batch jobs for heavy data processing
"""
from google.cloud import batch_v1
from google.protobuf.duration_pb2 import Duration
import uuid
import os

PROJECT_ID = os.environ.get("GCP_PROJECT", "betfair-data-explorer")
REGION = "us-central1"
BATCH_PARENT = f"projects/{PROJECT_ID}/locations/{REGION}"

CONTAINER_IMAGE = "gcr.io/betfair-data-explorer/chimera-worker:latest"


def submit_batch_job(manifest_gcs_path: str) -> str:
    """
    Submit a Cloud Batch job to process NDJSON data.

    Args:
        manifest_gcs_path: GCS path to the job manifest JSON

    Returns:
        The batch job ID
    """
    client = batch_v1.BatchServiceClient()

    job_id = f"chimera-{uuid.uuid4().hex[:12]}"

    runnable = batch_v1.Runnable(
        container=batch_v1.Runnable.Container(
            image_uri=CONTAINER_IMAGE,
            commands=[
                "python",
                "worker.py",
                "--manifest",
                manifest_gcs_path
            ]
        )
    )

    task = batch_v1.TaskSpec(
        runnables=[runnable],
        compute_resource=batch_v1.ComputeResource(
            cpu_milli=8000,      # 8 vCPU
            memory_mib=65536     # 64 GB RAM
        ),
        max_run_duration=Duration(seconds=7200)  # 2 hours
    )

    task_group = batch_v1.TaskGroup(
        task_spec=task,
        task_count=1
    )

    job = batch_v1.Job(
        task_groups=[task_group],
        allocation_policy=batch_v1.AllocationPolicy(
            instances=[
                batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
                    policy=batch_v1.AllocationPolicy.InstancePolicy(
                        machine_type="n2-highmem-8"
                    )
                )
            ]
        ),
        logs_policy=batch_v1.LogsPolicy(
            destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
        )
    )

    client.create_job(
        parent=BATCH_PARENT,
        job=job,
        job_id=job_id
    )

    return job_id


def get_job_status(job_id: str) -> dict:
    """
    Get the status of a Cloud Batch job.

    Args:
        job_id: The batch job ID

    Returns:
        Dict with status info
    """
    client = batch_v1.BatchServiceClient()
    job_name = f"{BATCH_PARENT}/jobs/{job_id}"

    try:
        job = client.get_job(name=job_name)

        # Map Batch job state to simple status
        state = job.status.state.name

        if state == "SUCCEEDED":
            return {"status": "complete", "batch_status": state}
        elif state in ("FAILED", "DELETION_IN_PROGRESS"):
            return {"status": "failed", "batch_status": state}
        else:
            return {"status": "running", "batch_status": state}

    except Exception as e:
        return {"status": "unknown", "error": str(e)}
