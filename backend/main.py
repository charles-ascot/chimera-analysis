"""
FastAPI backend for NDJSON Analysis Console
Lightweight job dispatcher - heavy work goes to Cloud Batch
"""

import json
import uuid
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import storage

from batch_launcher import submit_batch_job, get_job_status

app = FastAPI(title="NDJSON Analysis API")

# Allow CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GCS buckets for job coordination
MANIFESTS_BUCKET = "betfair-chimera-manifests"
RESULTS_BUCKET = "betfair-chimera-results"


class AnalyzeRequest(BaseModel):
    bucket_url: str


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/analyze")
async def analyze(request: AnalyzeRequest):
    """
    Submit an analysis job to Cloud Batch.

    Returns immediately with a job ID that can be polled for status.
    """
    job_id = f"job-{uuid.uuid4().hex[:10]}"

    # Create manifest for the batch worker
    manifest = {
        "job_id": job_id,
        "bucket_url": request.bucket_url,
        "output_prefix": f"gs://{RESULTS_BUCKET}/{job_id}/"
    }

    # Upload manifest to GCS
    client = storage.Client()
    bucket = client.bucket(MANIFESTS_BUCKET)
    blob = bucket.blob(f"{job_id}.json")
    blob.upload_from_string(
        json.dumps(manifest),
        content_type="application/json"
    )

    manifest_path = f"gs://{MANIFESTS_BUCKET}/{job_id}.json"

    # Submit batch job
    try:
        batch_job_id = submit_batch_job(manifest_path)
    except Exception as e:
        return {
            "type": "error",
            "message": f"Failed to submit batch job: {str(e)}"
        }

    return {
        "type": "submitted",
        "job_id": job_id,
        "batch_job_id": batch_job_id,
        "message": "Job submitted to Cloud Batch"
    }


@app.get("/status/{job_id}")
async def get_status(job_id: str):
    """
    Check the status of an analysis job.

    Returns:
    - status: "running" | "complete" | "failed" | "unknown"
    - result: (if complete) the analysis results
    """
    client = storage.Client()

    # First check if results exist in GCS
    results_bucket = client.bucket(RESULTS_BUCKET)
    result_blob = results_bucket.blob(f"{job_id}/analysis_result.json")

    if result_blob.exists():
        # Job complete - return results
        try:
            result_data = json.loads(result_blob.download_as_text())
            return {
                "status": "complete",
                "result": result_data
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to read results: {str(e)}"
            }

    # Results don't exist yet - check batch job status
    # Try to find the batch job ID from manifest
    manifests_bucket = client.bucket(MANIFESTS_BUCKET)
    manifest_blob = manifests_bucket.blob(f"{job_id}.json")

    if not manifest_blob.exists():
        return {
            "status": "unknown",
            "message": "Job not found"
        }

    # Job exists but not complete - it's still running
    return {
        "status": "running",
        "message": "Job is being processed by Cloud Batch"
    }


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "NDJSON Analysis API",
        "version": "2.0.0",
        "architecture": "Cloud Run + Cloud Batch",
        "endpoints": {
            "POST /analyze": "Submit analysis job to Cloud Batch",
            "GET /status/{job_id}": "Check job status and get results",
            "GET /health": "Health check"
        }
    }
