"""
CHIMERA Analysis API v2.0

FastAPI backend for dynamic Betfair data analysis.
Supports both direct analysis and Cloud Batch for large datasets.
"""

import os
import json
import uuid
import asyncio
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

from analyzer import analyze_records, load_records_from_gcs, stream_progress, stream_result, stream_error

# Initialize FastAPI
app = FastAPI(
    title="CHIMERA Analysis API",
    description="Dynamic field discovery for heterogeneous Betfair data",
    version="2.0.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job status (for small deployments)
# For production, use Firestore or Redis
job_status = {}


class AnalyzeRequest(BaseModel):
    bucket_url: str
    use_batch: bool = False  # Use Cloud Batch for large datasets


class JobStatus(BaseModel):
    job_id: str
    status: str
    message: Optional[str] = None
    progress: Optional[int] = None
    result: Optional[dict] = None


@app.get("/")
async def root():
    """Health check and API info."""
    return {
        "service": "CHIMERA Analysis API",
        "version": "2.0.0",
        "status": "healthy",
        "features": [
            "Dynamic field discovery",
            "No hardcoded assumptions",
            "Human-readable field names",
            "ML model suggestions",
            "BigQuery schema recommendations"
        ]
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.post("/analyze")
async def analyze(request: AnalyzeRequest, background_tasks: BackgroundTasks):
    """
    Start analysis of data at the given GCS URL.
    
    For small datasets, returns results directly.
    For large datasets, use use_batch=True to run on Cloud Batch.
    """
    job_id = f"job-{uuid.uuid4().hex[:12]}"
    
    # Initialize job status
    job_status[job_id] = {
        "status": "submitted",
        "message": "Analysis job submitted",
        "progress": 0,
        "bucket_url": request.bucket_url,
        "started_at": datetime.utcnow().isoformat(),
    }
    
    if request.use_batch:
        # Launch Cloud Batch job for large datasets
        try:
            from batch_launcher import launch_batch_job
            batch_job_id = launch_batch_job(job_id, request.bucket_url)
            job_status[job_id]["batch_job_id"] = batch_job_id
            job_status[job_id]["message"] = "Launched Cloud Batch job"
            
            return {
                "type": "submitted",
                "job_id": job_id,
                "batch_job_id": batch_job_id,
                "message": "Analysis job submitted to Cloud Batch",
                "status_url": f"/status/{job_id}"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to launch batch job: {str(e)}")
    else:
        # Run analysis directly (for smaller datasets)
        background_tasks.add_task(run_analysis_task, job_id, request.bucket_url)
        
        return {
            "type": "submitted",
            "job_id": job_id,
            "message": "Analysis started",
            "status_url": f"/status/{job_id}"
        }


async def run_analysis_task(job_id: str, bucket_url: str):
    """Background task to run analysis."""
    try:
        job_status[job_id]["status"] = "running"
        job_status[job_id]["message"] = "Loading data..."
        job_status[job_id]["progress"] = 10
        
        # Run analysis
        result = analyze_records(bucket_url=bucket_url)
        
        job_status[job_id]["status"] = "complete"
        job_status[job_id]["message"] = "Analysis complete"
        job_status[job_id]["progress"] = 100
        job_status[job_id]["result"] = result
        job_status[job_id]["completed_at"] = datetime.utcnow().isoformat()
        
    except Exception as e:
        job_status[job_id]["status"] = "error"
        job_status[job_id]["message"] = str(e)
        job_status[job_id]["error"] = str(e)


@app.get("/status/{job_id}")
async def get_status(job_id: str):
    """Get status of an analysis job."""
    if job_id not in job_status:
        # Try to load from GCS (for batch jobs)
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket("betfair-chimera-results")
            blob = bucket.blob(f"{job_id}/analysis_result.json")
            
            if blob.exists():
                result = json.loads(blob.download_as_text())
                return {
                    "status": "complete",
                    "job_id": job_id,
                    "result": result,
                }
        except:
            pass
        
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    
    status = job_status[job_id]
    
    response = {
        "job_id": job_id,
        "status": status["status"],
        "message": status.get("message"),
        "progress": status.get("progress"),
    }
    
    if status["status"] == "complete":
        response["result"] = status.get("result")
    
    if status["status"] == "error":
        response["error"] = status.get("error")
    
    return response


@app.post("/analyze/stream")
async def analyze_stream(request: AnalyzeRequest):
    """
    Stream analysis results as they're computed.
    Returns Server-Sent Events with progress updates.
    """
    async def generate():
        try:
            yield f"data: {stream_progress('Starting analysis...', 0)}\n\n"
            
            yield f"data: {stream_progress('Loading data from GCS...', 10)}\n\n"
            
            # Load records
            records = load_records_from_gcs(request.bucket_url)
            
            yield f"data: {stream_progress(f'Loaded {len(records):,} records', 30)}\n\n"
            
            yield f"data: {stream_progress('Discovering fields...', 40)}\n\n"
            
            # Run analysis
            result = analyze_records(records=records)
            
            yield f"data: {stream_progress('Analysis complete!', 100)}\n\n"
            
            yield f"data: {stream_result(result)}\n\n"
            
        except Exception as e:
            yield f"data: {stream_error(str(e))}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.get("/field-dictionary")
async def get_field_dictionary():
    """Get the complete Betfair field dictionary."""
    from betfair_dictionary import get_all_known_fields, FIELD_CATEGORIES
    
    return {
        "fields": get_all_known_fields(),
        "categories": FIELD_CATEGORIES,
    }


@app.get("/field/{field_name}")
async def get_field_info(field_name: str, context: Optional[str] = None):
    """Get information about a specific field."""
    from betfair_dictionary import get_field_info as get_info
    
    info = get_info(field_name, context)
    return {
        "field": field_name,
        "context": context,
        **info
    }


# Run with: uvicorn main:app --host 0.0.0.0 --port 8080
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
