"""
CHIMERA Analysis API v3.0

FastAPI backend for dynamic Betfair data analysis.
Supports both direct analysis and Cloud Batch for large datasets.
Includes session management for persistent storage and BigQuery integration.
"""

import os
import json
import uuid
import asyncio
from typing import Optional, List
from datetime import datetime

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, Response
from pydantic import BaseModel

from analyzer import analyze_records, load_records_from_gcs, stream_progress, stream_result, stream_error
from session_manager import (
    generate_session_id,
    save_session,
    get_session,
    list_sessions,
    delete_session,
    delete_sessions,
    export_session,
    export_sessions,
    get_session_for_bigquery,
)

# Initialize FastAPI
app = FastAPI(
    title="CHIMERA Analysis API",
    description="Dynamic field discovery for heterogeneous Betfair data with session management",
    version="3.0.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://analysis.thync.online",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
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
    create_session: bool = True  # Auto-create session with results


class DeleteSessionsRequest(BaseModel):
    session_ids: List[str]


class ExportSessionsRequest(BaseModel):
    session_ids: List[str]
    format: str = "json"  # 'json' or 'summary'


class JobStatus(BaseModel):
    job_id: str
    status: str
    message: Optional[str] = None
    progress: Optional[int] = None
    result: Optional[dict] = None
    session_id: Optional[str] = None


@app.get("/")
async def root():
    """Health check and API info."""
    return {
        "service": "CHIMERA Analysis API",
        "version": "3.0.0",
        "status": "healthy",
        "features": [
            "Dynamic field discovery",
            "No hardcoded assumptions",
            "Human-readable field names",
            "ML model suggestions",
            "BigQuery schema recommendations",
            "Session management",
            "Export & persistence"
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
    
    If create_session=True (default), a session will be created with the results.
    """
    job_id = f"job-{uuid.uuid4().hex[:12]}"
    session_id = generate_session_id() if request.create_session else None
    
    # Initialize job status
    job_status[job_id] = {
        "status": "submitted",
        "message": "Analysis job submitted",
        "progress": 0,
        "bucket_url": request.bucket_url,
        "started_at": datetime.utcnow().isoformat(),
        "create_session": request.create_session,
        "session_id": session_id,
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
                "session_id": session_id,
                "message": "Analysis job submitted to Cloud Batch",
                "status_url": f"/status/{job_id}"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to launch batch job: {str(e)}")
    else:
        # Run analysis directly (for smaller datasets)
        background_tasks.add_task(
            run_analysis_task, 
            job_id, 
            request.bucket_url, 
            request.create_session,
            session_id
        )
        
        return {
            "type": "submitted",
            "job_id": job_id,
            "session_id": session_id,
            "message": "Analysis started",
            "status_url": f"/status/{job_id}"
        }


async def run_analysis_task(job_id: str, bucket_url: str, create_session: bool = True, session_id: str = None):
    """Background task to run analysis and optionally create a session."""
    try:
        job_status[job_id]["status"] = "running"
        job_status[job_id]["message"] = "Loading data..."
        job_status[job_id]["progress"] = 10
        
        # Run analysis
        result = analyze_records(bucket_url=bucket_url)
        
        # Create session if requested
        if create_session and session_id:
            job_status[job_id]["message"] = "Saving session..."
            job_status[job_id]["progress"] = 90
            
            try:
                save_session(
                    session_id=session_id,
                    source_url=bucket_url,
                    analysis_result=result,
                    metadata={"job_id": job_id}
                )
                job_status[job_id]["session_saved"] = True
            except Exception as e:
                print(f"Failed to save session: {e}")
                job_status[job_id]["session_saved"] = False
                job_status[job_id]["session_error"] = str(e)
        
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
        "session_id": status.get("session_id"),
        "source_url": status.get("bucket_url"),
    }
    
    if status["status"] == "complete":
        response["result"] = status.get("result")
        response["session_saved"] = status.get("session_saved", False)
    
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


# =============================================================================
# SESSION MANAGEMENT ENDPOINTS
# =============================================================================

@app.get("/sessions")
async def get_sessions(limit: int = 50):
    """
    List all saved sessions.
    
    Returns session summaries (not full analysis data).
    """
    try:
        sessions = list_sessions(limit=limit)
        return {
            "sessions": sessions,
            "count": len(sessions),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list sessions: {str(e)}")


@app.get("/sessions/{session_id}")
async def get_session_by_id(session_id: str):
    """
    Get a specific session by ID.
    
    Returns full session data including analysis results.
    """
    session = get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    
    return session


@app.get("/sessions/{session_id}/summary")
async def get_session_summary(session_id: str):
    """
    Get a session summary (without full analysis data).
    
    Useful for quick lookups and listings.
    """
    session = get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    
    return {
        "session_id": session["session_id"],
        "source_url": session["source_url"],
        "created_at": session["created_at"],
        "status": session["status"],
        "summary": session.get("summary", {}),
        "schema_recommendations": session.get("schema_recommendations", {}),
        "ml_suggestions": session.get("ml_suggestions", []),
    }


@app.get("/sessions/{session_id}/bigquery")
async def get_session_for_bq(session_id: str):
    """
    Get session data formatted for BigQuery transport.
    
    Returns:
    - source_url: Where to fetch the data from
    - schema: BigQuery table schema recommendations
    - field info for mapping
    """
    bq_data = get_session_for_bigquery(session_id)
    
    if not bq_data:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    
    return bq_data


@app.delete("/sessions/{session_id}")
async def delete_session_by_id(session_id: str):
    """Delete a session by ID."""
    success = delete_session(session_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    
    return {"deleted": session_id, "success": True}


@app.post("/sessions/delete")
async def delete_multiple_sessions(request: DeleteSessionsRequest):
    """Delete multiple sessions at once."""
    result = delete_sessions(request.session_ids)
    return result


@app.get("/sessions/{session_id}/export")
async def export_session_by_id(session_id: str, format: str = "json"):
    """
    Export a session as downloadable JSON.
    
    Args:
        format: 'json' (full) or 'summary' (condensed)
    """
    export_data = export_session(session_id, format=format)
    
    if not export_data:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    
    filename = f"{session_id}.json" if format == "json" else f"{session_id}_summary.json"
    
    return Response(
        content=export_data,
        media_type="application/json",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


@app.post("/sessions/export")
async def export_multiple_sessions(request: ExportSessionsRequest):
    """
    Export multiple sessions as a combined JSON file.
    """
    export_data = export_sessions(request.session_ids, format=request.format)
    
    filename = f"sessions_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    
    return Response(
        content=export_data,
        media_type="application/json",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


# Run with: uvicorn main:app --host 0.0.0.0 --port 8080
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
