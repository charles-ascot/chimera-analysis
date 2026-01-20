"""
Session Manager for CHIMERA Analysis

Handles persistent storage of analysis sessions in GCS.
Each session stores:
- Session ID (unique identifier)
- Source URL (gs:// path to data)
- Analysis results
- Schema recommendations
- Timestamps

Sessions are stored in: gs://betfair-chimera-sessions/{session_id}.json
"""

import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from google.cloud import storage

# Configuration
SESSIONS_BUCKET = "betfair-chimera-sessions"


def generate_session_id() -> str:
    """Generate a unique session ID."""
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    unique = uuid.uuid4().hex[:8]
    return f"sess-{timestamp}-{unique}"


def get_storage_client():
    """Get GCS storage client."""
    return storage.Client()


def ensure_bucket_exists():
    """Ensure the sessions bucket exists."""
    client = get_storage_client()
    try:
        bucket = client.bucket(SESSIONS_BUCKET)
        if not bucket.exists():
            bucket = client.create_bucket(SESSIONS_BUCKET, location="us-central1")
            print(f"Created bucket: {SESSIONS_BUCKET}")
        return bucket
    except Exception as e:
        print(f"Bucket check/create error: {e}")
        return client.bucket(SESSIONS_BUCKET)


def save_session(
    session_id: str,
    source_url: str,
    analysis_result: Dict,
    metadata: Optional[Dict] = None
) -> Dict:
    """
    Save an analysis session to GCS.
    
    Args:
        session_id: Unique session identifier
        source_url: GCS URL of the analyzed data (gs://...)
        analysis_result: Complete analysis results
        metadata: Optional additional metadata
    
    Returns:
        Session summary object
    """
    client = get_storage_client()
    bucket = client.bucket(SESSIONS_BUCKET)
    
    # Build session object
    session = {
        "session_id": session_id,
        "source_url": source_url,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "status": "complete",
        "metadata": metadata or {},
        "summary": {
            "total_records": analysis_result.get("total_records", 0),
            "unique_fields": len(analysis_result.get("discovered_fields", [])),
            "categories": len(analysis_result.get("field_categories", {})),
            "duration": analysis_result.get("temporal_analysis", {}).get("duration_readable", "N/A"),
        },
        "analysis_result": analysis_result,
        "schema_recommendations": analysis_result.get("schema_recommendations", {}),
        "ml_suggestions": analysis_result.get("ml_suggestions", []),
    }
    
    # Save to GCS
    blob = bucket.blob(f"{session_id}.json")
    blob.upload_from_string(
        json.dumps(session, indent=2, default=str),
        content_type="application/json"
    )
    
    print(f"Session saved: {session_id}")
    
    # Return summary (without full analysis for response efficiency)
    return {
        "session_id": session_id,
        "source_url": source_url,
        "created_at": session["created_at"],
        "status": session["status"],
        "summary": session["summary"],
    }


def get_session(session_id: str) -> Optional[Dict]:
    """
    Retrieve a session by ID.
    
    Args:
        session_id: Session identifier
    
    Returns:
        Full session object or None if not found
    """
    client = get_storage_client()
    bucket = client.bucket(SESSIONS_BUCKET)
    blob = bucket.blob(f"{session_id}.json")
    
    try:
        if blob.exists():
            content = blob.download_as_text()
            return json.loads(content)
    except Exception as e:
        print(f"Error loading session {session_id}: {e}")
    
    return None


def list_sessions(limit: int = 50) -> List[Dict]:
    """
    List all sessions (summaries only, not full analysis).
    
    Args:
        limit: Maximum number of sessions to return
    
    Returns:
        List of session summaries, newest first
    """
    client = get_storage_client()
    bucket = client.bucket(SESSIONS_BUCKET)
    
    sessions = []
    
    try:
        blobs = list(bucket.list_blobs(max_results=limit * 2))  # Get extra for filtering
        
        for blob in blobs:
            if blob.name.endswith('.json'):
                try:
                    content = blob.download_as_text()
                    session = json.loads(content)
                    
                    # Return summary only
                    sessions.append({
                        "session_id": session.get("session_id"),
                        "source_url": session.get("source_url"),
                        "created_at": session.get("created_at"),
                        "status": session.get("status"),
                        "summary": session.get("summary", {}),
                    })
                except Exception as e:
                    print(f"Error parsing session {blob.name}: {e}")
                    continue
        
        # Sort by created_at descending (newest first)
        sessions.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        
    except Exception as e:
        print(f"Error listing sessions: {e}")
    
    return sessions[:limit]


def delete_session(session_id: str) -> bool:
    """
    Delete a session by ID.
    
    Args:
        session_id: Session identifier
    
    Returns:
        True if deleted, False if not found
    """
    client = get_storage_client()
    bucket = client.bucket(SESSIONS_BUCKET)
    blob = bucket.blob(f"{session_id}.json")
    
    try:
        if blob.exists():
            blob.delete()
            print(f"Session deleted: {session_id}")
            return True
    except Exception as e:
        print(f"Error deleting session {session_id}: {e}")
    
    return False


def delete_sessions(session_ids: List[str]) -> Dict:
    """
    Delete multiple sessions.
    
    Args:
        session_ids: List of session identifiers
    
    Returns:
        Result summary
    """
    deleted = []
    failed = []
    
    for session_id in session_ids:
        if delete_session(session_id):
            deleted.append(session_id)
        else:
            failed.append(session_id)
    
    return {
        "deleted": deleted,
        "failed": failed,
        "total_deleted": len(deleted),
        "total_failed": len(failed),
    }


def export_session(session_id: str, format: str = "json") -> Optional[str]:
    """
    Export a session in the specified format.
    
    Args:
        session_id: Session identifier
        format: Export format ('json' or 'summary')
    
    Returns:
        Formatted export string or None if not found
    """
    session = get_session(session_id)
    
    if not session:
        return None
    
    if format == "summary":
        # Return condensed summary
        return json.dumps({
            "session_id": session["session_id"],
            "source_url": session["source_url"],
            "created_at": session["created_at"],
            "summary": session["summary"],
            "schema_recommendations": session.get("schema_recommendations", {}),
            "ml_suggestions": session.get("ml_suggestions", []),
        }, indent=2, default=str)
    else:
        # Full JSON export
        return json.dumps(session, indent=2, default=str)


def export_sessions(session_ids: List[str], format: str = "json") -> str:
    """
    Export multiple sessions as a combined JSON array.
    
    Args:
        session_ids: List of session identifiers
        format: Export format
    
    Returns:
        JSON array of sessions
    """
    sessions = []
    
    for session_id in session_ids:
        session = get_session(session_id)
        if session:
            if format == "summary":
                sessions.append({
                    "session_id": session["session_id"],
                    "source_url": session["source_url"],
                    "created_at": session["created_at"],
                    "summary": session["summary"],
                    "schema_recommendations": session.get("schema_recommendations", {}),
                })
            else:
                sessions.append(session)
    
    return json.dumps(sessions, indent=2, default=str)


def get_session_for_bigquery(session_id: str) -> Optional[Dict]:
    """
    Get session data formatted for BigQuery transport.
    
    This returns the essential info needed by the BigQuery module:
    - Source URL (where to fetch data)
    - Schema recommendations (table structure)
    - Field mappings
    
    Args:
        session_id: Session identifier
    
    Returns:
        BigQuery-ready session info or None
    """
    session = get_session(session_id)
    
    if not session:
        return None
    
    return {
        "session_id": session["session_id"],
        "source_url": session["source_url"],
        "created_at": session["created_at"],
        "total_records": session.get("summary", {}).get("total_records", 0),
        "schema": session.get("schema_recommendations", {}),
        "field_categories": session.get("analysis_result", {}).get("field_categories", {}),
        "discovered_fields": session.get("analysis_result", {}).get("discovered_fields", []),
    }
