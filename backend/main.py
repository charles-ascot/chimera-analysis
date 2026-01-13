"""
FastAPI backend for NDJSON Analysis Console
Fetches sharded data from GCS, concatenates, and analyzes
"""

import json
import re
from typing import AsyncGenerator
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from google.cloud import storage

from analyzer import analyze_records, stream_progress, stream_result, stream_error

app = FastAPI(title="NDJSON Analysis API")

# Allow CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AnalyzeRequest(BaseModel):
    bucket_url: str


def parse_gcs_url(url: str) -> tuple[str, str]:
    """
    Parse GCS URL into bucket name and prefix.
    Supports formats:
    - gs://bucket-name/path/to/files/
    - https://storage.googleapis.com/bucket-name/path/to/files/
    - https://storage.cloud.google.com/bucket-name/path/to/files/
    """
    # gs:// format
    gs_match = re.match(r'gs://([^/]+)/?(.*)', url)
    if gs_match:
        return gs_match.group(1), gs_match.group(2).rstrip('/')

    # HTTPS format
    https_match = re.match(r'https://storage\.(?:googleapis|cloud\.google)\.com/([^/]+)/?(.*)', url)
    if https_match:
        return https_match.group(1), https_match.group(2).rstrip('/')

    raise ValueError(f"Invalid GCS URL format: {url}")


async def analyze_stream(bucket_url: str) -> AsyncGenerator[str, None]:
    """
    Stream analysis progress and results.
    """
    try:
        yield stream_progress("Parsing GCS URL...") + "\n"

        bucket_name, prefix = parse_gcs_url(bucket_url)
        yield stream_progress(f"Bucket: {bucket_name}, Prefix: {prefix}") + "\n"

        # Initialize GCS client
        yield stream_progress("Connecting to Google Cloud Storage...") + "\n"
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # List all blobs with the prefix
        yield stream_progress("Listing shard files...") + "\n"
        blobs = list(bucket.list_blobs(prefix=prefix))

        # Filter to only include files (not directories) with .ndjson or .json extension
        shard_blobs = [b for b in blobs if b.name.endswith(('.ndjson', '.json')) and b.size > 0]
        shard_blobs.sort(key=lambda b: b.name)  # Sort for consistent ordering

        if not shard_blobs:
            yield stream_error(f"No NDJSON files found at {bucket_url}") + "\n"
            return

        yield stream_progress(f"Found {len(shard_blobs)} shard files") + "\n"

        # Show file names
        for i, blob in enumerate(shard_blobs[:5]):
            yield stream_progress(f"  - {blob.name} ({blob.size / 1024 / 1024:.1f} MB)") + "\n"
        if len(shard_blobs) > 5:
            yield stream_progress(f"  ... and {len(shard_blobs) - 5} more files") + "\n"

        # Load and concatenate all shards
        yield stream_progress("Loading and concatenating shards...", 0) + "\n"

        records = []
        total_size = sum(b.size for b in shard_blobs)
        loaded_size = 0

        for i, blob in enumerate(shard_blobs):
            yield stream_progress(f"Loading shard {i+1}/{len(shard_blobs)}: {blob.name.split('/')[-1]}") + "\n"

            content = blob.download_as_text()
            lines = content.strip().split('\n')

            for line in lines:
                if line.strip():
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError:
                        continue

            loaded_size += blob.size
            progress = int((loaded_size / total_size) * 100)
            yield stream_progress(f"Loaded {len(records):,} records so far...", progress) + "\n"

        yield stream_progress(f"Total records loaded: {len(records):,}", 100) + "\n"

        # Run analysis
        yield stream_progress("Running analysis...") + "\n"
        results = analyze_records(records)

        yield stream_progress("Analysis complete!") + "\n"
        yield stream_result(results) + "\n"

    except ValueError as e:
        yield stream_error(str(e)) + "\n"
    except Exception as e:
        yield stream_error(f"Error: {str(e)}") + "\n"


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/analyze")
async def analyze(request: AnalyzeRequest):
    """
    Analyze NDJSON data from GCS bucket.
    Streams progress updates and final results.
    """
    return StreamingResponse(
        analyze_stream(request.bucket_url),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "NDJSON Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "POST /analyze": "Analyze NDJSON data from GCS bucket",
            "GET /health": "Health check"
        }
    }
