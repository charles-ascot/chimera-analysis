# NDJSON Analysis Console - Variables Reference

## Architecture Overview

```
Frontend (Cloudflare Pages)
    |
    v
Cloud Run (FastAPI - lightweight dispatcher)
    |
    v
Cloud Batch (heavy compute - 64GB RAM, 8 vCPU)
    |
    v
GCS (manifests + results)
```

## URLs & Endpoints

| Variable | Value | Location |
|----------|-------|----------|
| Frontend URL | `https://analyse.thync.online` | Cloudflare Pages |
| Frontend Alt URL | `https://chimera-analysis.pages.dev` | Cloudflare Pages |
| Cloud Run URL | `https://chimera-analysis-1026419041222.us-central1.run.app` | Cloud Run |
| API Endpoint - Submit | `POST /analyze` | Cloud Run |
| API Endpoint - Status | `GET /status/{job_id}` | Cloud Run |
| Health Endpoint | `GET /health` | Cloud Run |
| GitHub Repo | `https://github.com/charles-ascot/chimera-analysis.git` | GitHub |

## GCS Buckets

| Bucket | Purpose |
|--------|---------|
| `betfair-chimera-march15-normalized` | Sample test data (10 shards, ~3.7GB) |
| `betfair-chimera-manifests` | Job manifests (JSON configs) |
| `betfair-chimera-results` | Analysis output (JSON results) |

## GCS Data Configuration

| Variable | Value | Notes |
|----------|-------|-------|
| Project ID | `betfair-data-explorer` | GCP Project |
| Sample Bucket | `gs://betfair-chimera-march15-normalized` | Test data location |
| Shard Pattern | `march_15_2016_normalized.ndjson-XXXXX-of-00010` | 10 shards |
| Shard Size | ~370 MB each | ~3.7 GB total |
| Total Records | ~7.1 million | Estimated from 714K per shard |

## Cloud Run Configuration

| Variable | Value | Notes |
|----------|-------|-------|
| Service Name | `chimera-analysis` | |
| Region | `us-central1` | |
| Memory | `512 MiB` | Lightweight - just dispatches jobs |
| CPU | `1` | |
| Request Timeout | `60` seconds | Just submits and returns |
| Max Instances | `10` | |
| Min Instances | `0` | Scale to zero |
| Container Port | `8080` | Default |

## Cloud Batch Configuration

| Variable | Value | Notes |
|----------|-------|-------|
| Container Image | `gcr.io/betfair-data-explorer/chimera-worker:latest` | Worker image |
| Machine Type | `n2-highmem-8` | 8 vCPU, 64GB RAM |
| CPU | `8000` milli (8 vCPU) | |
| Memory | `65536` MiB (64GB) | |
| Max Run Duration | `7200` seconds (2 hours) | |
| Region | `us-central1` | |

## Backend Files

| File | Purpose |
|------|---------|
| `backend/main.py` | FastAPI app, job dispatcher |
| `backend/batch_launcher.py` | Cloud Batch job submission |
| `backend/analyzer.py` | Analysis logic (pure compute) |
| `backend/worker.py` | Batch worker entry point |
| `backend/requirements.txt` | Python dependencies |
| `backend/Dockerfile` | Cloud Run container |
| `backend/Dockerfile.worker` | Cloud Batch worker container |

## Frontend Files

| File | Purpose |
|------|---------|
| `frontend/index.html` | Main app, polls for status |
| `frontend/assets/app.css` | Chimera styling |
| `frontend/images/chimera.png` | Background image |

## API Request/Response

### POST /analyze

**Request:**
```json
{
  "bucket_url": "gs://bucket-name/prefix/"
}
```

**Response:**
```json
{
  "type": "submitted",
  "job_id": "job-abc123",
  "batch_job_id": "chimera-xyz789",
  "message": "Job submitted to Cloud Batch"
}
```

### GET /status/{job_id}

**Response (running):**
```json
{
  "status": "running",
  "message": "Job is being processed by Cloud Batch"
}
```

**Response (complete):**
```json
{
  "status": "complete",
  "result": { /* analysis results */ }
}
```

## Job Manifest Schema

Stored in `gs://betfair-chimera-manifests/{job_id}.json`:

```json
{
  "job_id": "job-abc123",
  "bucket_url": "gs://source-bucket/data/",
  "output_prefix": "gs://betfair-chimera-results/job-abc123/"
}
```

## Analysis Results Schema

Stored in `gs://betfair-chimera-results/{job_id}/analysis_result.json`:

| Field | Type | Description |
|-------|------|-------------|
| `total_records` | int | Total records processed |
| `field_presence` | array | `[{field, presence_pct, null_count}]` |
| `temporal_patterns` | object | `{first_10, middle_10, last_10, total_timestamps}` |
| `examples` | object | `{full_metadata, pricing_only, advanced_pricing}` |
| `distributions` | object | `{market_type, country_code}` |
| `field_stats` | object | Numeric field statistics |
| `recommendations` | object | `{mandatory, mostly_complete, sparse}` |

## File Detection Patterns

| Pattern | Description |
|---------|-------------|
| `.ndjson` extension | Standard NDJSON |
| `.json` extension | JSON files |
| `.ndjson` in filename | e.g., `file.ndjson-00000-of-00010` |
| `-\d{5}-of-\d{5}$` regex | Sharded files pattern |
| `text/plain` content type | Fallback detection |

## Build & Deploy Commands

### Cloud Run (auto-deploys from GitHub)

Triggered automatically on push to `main`.

### Cloud Batch Worker (manual build)

```bash
cd backend
docker build -f Dockerfile.worker -t gcr.io/betfair-data-explorer/chimera-worker:latest .
docker push gcr.io/betfair-data-explorer/chimera-worker:latest
```

## Required GCS Buckets to Create

```bash
gsutil mb -l us-central1 gs://betfair-chimera-manifests
gsutil mb -l us-central1 gs://betfair-chimera-results
```
