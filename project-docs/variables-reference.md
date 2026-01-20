# CHIMERA Analysis Console v2.0 - Variables Reference

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
GCS (data source + results)
```

## URLs & Endpoints

| Variable | Value | Location |
|----------|-------|----------|
| Frontend URL | `https://analyse.thync.online` | Cloudflare Pages |
| Cloud Run URL | `https://chimera-analysis-1026419041222.us-central1.run.app` | Cloud Run |
| API Endpoint - Analyze | `POST /analyze` | Cloud Run |
| API Endpoint - Stream | `POST /analyze/stream` | Cloud Run |
| API Endpoint - Status | `GET /status/{job_id}` | Cloud Run |
| API Endpoint - Dictionary | `GET /field-dictionary` | Cloud Run |
| Health Endpoint | `GET /health` | Cloud Run |

## GCS Buckets

| Bucket | Purpose |
|--------|---------|
| `betfair-basic-historic` | Source Betfair data |
| `betfair-chimera-manifests` | Job manifests (JSON configs) |
| `betfair-chimera-results` | Analysis output (JSON results) |
| Dataflow output bucket | User-specified, varies |

## Key Differences from v1

| Aspect | v1.0 | v2.0 |
|--------|------|------|
| Field discovery | Hardcoded list | Dynamic recursive |
| Data format | Pre-normalized | Raw Betfair streams |
| Field names | Flat (venue, ltp) | Nested (mc.rc.ltp) |
| Human names | None | Full dictionary |
| ML suggestions | None | Based on discovered fields |
| Schema output | None | BigQuery-ready |

## Betfair Field Dictionary

The backend includes a comprehensive dictionary mapping:

### Top-Level Fields
- `op` → Operation Type
- `pt` → Publish Time
- `clk` → Clock Token
- `mc` → Market Changes
- `oc` → Order Changes

### Runner Change Fields (Price Data)
- `ltp` → Last Traded Price
- `tv` → Traded Volume
- `batb` → Best Available To Back
- `batl` → Best Available To Lay
- `trd` → Traded Ladder
- `spb` → SP Back
- `spl` → SP Lay
- `spn` → SP Near Price
- `spf` → SP Far Price

### Market Definition Fields
- `marketId` → Market ID
- `eventId` → Event ID
- `marketType` → Market Type (WIN, PLACE, etc.)
- `venue` → Venue
- `countryCode` → Country Code
- `status` → Market Status
- `numberOfActiveRunners` → Active Runners
- `bettingType` → Betting Type

## API Request/Response

### POST /analyze

**Request:**
```json
{
  "bucket_url": "gs://bucket-name/prefix/",
  "use_batch": false
}
```

**Response:**
```json
{
  "type": "submitted",
  "job_id": "job-abc123",
  "message": "Analysis started",
  "status_url": "/status/job-abc123"
}
```

### GET /status/{job_id}

**Response (complete):**
```json
{
  "job_id": "job-abc123",
  "status": "complete",
  "result": {
    "total_records": 123456,
    "discovered_fields": [...],
    "field_categories": {...},
    "value_distributions": {...},
    "temporal_analysis": {...},
    "examples": {...},
    "ml_suggestions": [...],
    "schema_recommendations": {...}
  }
}
```

## Analysis Result Schema

| Field | Type | Description |
|-------|------|-------------|
| `total_records` | int | Total records processed |
| `discovered_fields` | array | All unique field paths with stats |
| `field_categories` | object | Fields grouped by category |
| `value_distributions` | object | Distribution charts for categorical fields |
| `temporal_analysis` | object | Timestamp range and duration |
| `examples` | object | Sample records |
| `data_quality` | object | Completeness metrics |
| `ml_suggestions` | array | Recommended ML models |
| `schema_recommendations` | object | BigQuery schema |

## Field Categories

| Category | Icon | Color | Description |
|----------|------|-------|-------------|
| Message Metadata | 📨 | #6B7280 | Stream control fields |
| Market Identity | 🏷️ | #3B82F6 | Market identification |
| Event Identity | 📅 | #8B5CF6 | Event identification |
| Event Location | 📍 | #EC4899 | Venue and location |
| Event Timing | ⏰ | #F59E0B | Timing and scheduling |
| Market State | 🔄 | #10B981 | Current status |
| Price - Core | 💲 | #EF4444 | Core pricing |
| Volume | 📊 | #3B82F6 | Trading volume |
| Order Book - Back | 📗 | #22C55E | Back offers |
| Order Book - Lay | 📕 | #EF4444 | Lay offers |
| Trade History | 📜 | #F59E0B | Historical trades |
| Starting Price | 🏁 | #EC4899 | BSP data |

## Build & Deploy Commands

### Cloud Run (Backend)

```bash
cd backend
gcloud builds submit --tag gcr.io/betfair-data-explorer/chimera-analysis:v2
gcloud run deploy chimera-analysis \
  --image gcr.io/betfair-data-explorer/chimera-analysis:v2 \
  --region us-central1 \
  --memory 2Gi \
  --timeout 300 \
  --allow-unauthenticated
```

### Cloud Batch Worker

```bash
cd backend
docker build -f Dockerfile.worker -t gcr.io/betfair-data-explorer/chimera-worker:v2 .
docker push gcr.io/betfair-data-explorer/chimera-worker:v2
```

### Cloudflare Pages (Frontend)

Connected to GitHub - auto-deploys on push to main.

## Local Development

```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8080

# Frontend (separate terminal)
cd frontend
python -m http.server 3000
# Open http://localhost:3000
```
