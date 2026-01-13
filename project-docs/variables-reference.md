# NDJSON Analysis Console - Variables Reference

## URLs & Endpoints

| Variable | Value | Location |
|----------|-------|----------|
| Frontend URL | `https://analyse.thync.online` | Cloudflare Pages |
| Frontend Alt URL | `https://chimera-analysis.pages.dev` | Cloudflare Pages |
| Cloud Run URL | `https://chimera-analysis-1026419041222.us-central1.run.app` | Cloud Run |
| API Endpoint | `POST /analyze` | Cloud Run |
| Health Endpoint | `GET /health` | Cloud Run |
| GitHub Repo | `https://github.com/charles-ascot/chimera-analysis.git` | GitHub |

## GCS Configuration

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
| Region | `us-central1` | Changed from europe-west1 |
| Memory | `16 GiB` | Max available without quota increase |
| CPU | `4` | |
| Request Timeout | `900` seconds | 15 minutes |
| Max Concurrent Requests | `1` | Heavy memory job |
| Max Instances | `2` | Quota limit |
| Min Instances | `0` | Scale to zero |
| Container Port | `8080` | Default |

## Backend Files

| File | Purpose |
|------|---------|
| `backend/main.py` | FastAPI app, `/analyze` endpoint |
| `backend/analyzer.py` | Analysis logic, `analyze_records()` function |
| `backend/requirements.txt` | Python dependencies |
| `backend/Dockerfile` | Container build |

## Frontend Files

| File | Purpose |
|------|---------|
| `frontend/index.html` | Main app, calls API |
| `frontend/assets/app.css` | Chimera styling |
| `frontend/images/chimera.png` | Background image |

## API Request/Response

| Field | Type | Description |
|-------|------|-------------|
| **Request** | | |
| `bucket_url` | string | GCS path, e.g., `gs://bucket-name/prefix/` |
| **Response (streamed)** | | |
| `type: "progress"` | JSON | `{type, message, progress?}` |
| `type: "error"` | JSON | `{type, message}` |
| `type: "result"` | JSON | `{type, data: {analysis results}}` |

## Analysis Results Schema

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
