# CHIMERA Analysis Console v2.0

**Dynamic Field Discovery for Heterogeneous Betfair Data**

## Overview

CHIMERA Analysis v2.0 is a complete rewrite focused on discovering and analyzing raw Betfair streaming data **without any hardcoded assumptions**. Unlike v1 which expected pre-normalized data, v2 handles the highly heterogeneous nature of live Betfair streams.

### Key Philosophy

1. **NEVER Normalize** - Keep raw data intact after concatenation
2. **DISCOVER, Don't Assume** - Analyze what's actually present, not what you expect
3. **Open Architecture** - Handle any fields that appear, not a preset list
4. **Human-Readable** - Map cryptic codes (ltp, batb, rc) to proper names

## Features

### Dynamic Field Discovery
- Recursively scans all nested structures
- No hardcoded field expectations
- Discovers every unique field path in the data
- Calculates presence percentages

### Human-Readable Mapping
- Built-in Betfair field dictionary
- Maps codes like `ltp` → "Last Traded Price"
- Categorizes fields (Price, Volume, Order Book, etc.)
- Visual icons and color coding

### Analysis Outputs
- **Field Categories**: Grouped by function with descriptions
- **Value Distributions**: Charts for categorical fields
- **Structure Analysis**: Data hierarchy visualization
- **Example Records**: Sample data for inspection
- **ML Suggestions**: Recommended models based on available features
- **BigQuery Schema**: Ready-to-use table definitions

## Architecture

```
Frontend (Cloudflare Pages)
    ↓
Cloud Run (FastAPI API)
    ↓
Cloud Batch (for large datasets)
    ↓
GCS (data source + results)
```

## Betfair Data Structure

Raw Betfair streaming data has this nested structure:

```json
{
  "op": "mcm",              // Operation type
  "pt": 1432122633617,      // Publish timestamp (ms)
  "clk": "769973511",       // Clock token
  "mc": [                   // Market changes array
    {
      "id": "1.123456789",  // Market ID
      "marketDefinition": { // Full market metadata
        "venue": "Ascot",
        "marketType": "WIN",
        "runners": [...]
      },
      "rc": [               // Runner changes (prices)
        {
          "id": 12345,      // Selection ID
          "ltp": 3.5,       // Last Traded Price
          "tv": 1234.56,    // Traded Volume
          "batb": [...],    // Best Available To Back
          "batl": [...],    // Best Available To Lay
          "trd": [...]      // Traded ladder
        }
      ]
    }
  ]
}
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check & API info |
| `/health` | GET | Health check |
| `/analyze` | POST | Start analysis job |
| `/analyze/stream` | POST | Stream analysis results |
| `/status/{job_id}` | GET | Get job status |
| `/field-dictionary` | GET | Get all known field mappings |
| `/field/{name}` | GET | Get info for specific field |

### Example Request

```bash
curl -X POST https://your-api.run.app/analyze \
  -H "Content-Type: application/json" \
  -d '{"bucket_url": "gs://your-bucket/dataflow-output/"}'
```

## Deployment

### Backend (Cloud Run)

```bash
cd backend
gcloud builds submit --tag gcr.io/PROJECT_ID/chimera-analysis
gcloud run deploy chimera-analysis \
  --image gcr.io/PROJECT_ID/chimera-analysis \
  --region us-central1 \
  --allow-unauthenticated
```

### Frontend (Cloudflare Pages)

```bash
cd frontend
# Connect to GitHub repo
# Set build command: (none)
# Set output directory: /
```

## Local Development

```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8080

# Frontend
cd frontend
python -m http.server 3000
```

## Field Categories

The analyzer groups fields into these categories:

| Category | Icon | Examples |
|----------|------|----------|
| Message Metadata | 📨 | op, pt, clk, ct |
| Market Identity | 🏷️ | marketId, eventId |
| Event Location | 📍 | venue, countryCode |
| Price - Core | 💲 | ltp |
| Volume | 📊 | tv |
| Order Book - Back | 📗 | batb, atb |
| Order Book - Lay | 📕 | batl, atl |
| Trade History | 📜 | trd |
| Starting Price | 🏁 | spb, spl, spn, spf |

## ML Model Suggestions

Based on discovered fields, the analyzer suggests:

1. **Price Movement Prediction** (LSTM/Transformer)
   - Uses: ltp, batb, batl time series
   - Target: Price direction

2. **Market Microstructure Analysis** (XGBoost)
   - Uses: Order book, trades, volume
   - Target: Execution quality

3. **Visual Price Patterns** (CNN)
   - Uses: Gramian Angular Field encoding
   - Target: Pattern classification

4. **Market Classification** (Random Forest)
   - Uses: marketType, venue, runners
   - Target: Market category

## Version History

- **v2.0** - Dynamic field discovery, no hardcoded assumptions
- **v1.0** - Required pre-normalized data

## License

Proprietary - Ascot Wealth Management
