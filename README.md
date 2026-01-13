# NDJSON Analysis Console

A web-based console for analyzing sharded NDJSON data from Google Cloud Storage.

## Architecture

- **Backend**: FastAPI on Google Cloud Run
- **Frontend**: Static HTML/CSS/JS on Cloudflare Pages

## Project Structure

```
chimera-analysis/
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py          # FastAPI application
│   └── analyzer.py      # Analysis logic
├── frontend/
│   ├── index.html
│   ├── assets/
│   │   └── styles.css   # Custom styling
│   └── images/
│       └── (background images)
└── README.md
```

## Deployment

### Backend (Cloud Run)

1. Build and deploy:
```bash
cd backend
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/ndjson-analyzer
gcloud run deploy ndjson-analyzer \
    --image gcr.io/YOUR_PROJECT_ID/ndjson-analyzer \
    --platform managed \
    --region us-central1 \
    --memory 4Gi \
    --timeout 900 \
    --allow-unauthenticated
```

2. Note the Cloud Run URL from the output.

### Frontend (Cloudflare Pages)

1. Update `API_URL` in `frontend/index.html` with your Cloud Run URL
2. Push to GitHub
3. Connect repository to Cloudflare Pages
4. Set build output directory to `frontend`

## Usage

1. Open the frontend URL
2. Paste a GCS bucket URL containing NDJSON shards (e.g., `gs://my-bucket/data-shards/`)
3. Click "Analyze"
4. View streaming progress and results

## GCS Authentication

Cloud Run uses the default service account. Ensure it has `storage.objects.get` and `storage.objects.list` permissions on your GCS bucket.
