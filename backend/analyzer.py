"""
Analysis logic for NDJSON data
Processes records to understand heterogeneity patterns

This module is pure compute - no HTTP/streaming dependencies.
Used by both Cloud Run (directly) and Cloud Batch (via worker.py).
"""

import json
import re
from collections import defaultdict
from typing import Optional
import pandas as pd
from google.cloud import storage


def load_records_from_gcs(bucket_url: str) -> list:
    """
    Load all NDJSON records from a GCS bucket path.

    Args:
        bucket_url: GCS path (e.g., gs://bucket-name/prefix/)

    Returns:
        List of parsed JSON records
    """
    # Parse GCS URL
    bucket_name, prefix = parse_gcs_url(bucket_url)

    print(f"Connecting to GCS bucket: {bucket_name}, prefix: {prefix}")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # List all blobs
    blobs = list(bucket.list_blobs(prefix=prefix))

    # Filter to data files
    shard_pattern = re.compile(r'-\d{5}-of-\d{5}$')
    shard_blobs = [b for b in blobs if b.size > 0 and (
        b.name.endswith(('.ndjson', '.json')) or
        '.ndjson' in b.name.lower() or
        '.json' in b.name.lower() or
        shard_pattern.search(b.name) or
        b.content_type in ('application/json', 'application/x-ndjson', 'text/plain')
    )]
    shard_blobs.sort(key=lambda b: b.name)

    if not shard_blobs:
        raise ValueError(f"No NDJSON files found at {bucket_url}")

    print(f"Found {len(shard_blobs)} shard files")

    # Load all records
    records = []
    total_size = sum(b.size for b in shard_blobs)
    loaded_size = 0

    for i, blob in enumerate(shard_blobs):
        print(f"Loading shard {i+1}/{len(shard_blobs)}: {blob.name.split('/')[-1]}")

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
        print(f"Progress: {progress}% - Loaded {len(records):,} records")

    print(f"Total records loaded: {len(records):,}")
    return records


def parse_gcs_url(url: str) -> tuple:
    """
    Parse GCS URL into bucket name and prefix.

    Supports formats:
    - gs://bucket-name/path/to/files/
    - https://storage.googleapis.com/bucket-name/path/to/files/
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


def analyze_records(bucket_url: str = None, records: list = None) -> dict:
    """
    Run complete analysis on NDJSON records.

    Can load from GCS (bucket_url) or use pre-loaded records.

    Args:
        bucket_url: GCS path to load data from
        records: Pre-loaded list of records (if bucket_url not provided)

    Returns:
        Dictionary with all analysis results
    """
    # Load records if bucket_url provided
    if bucket_url and not records:
        records = load_records_from_gcs(bucket_url)
    elif not records:
        records = []

    results = {
        "total_records": len(records),
        "field_presence": [],
        "temporal_patterns": [],
        "examples": {},
        "field_stats": {},
        "distributions": {},
        "recommendations": {
            "mandatory": [],
            "mostly_complete": [],
            "sparse": []
        }
    }

    if not records:
        return results

    print("Building DataFrame...")

    # Build DataFrame
    df = pd.DataFrame(records)

    # ========================================================================
    # FIELD PRESENCE ANALYSIS
    # ========================================================================
    print("Analyzing field presence...")

    field_presence = {}
    for col in df.columns:
        null_count = int(df[col].isnull().sum())
        presence_pct = (1 - null_count / len(df)) * 100
        field_presence[col] = {
            "field": col,
            "presence_pct": round(presence_pct, 2),
            "null_count": null_count
        }

    # Sort by presence (most to least)
    sorted_fields = sorted(field_presence.values(), key=lambda x: x["presence_pct"], reverse=True)
    results["field_presence"] = sorted_fields

    # ========================================================================
    # TEMPORAL ANALYSIS
    # ========================================================================
    print("Analyzing temporal patterns...")

    metadata_by_timestamp = defaultdict(list)

    for record in records:
        ts = record.get('timestamp', 'unknown')
        metadata_by_timestamp[ts].append({
            'has_venue': record.get('venue') is not None,
            'has_race_name': record.get('race_name') is not None,
            'has_event_name': record.get('event_name') is not None,
            'ltp': record.get('ltp') is not None,
            'batb': record.get('batb') is not None,
            'batl': record.get('batl') is not None,
            'trd': record.get('trd') is not None,
        })

    temporal_patterns = []
    for ts in sorted(metadata_by_timestamp.keys()):
        data = metadata_by_timestamp[ts]
        if len(data) == 0:
            continue

        temporal_patterns.append({
            'timestamp': str(ts),
            'count': len(data),
            'venue_pct': round(sum(d['has_venue'] for d in data) / len(data) * 100, 1),
            'race_name_pct': round(sum(d['has_race_name'] for d in data) / len(data) * 100, 1),
            'event_name_pct': round(sum(d['has_event_name'] for d in data) / len(data) * 100, 1),
            'ltp_pct': round(sum(d['ltp'] for d in data) / len(data) * 100, 1),
            'batb_pct': round(sum(d['batb'] for d in data) / len(data) * 100, 1),
            'trd_pct': round(sum(d['trd'] for d in data) / len(data) * 100, 1),
        })

    results["temporal_patterns"] = {
        "first_10": temporal_patterns[:10],
        "middle_10": temporal_patterns[max(0, len(temporal_patterns)//2 - 5):min(len(temporal_patterns), len(temporal_patterns)//2 + 5)],
        "last_10": temporal_patterns[-10:] if len(temporal_patterns) >= 10 else temporal_patterns,
        "total_timestamps": len(temporal_patterns)
    }

    # ========================================================================
    # EXAMPLE RECORDS
    # ========================================================================
    print("Finding example records...")

    # Example 1: Record with all metadata
    for record in records:
        if (record.get('venue') and record.get('race_name') and
            record.get('event_name') and record.get('ltp')):
            results["examples"]["full_metadata"] = record
            break

    # Example 2: Record with NO metadata (just pricing)
    for record in records:
        if (not record.get('venue') and not record.get('race_name') and
            record.get('ltp') is not None):
            results["examples"]["pricing_only"] = record
            break

    # Example 3: Record with ADVANCED tier pricing ladder
    for record in records:
        if record.get('batb') and record.get('batl') and record.get('trd'):
            record_copy = record.copy()
            if isinstance(record_copy.get('batb'), list):
                record_copy['batb'] = record_copy['batb'][:3]
            if isinstance(record_copy.get('batl'), list):
                record_copy['batl'] = record_copy['batl'][:3]
            if isinstance(record_copy.get('trd'), list):
                record_copy['trd'] = record_copy['trd'][:3]
            results["examples"]["advanced_pricing"] = record_copy
            break

    # ========================================================================
    # FIELD VALUE DISTRIBUTIONS
    # ========================================================================
    print("Computing distributions...")

    # Market type distribution
    if 'market_type' in df.columns:
        market_types = df['market_type'].value_counts().to_dict()
        results["distributions"]["market_type"] = [
            {"value": str(k), "count": int(v), "pct": round(v / len(df) * 100, 2)}
            for k, v in market_types.items()
        ]

    # Country code distribution
    if 'country_code' in df.columns:
        countries = df['country_code'].value_counts().to_dict()
        results["distributions"]["country_code"] = [
            {"value": str(k), "count": int(v), "pct": round(v / len(df) * 100, 2)}
            for k, v in countries.items()
        ]

    # Numeric field statistics
    print("Computing numeric statistics...")

    numeric_fields = ['ltp', 'back_price', 'lay_price', 'back_volume', 'lay_volume', 'total_matched_volume']
    for field in numeric_fields:
        if field in df.columns:
            try:
                stats = df[field].describe()
                results["field_stats"][field] = {
                    "count": int(stats.get('count', 0)),
                    "mean": round(float(stats.get('mean', 0)), 2),
                    "std": round(float(stats.get('std', 0)), 2),
                    "min": round(float(stats.get('min', 0)), 2),
                    "max": round(float(stats.get('max', 0)), 2)
                }
            except:
                pass

    # ========================================================================
    # SCHEMA RECOMMENDATIONS
    # ========================================================================
    print("Generating schema recommendations...")

    for field_data in sorted_fields:
        field = field_data["field"]
        pct = field_data["presence_pct"]

        if pct == 100:
            results["recommendations"]["mandatory"].append(field)
        elif pct >= 95:
            results["recommendations"]["mostly_complete"].append({"field": field, "pct": pct})
        elif pct < 50:
            results["recommendations"]["sparse"].append({"field": field, "pct": pct})

    print("Analysis complete!")
    return results


# Legacy streaming helpers (still used by main.py for progress updates)
def stream_progress(message: str, progress: int = None) -> str:
    """Format a progress message for streaming."""
    data = {"type": "progress", "message": message}
    if progress is not None:
        data["progress"] = progress
    return json.dumps(data)


def stream_result(results: dict) -> str:
    """Format final results for streaming."""
    return json.dumps({"type": "result", "data": results}, default=str)


def stream_error(error: str) -> str:
    """Format an error message for streaming."""
    return json.dumps({"type": "error", "message": error})
