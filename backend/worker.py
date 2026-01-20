# worker.py
"""
Cloud Batch worker - runs the heavy analysis on a big machine
"""
import argparse
import json
from google.cloud import storage
from analyzer import analyze_records


def load_manifest(gcs_path: str) -> dict:
    """
    Load job manifest from GCS.

    Args:
        gcs_path: Full GCS path (gs://bucket/path/to/file.json)

    Returns:
        Parsed manifest dict
    """
    assert gcs_path.startswith("gs://"), f"Invalid GCS path: {gcs_path}"

    # Parse gs://bucket/path/to/file.json
    path_without_prefix = gcs_path.replace("gs://", "")
    parts = path_without_prefix.split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1] if len(parts) > 1 else ""

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    return json.loads(blob.download_as_text())


def save_results(results: dict, output_prefix: str, job_id: str):
    """
    Save analysis results to GCS.

    Args:
        results: Analysis results dict
        output_prefix: GCS prefix for output (gs://bucket/path/)
        job_id: Job ID for the filename
    """
    # Parse output prefix
    path_without_prefix = output_prefix.replace("gs://", "")
    parts = path_without_prefix.split("/", 1)
    bucket_name = parts[0]
    blob_prefix = parts[1] if len(parts) > 1 else ""

    # Ensure prefix ends with /
    if blob_prefix and not blob_prefix.endswith("/"):
        blob_prefix += "/"

    output_path = f"{blob_prefix}analysis_result.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(output_path)

    blob.upload_from_string(
        json.dumps(results, indent=2, default=str),
        content_type="application/json"
    )

    print(f"Results saved to gs://{bucket_name}/{output_path}")


def main():
    parser = argparse.ArgumentParser(description="NDJSON Analysis Worker")
    parser.add_argument("--manifest", required=True, help="GCS path to job manifest")
    args = parser.parse_args()

    print(f"Loading manifest from {args.manifest}")
    manifest = load_manifest(args.manifest)

    job_id = manifest["job_id"]
    bucket_url = manifest["bucket_url"]
    output_prefix = manifest["output_prefix"]

    print(f"Job ID: {job_id}")
    print(f"Bucket URL: {bucket_url}")
    print(f"Output prefix: {output_prefix}")

    print("Starting analysis...")
    results = analyze_records(bucket_url=bucket_url)

    print("Saving results...")
    save_results(results, output_prefix, job_id)

    print("Done!")


if __name__ == "__main__":
    main()
