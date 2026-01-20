"""
CHIMERA Analysis Engine v2.1 - Dynamic Field Discovery with Plugin Support

This analyzer discovers ALL fields present in raw Betfair data without any
hardcoded assumptions. It handles nested structures, maps fields to human-readable
names using external plugins, and provides comprehensive statistics.

Philosophy:
- NEVER normalize data - keep raw format
- DISCOVER all fields dynamically - no presets
- HANDLE heterogeneous data - every batch is different
- PRESENT visually - human-readable names, not codes
- USE PLUGINS for field definitions - no hardcoded mappings
"""

import json
import re
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple
from google.cloud import storage

# Try to load plugin system, fall back to legacy dictionary
try:
    from plugin_loader import (
        load_plugin,
        get_field_info,
        get_category_info,
        get_all_categories,
        get_ml_recommendations,
        get_bigquery_config,
        get_derived_features,
    )
    USE_PLUGINS = True
    print("Plugin system loaded successfully")
except ImportError:
    from betfair_dictionary import get_field_info, FIELD_CATEGORIES
    USE_PLUGINS = False
    print("Using legacy betfair_dictionary (plugin_loader not available)")

# Active plugin ID (can be overridden)
ACTIVE_PLUGIN = os.environ.get("CHIMERA_PLUGIN", "betfair")


def load_records_from_gcs(bucket_url: str) -> List[dict]:
    """
    Load all NDJSON records from a GCS bucket path.
    
    Args:
        bucket_url: GCS path (e.g., gs://bucket-name/prefix/)
    
    Returns:
        List of parsed JSON records (raw, unmodified)
    """
    bucket_name, prefix = parse_gcs_url(bucket_url)
    
    print(f"Connecting to GCS bucket: {bucket_name}, prefix: {prefix}")
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # List all blobs
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    # Filter to data files (be very permissive)
    shard_pattern = re.compile(r'-\d{5}-of-\d{5}')
    data_blobs = [b for b in blobs if b.size > 0 and (
        b.name.endswith(('.ndjson', '.json', '.jsonl')) or
        '.ndjson' in b.name.lower() or
        shard_pattern.search(b.name) or
        b.content_type in ('application/json', 'application/x-ndjson', 'text/plain')
    )]
    data_blobs.sort(key=lambda b: b.name)
    
    if not data_blobs:
        raise ValueError(f"No NDJSON files found at {bucket_url}")
    
    print(f"Found {len(data_blobs)} data files")
    
    # Load all records RAW - no processing
    records = []
    total_size = sum(b.size for b in data_blobs)
    loaded_size = 0
    
    for i, blob in enumerate(data_blobs):
        print(f"Loading file {i+1}/{len(data_blobs)}: {blob.name.split('/')[-1]}")
        
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


def parse_gcs_url(url: str) -> Tuple[str, str]:
    """Parse GCS URL into bucket name and prefix."""
    gs_match = re.match(r'gs://([^/]+)/?(.*)', url)
    if gs_match:
        return gs_match.group(1), gs_match.group(2).rstrip('/')
    
    https_match = re.match(r'https://storage\.(?:googleapis|cloud\.google)\.com/([^/]+)/?(.*)', url)
    if https_match:
        return https_match.group(1), https_match.group(2).rstrip('/')
    
    raise ValueError(f"Invalid GCS URL format: {url}")


def discover_fields_recursive(
    obj: Any,
    path: str = "",
    field_registry: Dict = None,
    depth: int = 0,
    max_depth: int = 10,
    context: str = None,
    plugin_id: str = None
) -> Dict:
    """
    Recursively discover all fields in a nested structure.
    
    Args:
        obj: The object to analyze
        path: Current path (e.g., "mc.marketDefinition.venue")
        field_registry: Accumulator for discovered fields
        depth: Current recursion depth
        max_depth: Maximum recursion depth
        context: Context hint for field lookup (e.g., 'rc', 'marketDefinition')
        plugin_id: Plugin to use for field definitions
    
    Returns:
        Updated field registry
    """
    if field_registry is None:
        field_registry = {}
    
    if depth > max_depth:
        return field_registry
    
    if plugin_id is None:
        plugin_id = ACTIVE_PLUGIN
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            field_path = f"{path}.{key}" if path else key
            
            # Determine context for child fields
            child_context = context
            if key == 'mc':
                child_context = 'mc'
            elif key == 'rc':
                child_context = 'rc'
            elif key == 'marketDefinition':
                child_context = 'marketDefinition'
            elif key == 'runners':
                child_context = 'runners'
            elif key == 'oc':
                child_context = 'oc'
            elif key == 'uo':
                child_context = 'uo'
            
            # Get field info from plugin
            if USE_PLUGINS:
                field_info = get_field_info(key, plugin_id, child_context)
            else:
                field_info = get_field_info(key, child_context)
            
            # Determine value type
            value_type = type(value).__name__
            if isinstance(value, list) and len(value) > 0:
                first_item = value[0]
                if isinstance(first_item, dict):
                    value_type = "array[object]"
                elif isinstance(first_item, list):
                    value_type = "array[array]"
                else:
                    value_type = f"array[{type(first_item).__name__}]"
            
            # Register field
            if field_path not in field_registry:
                field_registry[field_path] = {
                    "path": field_path,
                    "key": key,
                    "name": field_info.get("name", key),
                    "description": field_info.get("description", ""),
                    "category": field_info.get("category", "Unknown"),
                    "ml_relevance": field_info.get("ml_relevance", "unknown"),
                    "type": value_type,
                    "count": 0,
                    "sample_values": [],
                    "contexts": set(),
                }
            
            field_registry[field_path]["count"] += 1
            if context:
                field_registry[field_path]["contexts"].add(context)
            
            # Collect sample values (limit to 5)
            if len(field_registry[field_path]["sample_values"]) < 5:
                if not isinstance(value, (dict, list)) or (isinstance(value, list) and len(value) < 10):
                    sample = value
                    if isinstance(value, list) and len(value) > 0:
                        sample = value[:3]  # Truncate arrays
                    field_registry[field_path]["sample_values"].append(sample)
            
            # Recurse into nested structures
            if isinstance(value, dict):
                discover_fields_recursive(value, field_path, field_registry, depth + 1, max_depth, child_context, plugin_id)
            elif isinstance(value, list):
                # Sample first few items of arrays
                for i, item in enumerate(value[:3]):
                    if isinstance(item, dict):
                        item_path = f"{field_path}[{i}]"
                        discover_fields_recursive(item, item_path, field_registry, depth + 1, max_depth, child_context, plugin_id)
    
    return field_registry


def analyze_records(bucket_url: str = None, records: List = None, plugin_id: str = None) -> Dict:
    """
    Run complete dynamic analysis on NDJSON records.
    
    This function discovers ALL fields present without any hardcoded assumptions.
    Uses external plugin for field definitions and ML recommendations.
    
    Args:
        bucket_url: GCS path to load data from
        records: Pre-loaded list of records
        plugin_id: Plugin to use for field definitions (default: ACTIVE_PLUGIN)
    
    Returns:
        Comprehensive analysis results
    """
    if plugin_id is None:
        plugin_id = ACTIVE_PLUGIN
    
    # Load records if needed
    if bucket_url and not records:
        records = load_records_from_gcs(bucket_url)
    elif not records:
        records = []
    
    print(f"Analyzing {len(records):,} records using plugin: {plugin_id}")
    
    # Load plugin for category info
    if USE_PLUGINS:
        plugin = load_plugin(plugin_id)
        all_categories = get_all_categories(plugin_id)
    else:
        all_categories = FIELD_CATEGORIES
    
    results = {
        "total_records": len(records),
        "plugin_id": plugin_id,
        "discovered_fields": [],
        "field_categories": {},
        "structure_analysis": {},
        "value_distributions": {},
        "temporal_analysis": {},
        "examples": {},
        "data_quality": {},
        "schema_recommendations": {},
        "ml_suggestions": [],
    }
    
    if not records:
        return results
    
    # ========================================================================
    # PHASE 1: DYNAMIC FIELD DISCOVERY
    # ========================================================================
    print("Phase 1: Discovering all fields...")
    
    field_registry = {}
    
    for i, record in enumerate(records):
        discover_fields_recursive(record, "", field_registry, plugin_id=plugin_id)
        
        if (i + 1) % 10000 == 0:
            print(f"  Scanned {i + 1:,} records, found {len(field_registry)} unique field paths")
    
    print(f"  Discovered {len(field_registry)} unique field paths")
    
    # Convert sets to lists for JSON serialization
    for field_data in field_registry.values():
        field_data["contexts"] = list(field_data["contexts"])
    
    # Calculate presence percentages
    for field_path, field_data in field_registry.items():
        field_data["presence_pct"] = round((field_data["count"] / len(records)) * 100, 2)
    
    # Sort by presence (most common first)
    sorted_fields = sorted(
        field_registry.values(),
        key=lambda x: (-x["presence_pct"], x["path"])
    )
    
    results["discovered_fields"] = sorted_fields
    
    # ========================================================================
    # PHASE 2: CATEGORIZE FIELDS
    # ========================================================================
    print("Phase 2: Categorizing fields...")
    
    categories = defaultdict(list)
    for field_data in sorted_fields:
        cat = field_data["category"]
        categories[cat].append({
            "path": field_data["path"],
            "key": field_data["key"],
            "name": field_data["name"],
            "type": field_data["type"],
            "presence_pct": field_data["presence_pct"],
            "ml_relevance": field_data.get("ml_relevance", "unknown"),
        })
    
    # Add category metadata from plugin
    for cat_name, fields in categories.items():
        if USE_PLUGINS:
            cat_info = get_category_info(cat_name, plugin_id)
        else:
            cat_info = FIELD_CATEGORIES.get(cat_name, FIELD_CATEGORIES.get("Unknown", {
                "icon": "❓",
                "description": "Unknown category",
                "color": "#6B7280"
            }))
        
        results["field_categories"][cat_name] = {
            "icon": cat_info.get("icon", "❓"),
            "description": cat_info.get("description", ""),
            "color": cat_info.get("color", "#6B7280"),
            "field_count": len(fields),
            "fields": fields,
        }
    
    # ========================================================================
    # PHASE 3: STRUCTURE ANALYSIS
    # ========================================================================
    print("Phase 3: Analyzing data structure...")
    
    # Identify top-level structure
    top_level_fields = [f for f in sorted_fields if "." not in f["path"] and "[" not in f["path"]]
    
    # Identify nested structures
    nested_paths = set()
    for f in sorted_fields:
        parts = f["path"].split(".")
        for i in range(1, len(parts)):
            nested_paths.add(".".join(parts[:i]))
    
    results["structure_analysis"] = {
        "top_level_fields": [f["key"] for f in top_level_fields],
        "nested_structures": list(nested_paths),
        "max_depth": max(len(f["path"].split(".")) for f in sorted_fields) if sorted_fields else 0,
        "total_unique_paths": len(sorted_fields),
    }
    
    # ========================================================================
    # PHASE 4: VALUE DISTRIBUTIONS
    # ========================================================================
    print("Phase 4: Computing value distributions...")
    
    # Find categorical fields and compute distributions
    categorical_candidates = [
        "op", "status", "marketType", "bettingType", "countryCode",
        "venue", "ct", "inPlay", "complete", "side"
    ]
    
    for field_path in list(field_registry.keys()):
        field_key = field_path.split(".")[-1].replace("[0]", "").replace("[1]", "").replace("[2]", "")
        
        if field_key in categorical_candidates:
            # Collect values for this field
            values = []
            for record in records[:10000]:  # Sample for performance
                value = get_nested_value(record, field_path)
                if value is not None and not isinstance(value, (dict, list)):
                    values.append(str(value))
            
            if values:
                # Count occurrences
                value_counts = defaultdict(int)
                for v in values:
                    value_counts[v] += 1
                
                # Top 20 values
                sorted_counts = sorted(value_counts.items(), key=lambda x: -x[1])[:20]
                
                results["value_distributions"][field_path] = {
                    "field": field_path,
                    "field_name": field_registry.get(field_path, {}).get("name", field_key),
                    "unique_values": len(value_counts),
                    "sample_size": len(values),
                    "distribution": [
                        {"value": v, "count": c, "pct": round((c / len(values)) * 100, 2)}
                        for v, c in sorted_counts
                    ]
                }
    
    # ========================================================================
    # PHASE 5: TEMPORAL ANALYSIS
    # ========================================================================
    print("Phase 5: Analyzing temporal patterns...")
    
    # Find timestamp field (pt = publish time)
    timestamps = []
    for record in records:
        pt = record.get("pt")
        if pt:
            timestamps.append(pt)
    
    if timestamps:
        timestamps.sort()
        results["temporal_analysis"] = {
            "timestamp_field": "pt (Publish Time)",
            "first_timestamp": timestamps[0],
            "last_timestamp": timestamps[-1],
            "duration_ms": timestamps[-1] - timestamps[0] if len(timestamps) > 1 else 0,
            "duration_readable": format_duration(timestamps[-1] - timestamps[0]) if len(timestamps) > 1 else "N/A",
            "total_timestamps": len(timestamps),
            "avg_interval_ms": round((timestamps[-1] - timestamps[0]) / len(timestamps)) if len(timestamps) > 1 else 0,
        }
    
    # ========================================================================
    # PHASE 6: EXAMPLE RECORDS
    # ========================================================================
    print("Phase 6: Collecting example records...")
    
    # First record
    if records:
        results["examples"]["first_record"] = records[0]
    
    # Record with marketDefinition
    for record in records[:1000]:
        mc = record.get("mc", [])
        if mc and isinstance(mc, list):
            for m in mc:
                if isinstance(m, dict) and "marketDefinition" in m:
                    results["examples"]["with_market_definition"] = record
                    break
        if "with_market_definition" in results["examples"]:
            break
    
    # Record with price data (rc)
    for record in records[:1000]:
        mc = record.get("mc", [])
        if mc and isinstance(mc, list):
            for m in mc:
                if isinstance(m, dict) and "rc" in m:
                    rc = m["rc"]
                    if rc and isinstance(rc, list) and len(rc) > 0:
                        # Check for price fields
                        for r in rc:
                            if isinstance(r, dict) and any(k in r for k in ["ltp", "batb", "batl", "trd"]):
                                results["examples"]["with_price_data"] = record
                                break
                if "with_price_data" in results["examples"]:
                    break
        if "with_price_data" in results["examples"]:
            break
    
    # ========================================================================
    # PHASE 7: DATA QUALITY ASSESSMENT
    # ========================================================================
    print("Phase 7: Assessing data quality...")
    
    # Calculate quality metrics
    always_present = [f for f in sorted_fields if f["presence_pct"] == 100]
    mostly_present = [f for f in sorted_fields if 95 <= f["presence_pct"] < 100]
    sometimes_present = [f for f in sorted_fields if 50 <= f["presence_pct"] < 95]
    rarely_present = [f for f in sorted_fields if f["presence_pct"] < 50]
    
    results["data_quality"] = {
        "completeness": {
            "always_present": len(always_present),
            "mostly_present": len(mostly_present),
            "sometimes_present": len(sometimes_present),
            "rarely_present": len(rarely_present),
        },
        "field_summary": {
            "always_present_fields": [f["path"] for f in always_present],
            "rarely_present_fields": [f["path"] for f in rarely_present[:20]],  # Top 20
        },
        "record_count": len(records),
        "unique_field_paths": len(sorted_fields),
    }
    
    # ========================================================================
    # PHASE 8: SCHEMA RECOMMENDATIONS
    # ========================================================================
    print("Phase 8: Generating schema recommendations...")
    
    # BigQuery schema suggestions
    bq_schema = []
    for field_data in sorted_fields:
        if field_data["presence_pct"] >= 50:  # Only include commonly present fields
            bq_type = infer_bq_type(field_data["type"], field_data["sample_values"])
            bq_schema.append({
                "name": sanitize_bq_field_name(field_data["path"]),
                "type": bq_type,
                "mode": "REQUIRED" if field_data["presence_pct"] == 100 else "NULLABLE",
                "description": field_data["description"],
                "source_path": field_data["path"],
            })
    
    results["schema_recommendations"] = {
        "bigquery_schema": bq_schema[:50],  # Top 50 fields
        "notes": [
            "Schema based on fields present in ≥50% of records",
            "Nested fields flattened with underscore separators",
            "Array fields may need additional handling",
        ]
    }
    
    # ========================================================================
    # PHASE 9: ML MODEL SUGGESTIONS (from plugin)
    # ========================================================================
    print("Phase 9: Suggesting ML models...")
    
    # Analyze what kind of data we have
    has_prices = any("ltp" in f["path"] or "batb" in f["path"] for f in sorted_fields)
    has_volume = any("tv" in f["path"] or "trd" in f["path"] for f in sorted_fields)
    has_time_series = len(timestamps) > 100
    has_order_book = any("batb" in f["path"] or "batl" in f["path"] for f in sorted_fields)
    has_market_definition = any("marketDefinition" in f["path"] for f in sorted_fields)
    has_trd = any("trd" in f["path"] for f in sorted_fields)
    
    suggestions = []
    
    # Get recommendations from plugin
    if USE_PLUGINS:
        plugin_ml = get_ml_recommendations(plugin_id)
        derived_features = get_derived_features(plugin_id)
        
        # Map data availability to plugin recommendations
        data_profile = {
            "has_prices": has_prices,
            "has_volume": has_volume,
            "has_order_book": has_order_book,
            "has_time_series": has_time_series,
            "has_market_definition": has_market_definition,
            "has_trd": has_trd,
        }
        
        # DeepLOB - needs order book data
        if "DeepLOB" in plugin_ml and has_order_book and has_time_series:
            model_info = plugin_ml["DeepLOB"]
            suggestions.append({
                "model_type": model_info.get("name", "DeepLOB"),
                "approach": model_info.get("architecture", "CNN + Inception + LSTM"),
                "description": model_info.get("description", "State-of-the-art limit order book prediction"),
                "key_features": model_info.get("input_features", {}).get("primary", ["batb", "batl", "ltp", "tv"]),
                "target": model_info.get("target", "Mid-price direction"),
                "complexity": model_info.get("complexity", "High"),
                "reference": model_info.get("reference", "Zhang et al., 2019"),
                "suitability": model_info.get("suitability", "Primary recommendation"),
            })
        
        # GAF_CNN - needs price time series
        if "GAF_CNN" in plugin_ml and has_prices and has_time_series:
            model_info = plugin_ml["GAF_CNN"]
            suggestions.append({
                "model_type": model_info.get("name", "Gramian Angular Field + CNN"),
                "approach": model_info.get("architecture", "GAF Image Encoding + Pre-trained CNN"),
                "description": model_info.get("description", "Visual pattern recognition on time series"),
                "key_features": model_info.get("input_features", {}).get("primary", ["ltp"]),
                "target": model_info.get("target", "Price direction, pattern classification"),
                "complexity": model_info.get("complexity", "High"),
                "preprocessing": model_info.get("preprocessing", {}),
                "reference": model_info.get("reference", "Chen (2021)"),
                "suitability": model_info.get("suitability", "Secondary recommendation"),
                "note": "This is your Heat Map approach - converts market data to images for CNN pattern recognition",
            })
        
        # XGBoost Baseline - always applicable with prices
        if "XGBoost_Baseline" in plugin_ml and has_prices:
            model_info = plugin_ml["XGBoost_Baseline"]
            suggestions.append({
                "model_type": model_info.get("name", "XGBoost/LightGBM Baseline"),
                "approach": model_info.get("architecture", "Gradient Boosted Decision Trees"),
                "description": model_info.get("description", "Interpretable baseline with engineered features"),
                "key_features": model_info.get("input_features", {}).get("engineered", ["spread", "order_imbalance"]),
                "target": model_info.get("target", "Price direction"),
                "complexity": model_info.get("complexity", "Medium"),
                "suitability": model_info.get("suitability", "Baseline"),
            })
        
        # Temporal Fusion Transformer - for advanced multi-horizon
        if "Temporal_Fusion_Transformer" in plugin_ml and has_time_series and has_market_definition:
            model_info = plugin_ml["Temporal_Fusion_Transformer"]
            suggestions.append({
                "model_type": model_info.get("name", "Temporal Fusion Transformer"),
                "approach": model_info.get("architecture", "Multi-horizon attention-based forecasting"),
                "description": model_info.get("description", "Advanced model for static and dynamic features"),
                "key_features": {
                    "static": model_info.get("input_features", {}).get("static", []),
                    "dynamic": model_info.get("input_features", {}).get("dynamic", []),
                },
                "target": model_info.get("target", "Multi-horizon price prediction"),
                "complexity": model_info.get("complexity", "Very High"),
                "advantages": model_info.get("advantages", []),
                "suitability": model_info.get("suitability", "Advanced"),
            })
        
        # Add derived features to results
        results["derived_features"] = derived_features
        results["data_profile"] = data_profile
        
    else:
        # Legacy fallback - hardcoded suggestions
        if has_prices and has_time_series:
            suggestions.append({
                "model_type": "Price Movement Prediction",
                "approach": "LSTM / Transformer",
                "description": "Predict price direction using time-series of ltp, batb, batl",
                "key_features": ["ltp", "batb", "batl", "tv"],
                "target": "Price direction (up/down) or price at T+n",
                "complexity": "High",
            })
        
        if has_prices and has_volume:
            suggestions.append({
                "model_type": "Market Microstructure Analysis",
                "approach": "Gradient Boosting (XGBoost/LightGBM)",
                "description": "Analyze order book dynamics and trading patterns",
                "key_features": ["batb", "batl", "trd", "tv", "spread"],
                "target": "Execution quality, market impact",
                "complexity": "Medium",
            })
        
        if has_prices:
            suggestions.append({
                "model_type": "Visual Price Patterns (Heat Map CNN)",
                "approach": "Convolutional Neural Network",
                "description": "Convert price ladders to images using Gramian Angular Field encoding",
                "key_features": ["batb", "batl", "ltp time series"],
                "target": "Pattern classification, price prediction",
                "complexity": "High",
                "note": "Novel approach with potential 40%+ improvement over traditional ML",
            })
        
        if has_market_definition:
            suggestions.append({
                "model_type": "Market Classification",
                "approach": "Random Forest / Neural Network",
                "description": "Classify market types, predict liquidity",
                "key_features": ["marketType", "numberOfActiveRunners", "venue", "countryCode"],
                "target": "Market category, expected volume",
                "complexity": "Low",
            })
    
    results["ml_suggestions"] = suggestions
    
    print("Analysis complete!")
    return results


def get_nested_value(obj: dict, path: str) -> Any:
    """Get value from nested dict using dot notation path."""
    parts = path.replace("[", ".").replace("]", "").split(".")
    current = obj
    
    for part in parts:
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            except (ValueError, IndexError):
                return None
        else:
            return None
        
        if current is None:
            return None
    
    return current


def format_duration(ms: int) -> str:
    """Format milliseconds as human-readable duration."""
    seconds = ms // 1000
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def infer_bq_type(python_type: str, sample_values: list) -> str:
    """Infer BigQuery type from Python type and sample values."""
    if "int" in python_type:
        return "INT64"
    elif "float" in python_type:
        return "FLOAT64"
    elif "bool" in python_type:
        return "BOOL"
    elif "array" in python_type:
        if "object" in python_type:
            return "RECORD"  # Needs struct definition
        elif "array" in python_type:
            return "STRING"  # Serialize nested arrays
        else:
            return "ARRAY<STRING>"
    elif "dict" in python_type:
        return "RECORD"
    else:
        # Check sample values
        if sample_values:
            first = sample_values[0]
            if isinstance(first, bool):
                return "BOOL"
            elif isinstance(first, int):
                return "INT64"
            elif isinstance(first, float):
                return "FLOAT64"
        return "STRING"


def sanitize_bq_field_name(path: str) -> str:
    """Convert field path to valid BigQuery column name."""
    # Replace dots and brackets with underscores
    name = re.sub(r'[\.\[\]]', '_', path)
    # Remove consecutive underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    # Ensure starts with letter
    if name and not name[0].isalpha():
        name = 'f_' + name
    return name[:128]  # BigQuery max column name length


# Streaming helpers for progress updates
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
