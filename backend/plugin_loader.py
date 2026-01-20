"""
Plugin Loader for CHIMERA Analysis

Loads field definitions, categories, validation rules, and ML recommendations
from external plugin files stored in GCS.

Plugin Structure:
  gs://betfair-chimera-plugins/{provider}/
  ├── manifest.json        # Plugin metadata
  ├── fields.json          # Field definitions
  ├── categories.json      # Category definitions
  ├── validation.json      # Validation rules
  ├── ml_recommendations.json  # ML model suggestions
  └── bigquery.json        # BigQuery schema recommendations
"""

import json
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from google.cloud import storage
import logging

logger = logging.getLogger(__name__)

# Configuration
PLUGINS_BUCKET = os.environ.get("CHIMERA_PLUGINS_BUCKET", "betfair-chimera-plugins")
DEFAULT_PLUGIN = os.environ.get("CHIMERA_DEFAULT_PLUGIN", "betfair")

# Check multiple local paths for plugins
LOCAL_PLUGIN_PATHS = [
    os.environ.get("CHIMERA_LOCAL_PLUGINS", "/app/plugins"),
    "/app/plugins",
    "./plugins",
    os.path.join(os.path.dirname(__file__), "plugins"),
]

# Cache for loaded plugins
_plugin_cache: Dict[str, 'Plugin'] = {}


@dataclass
class FieldDefinition:
    """Definition for a single field."""
    key: str
    full_name: str
    type: str
    category: str
    description: str
    required: bool = False
    ml_relevance: str = "low"
    ml_notes: str = ""
    values: Dict[str, str] = field(default_factory=dict)
    structure: Dict[str, Any] = field(default_factory=dict)
    filter_flag: str = ""
    contexts: List[str] = field(default_factory=list)
    common_errors: Dict[str, str] = field(default_factory=dict)
    bigquery: Dict[str, str] = field(default_factory=dict)
    format: str = ""
    unit: str = ""
    default: Any = None


@dataclass
class Category:
    """Definition for a field category."""
    name: str
    icon: str
    color: str
    description: str
    fields_pattern: List[str] = field(default_factory=list)


@dataclass
class Plugin:
    """Complete plugin with all definitions."""
    plugin_id: str
    name: str
    version: str
    description: str
    
    fields: Dict[str, FieldDefinition] = field(default_factory=dict)
    categories: Dict[str, Category] = field(default_factory=dict)
    category_priority: List[str] = field(default_factory=list)
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    ml_recommendations: Dict[str, Any] = field(default_factory=dict)
    bigquery_config: Dict[str, Any] = field(default_factory=dict)
    derived_features: Dict[str, Any] = field(default_factory=dict)


def get_storage_client():
    """Get GCS storage client."""
    try:
        return storage.Client()
    except Exception as e:
        logger.warning(f"Could not create GCS client: {e}")
        return None


def load_json_from_gcs(bucket_name: str, blob_path: str) -> Optional[Dict]:
    """Load JSON file from GCS."""
    try:
        client = get_storage_client()
        if not client:
            return None
        
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        if blob.exists():
            content = blob.download_as_text()
            return json.loads(content)
        else:
            logger.warning(f"Blob not found: gs://{bucket_name}/{blob_path}")
            return None
    except Exception as e:
        logger.error(f"Error loading from GCS: {e}")
        return None


def load_json_from_local(file_path: str) -> Optional[Dict]:
    """Load JSON file from local filesystem."""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        return None
    except Exception as e:
        logger.error(f"Error loading local file {file_path}: {e}")
        return None


def find_local_plugin_file(plugin_id: str, filename: str) -> Optional[str]:
    """Find a plugin file in any of the local plugin paths."""
    for base_path in LOCAL_PLUGIN_PATHS:
        file_path = os.path.join(base_path, plugin_id, filename)
        if os.path.exists(file_path):
            return file_path
    return None


def load_plugin_file(plugin_id: str, filename: str) -> Optional[Dict]:
    """Load a plugin file, trying GCS first then local paths."""
    # Try GCS first
    gcs_path = f"{plugin_id}/{filename}"
    data = load_json_from_gcs(PLUGINS_BUCKET, gcs_path)
    
    if data:
        logger.info(f"Loaded {filename} from GCS for plugin {plugin_id}")
        return data
    
    # Fall back to local paths
    local_path = find_local_plugin_file(plugin_id, filename)
    if local_path:
        data = load_json_from_local(local_path)
        if data:
            logger.info(f"Loaded {filename} from local ({local_path}) for plugin {plugin_id}")
            return data
    
    logger.warning(f"Could not load {filename} for plugin {plugin_id}")
    return None


def parse_fields(fields_data: Dict) -> Dict[str, FieldDefinition]:
    """Parse fields.json into FieldDefinition objects."""
    fields = {}
    
    raw_fields = fields_data.get("fields", fields_data)
    
    for key, data in raw_fields.items():
        if key.startswith("_"):  # Skip metadata
            continue
        
        fields[key] = FieldDefinition(
            key=key,
            full_name=data.get("full_name", key),
            type=data.get("type", "unknown"),
            category=data.get("category", "Unknown"),
            description=data.get("description", ""),
            required=data.get("required", False),
            ml_relevance=data.get("ml_relevance", "low"),
            ml_notes=data.get("ml_notes", ""),
            values=data.get("values", {}),
            structure=data.get("structure", {}),
            filter_flag=data.get("filter_flag", ""),
            contexts=data.get("contexts", []),
            common_errors=data.get("common_errors", {}),
            bigquery=data.get("bigquery", {}),
            format=data.get("format", ""),
            unit=data.get("unit", ""),
            default=data.get("default"),
        )
    
    return fields


def parse_categories(categories_data: Dict) -> tuple[Dict[str, Category], List[str]]:
    """Parse categories.json into Category objects."""
    categories = {}
    
    raw_categories = categories_data.get("categories", {})
    priority = categories_data.get("category_priority", [])
    
    for name, data in raw_categories.items():
        categories[name] = Category(
            name=name,
            icon=data.get("icon", "📁"),
            color=data.get("color", "#8B5CF6"),
            description=data.get("description", ""),
            fields_pattern=data.get("fields_pattern", []),
        )
    
    return categories, priority


def load_plugin(plugin_id: str = None, force_reload: bool = False) -> Plugin:
    """
    Load a complete plugin with all its definitions.
    
    Args:
        plugin_id: Plugin identifier (e.g., 'betfair')
        force_reload: If True, bypass cache and reload from source
    
    Returns:
        Plugin object with all definitions
    """
    if plugin_id is None:
        plugin_id = DEFAULT_PLUGIN
    
    # Check cache
    if not force_reload and plugin_id in _plugin_cache:
        return _plugin_cache[plugin_id]
    
    logger.info(f"Loading plugin: {plugin_id}")
    
    # Load manifest
    manifest = load_plugin_file(plugin_id, "manifest.json") or {}
    
    # Load fields
    fields_data = load_plugin_file(plugin_id, "fields.json") or {"fields": {}}
    fields = parse_fields(fields_data)
    
    # Load categories
    categories_data = load_plugin_file(plugin_id, "categories.json") or {"categories": {}}
    categories, category_priority = parse_categories(categories_data)
    
    # Load validation rules
    validation_data = load_plugin_file(plugin_id, "validation.json") or {}
    
    # Load ML recommendations
    ml_data = load_plugin_file(plugin_id, "ml_recommendations.json") or {}
    
    # Load BigQuery config
    bq_data = load_plugin_file(plugin_id, "bigquery.json") or {}
    
    # Create plugin
    plugin = Plugin(
        plugin_id=manifest.get("plugin_id", plugin_id),
        name=manifest.get("name", plugin_id),
        version=manifest.get("version", "0.0.0"),
        description=manifest.get("description", ""),
        fields=fields,
        categories=categories,
        category_priority=category_priority,
        validation_rules=validation_data.get("field_validations", {}),
        ml_recommendations=ml_data.get("recommended_models", {}),
        bigquery_config=bq_data,
        derived_features=ml_data.get("derived_features", {}),
    )
    
    # Cache it
    _plugin_cache[plugin_id] = plugin
    
    logger.info(f"Loaded plugin {plugin_id}: {len(fields)} fields, {len(categories)} categories")
    
    return plugin


def get_field_info(field_key: str, plugin_id: str = None, context: str = None) -> Dict[str, Any]:
    """
    Get information about a specific field.
    
    Args:
        field_key: Field name (e.g., 'ltp', 'batb', 'pt')
        plugin_id: Plugin to use
        context: Optional context (e.g., 'rc[]', 'marketDefinition')
    
    Returns:
        Dictionary with field information
    """
    plugin = load_plugin(plugin_id)
    
    # Try exact match first
    if field_key in plugin.fields:
        field_def = plugin.fields[field_key]
        return {
            "key": field_def.key,
            "name": field_def.full_name,
            "type": field_def.type,
            "category": field_def.category,
            "description": field_def.description,
            "ml_relevance": field_def.ml_relevance,
            "ml_notes": field_def.ml_notes,
            "values": field_def.values,
            "structure": field_def.structure,
            "filter_flag": field_def.filter_flag,
            "format": field_def.format,
            "unit": field_def.unit,
        }
    
    # Try with context prefix (e.g., 'rc[].ltp' -> check for 'ltp')
    base_key = field_key.split('.')[-1].replace('[]', '')
    if base_key in plugin.fields:
        field_def = plugin.fields[base_key]
        return {
            "key": base_key,
            "name": field_def.full_name,
            "type": field_def.type,
            "category": field_def.category,
            "description": field_def.description,
            "ml_relevance": field_def.ml_relevance,
            "ml_notes": field_def.ml_notes,
            "matched_from": base_key,
            "original_path": field_key,
        }
    
    # Check for array index patterns (mc[0], rc[1], etc.)
    import re
    cleaned_key = re.sub(r'\[\d+\]', '[]', field_key)
    cleaned_base = cleaned_key.split('.')[-1].replace('[]', '')
    
    if cleaned_base in plugin.fields:
        field_def = plugin.fields[cleaned_base]
        return {
            "key": cleaned_base,
            "name": field_def.full_name,
            "type": field_def.type,
            "category": field_def.category,
            "description": field_def.description,
            "ml_relevance": field_def.ml_relevance,
            "matched_from": cleaned_base,
            "original_path": field_key,
        }
    
    # Unknown field
    return {
        "key": field_key,
        "name": field_key.title().replace('_', ' '),
        "type": "unknown",
        "category": "Unknown",
        "description": f"Unknown field: {field_key}",
        "ml_relevance": "unknown",
    }


def get_category_for_field(field_key: str, plugin_id: str = None) -> str:
    """Get the category name for a field."""
    info = get_field_info(field_key, plugin_id)
    return info.get("category", "Unknown")


def get_category_info(category_name: str, plugin_id: str = None) -> Dict[str, Any]:
    """Get information about a category."""
    plugin = load_plugin(plugin_id)
    
    if category_name in plugin.categories:
        cat = plugin.categories[category_name]
        return {
            "name": cat.name,
            "icon": cat.icon,
            "color": cat.color,
            "description": cat.description,
        }
    
    return {
        "name": category_name,
        "icon": "❓",
        "color": "#6B7280",
        "description": f"Unknown category: {category_name}",
    }


def get_all_categories(plugin_id: str = None) -> Dict[str, Dict[str, Any]]:
    """Get all categories with their metadata."""
    plugin = load_plugin(plugin_id)
    
    result = {}
    for name, cat in plugin.categories.items():
        result[name] = {
            "name": cat.name,
            "icon": cat.icon,
            "color": cat.color,
            "description": cat.description,
        }
    
    return result


def get_ml_recommendations(plugin_id: str = None) -> Dict[str, Any]:
    """Get ML model recommendations from plugin."""
    plugin = load_plugin(plugin_id)
    return plugin.ml_recommendations


def get_derived_features(plugin_id: str = None) -> Dict[str, Any]:
    """Get derived feature definitions from plugin."""
    plugin = load_plugin(plugin_id)
    return plugin.derived_features


def get_bigquery_config(plugin_id: str = None) -> Dict[str, Any]:
    """Get BigQuery configuration from plugin."""
    plugin = load_plugin(plugin_id)
    return plugin.bigquery_config


def get_validation_rules(plugin_id: str = None) -> Dict[str, Any]:
    """Get validation rules from plugin."""
    plugin = load_plugin(plugin_id)
    return plugin.validation_rules


def list_available_plugins() -> List[Dict[str, str]]:
    """List all available plugins."""
    plugins = []
    found_ids = set()
    
    # Check GCS
    try:
        client = get_storage_client()
        if client:
            bucket = client.bucket(PLUGINS_BUCKET)
            blobs = bucket.list_blobs(delimiter='/')
            
            # Get prefixes (directories)
            prefixes = set()
            for blob in blobs:
                if '/' in blob.name:
                    prefix = blob.name.split('/')[0]
                    prefixes.add(prefix)
            
            for prefix in prefixes:
                manifest = load_json_from_gcs(PLUGINS_BUCKET, f"{prefix}/manifest.json")
                if manifest:
                    plugin_id = manifest.get("plugin_id", prefix)
                    if plugin_id not in found_ids:
                        plugins.append({
                            "plugin_id": plugin_id,
                            "name": manifest.get("name", prefix),
                            "version": manifest.get("version", "unknown"),
                            "source": "gcs",
                        })
                        found_ids.add(plugin_id)
    except Exception as e:
        logger.warning(f"Could not list GCS plugins: {e}")
    
    # Check all local paths
    for local_path in LOCAL_PLUGIN_PATHS:
        if os.path.exists(local_path):
            for item in os.listdir(local_path):
                item_path = os.path.join(local_path, item)
                if os.path.isdir(item_path):
                    manifest_path = os.path.join(item_path, "manifest.json")
                    if os.path.exists(manifest_path):
                        manifest = load_json_from_local(manifest_path)
                        if manifest:
                            plugin_id = manifest.get("plugin_id", item)
                            if plugin_id not in found_ids:
                                plugins.append({
                                    "plugin_id": plugin_id,
                                    "name": manifest.get("name", item),
                                    "version": manifest.get("version", "unknown"),
                                    "source": f"local:{local_path}",
                                })
                                found_ids.add(plugin_id)
    
    return plugins


# Convenience function for backward compatibility
def get_all_known_fields(plugin_id: str = None) -> Dict[str, Dict[str, Any]]:
    """Get all field definitions as a dictionary (backward compatible)."""
    plugin = load_plugin(plugin_id)
    
    result = {}
    for key, field_def in plugin.fields.items():
        result[key] = {
            "name": field_def.full_name,
            "type": field_def.type,
            "category": field_def.category,
            "description": field_def.description,
            "ml_relevance": field_def.ml_relevance,
        }
    
    return result
