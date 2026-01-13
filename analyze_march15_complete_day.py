#!/usr/bin/env python3
"""
Complete Analysis of March 15 2016 NDJSON Data
Processes ALL records (not sampled) to understand heterogeneity patterns
"""

import json
import pandas as pd
from collections import defaultdict
from datetime import datetime

# ============================================================================
# LOAD ALL DATA (NOT SAMPLED)
# ============================================================================

print("Loading all records from march_15_data_full.ndjson...")
print("(This may take a few minutes for 700MB file)")
print()

records = []
metadata_by_timestamp = defaultdict(list)

with open('march_15_data_full.ndjson') as f:
    for i, line in enumerate(f):
        try:
            record = json.loads(line)
            records.append(record)
            
            # Track metadata presence by timestamp
            ts = record.get('timestamp', 'unknown')
            has_venue = record.get('venue') is not None
            has_race_name = record.get('race_name') is not None
            has_event_name = record.get('event_name') is not None
            
            metadata_by_timestamp[ts].append({
                'has_venue': has_venue,
                'has_race_name': has_race_name,
                'has_event_name': has_event_name,
                'ltp': record.get('ltp') is not None,
                'batb': record.get('batb') is not None,
                'batl': record.get('batl') is not None,
                'trd': record.get('trd') is not None,
            })
        except json.JSONDecodeError as e:
            print(f"Warning: Line {i} is invalid JSON: {e}")
            continue
        
        if (i + 1) % 100000 == 0:
            print(f"  Loaded {i + 1} records...")

print()
print(f"✅ TOTAL RECORDS LOADED: {len(records):,}")
print()

# ============================================================================
# FIELD PRESENCE ANALYSIS
# ============================================================================

print("=" * 80)
print("FIELD PRESENCE ANALYSIS (% of records with non-null values)")
print("=" * 80)

df = pd.DataFrame(records)

field_presence = {}
for col in df.columns:
    null_count = df[col].isnull().sum()
    presence_pct = (1 - null_count / len(df)) * 100
    field_presence[col] = presence_pct
    
# Sort by presence (most to least)
sorted_fields = sorted(field_presence.items(), key=lambda x: x[1], reverse=True)

print(f"\n{'Field':<30} {'Presence %':<15} {'Null Count':<15}")
print("-" * 60)

for field, pct in sorted_fields:
    null_count = df[field].isnull().sum()
    status = "✅ COMPLETE" if pct == 100 else "⚠️  SPARSE" if pct < 50 else "✓  MOSTLY"
    print(f"{field:<30} {pct:>6.1f}%        {null_count:>12,}    {status}")

print()

# ============================================================================
# TEMPORAL ANALYSIS - Does metadata presence change throughout day?
# ============================================================================

print("=" * 80)
print("TEMPORAL ANALYSIS - Metadata presence throughout the day")
print("=" * 80)
print()

# Group by timestamp and analyze
temporal_patterns = []
for ts in sorted(metadata_by_timestamp.keys()):
    data = metadata_by_timestamp[ts]
    if len(data) == 0:
        continue
    
    venue_pct = sum(d['has_venue'] for d in data) / len(data) * 100
    race_pct = sum(d['has_race_name'] for d in data) / len(data) * 100
    event_pct = sum(d['has_event_name'] for d in data) / len(data) * 100
    ltp_pct = sum(d['ltp'] for d in data) / len(data) * 100
    batb_pct = sum(d['batb'] for d in data) / len(data) * 100
    trd_pct = sum(d['trd'] for d in data) / len(data) * 100
    
    temporal_patterns.append({
        'timestamp': ts,
        'count': len(data),
        'venue_pct': venue_pct,
        'race_name_pct': race_pct,
        'event_name_pct': event_pct,
        'ltp_pct': ltp_pct,
        'batb_pct': batb_pct,
        'trd_pct': trd_pct,
    })

# Show first 10, middle 10, last 10 timestamps
print(f"Showing FIRST 10 timestamps:")
print(f"{'Timestamp':<30} {'Venue%':<10} {'Race%':<10} {'LTP%':<10} {'BATB%':<10} {'TRD%':<10} Count")
print("-" * 100)
for pattern in temporal_patterns[:10]:
    print(f"{pattern['timestamp']:<30} {pattern['venue_pct']:>6.0f}%   "
          f"{pattern['race_name_pct']:>6.0f}%   {pattern['ltp_pct']:>6.0f}%   "
          f"{pattern['batb_pct']:>6.0f}%   {pattern['trd_pct']:>6.0f}%   "
          f"{pattern['count']:>6}")

print()
print(f"Showing MIDDLE timestamps (around {len(temporal_patterns)//2}):")
start_idx = max(0, len(temporal_patterns)//2 - 5)
end_idx = min(len(temporal_patterns), len(temporal_patterns)//2 + 5)
for pattern in temporal_patterns[start_idx:end_idx]:
    print(f"{pattern['timestamp']:<30} {pattern['venue_pct']:>6.0f}%   "
          f"{pattern['race_name_pct']:>6.0f}%   {pattern['ltp_pct']:>6.0f}%   "
          f"{pattern['batb_pct']:>6.0f}%   {pattern['trd_pct']:>6.0f}%   "
          f"{pattern['count']:>6}")

print()
print(f"Showing LAST 10 timestamps:")
for pattern in temporal_patterns[-10:]:
    print(f"{pattern['timestamp']:<30} {pattern['venue_pct']:>6.0f}%   "
          f"{pattern['race_name_pct']:>6.0f}%   {pattern['ltp_pct']:>6.0f}%   "
          f"{pattern['batb_pct']:>6.0f}%   {pattern['trd_pct']:>6.0f}%   "
          f"{pattern['count']:>6}")

print()

# ============================================================================
# RECORD TYPE EXAMPLES
# ============================================================================

print("=" * 80)
print("EXAMPLE RECORDS - Different patterns found in data")
print("=" * 80)
print()

# Example 1: Record with all metadata
print("EXAMPLE 1: Record with ALL metadata fields populated")
print("-" * 80)
for record in records:
    if (record.get('venue') and record.get('race_name') and 
        record.get('event_name') and record.get('ltp')):
        print(json.dumps(record, indent=2, default=str))
        break

print()

# Example 2: Record with NO metadata (just pricing)
print("EXAMPLE 2: Record with NO metadata (just pricing updates)")
print("-" * 80)
for record in records:
    if (not record.get('venue') and not record.get('race_name') and 
        record.get('ltp') is not None):
        print(json.dumps(record, indent=2, default=str))
        break

print()

# Example 3: Record with ADVANCED tier pricing ladder
print("EXAMPLE 3: Record with ADVANCED tier pricing ladder (batb/batl/trd)")
print("-" * 80)
for record in records:
    if record.get('batb') and record.get('batl') and record.get('trd'):
        # Truncate the arrays for readability
        record_copy = record.copy()
        record_copy['batb'] = record_copy['batb'][:3]  # First 3 elements
        record_copy['batl'] = record_copy['batl'][:3]
        record_copy['trd'] = record_copy['trd'][:3]
        print(json.dumps(record_copy, indent=2, default=str))
        break

print()

# ============================================================================
# FIELD VALUE DISTRIBUTION
# ============================================================================

print("=" * 80)
print("FIELD VALUE STATISTICS")
print("=" * 80)
print()

print("Market Type Distribution:")
market_types = df['market_type'].value_counts()
for mtype, count in market_types.items():
    pct = count / len(df) * 100
    print(f"  {mtype:<20} {count:>10,} records ({pct:>5.1f}%)")

print()
print("Country Code Distribution:")
countries = df['country_code'].value_counts()
for country, count in countries.items():
    pct = count / len(df) * 100
    print(f"  {country:<20} {count:>10,} records ({pct:>5.1f}%)")

print()
print("Numeric Field Statistics:")
for field in ['ltp', 'back_price', 'lay_price', 'back_volume', 'lay_volume', 'total_matched_volume']:
    if field in df.columns:
        stats = df[field].describe()
        print(f"\n{field}:")
        print(f"  Count: {stats['count']:,.0f}")
        print(f"  Mean:  {stats['mean']:.2f}")
        print(f"  Std:   {stats['std']:.2f}")
        print(f"  Min:   {stats['min']:.2f}")
        print(f"  Max:   {stats['max']:.2f}")

print()

# ============================================================================
# RECOMMENDATIONS FOR SCHEMA
# ============================================================================

print("=" * 80)
print("SCHEMA DESIGN RECOMMENDATIONS")
print("=" * 80)
print()

print("MANDATORY FIELDS (100% presence):")
mandatory = [field for field, pct in sorted_fields if pct == 100]
for field in mandatory:
    print(f"  ✅ {field:<30} NOT NULL")

print()
print("MOSTLY COMPLETE (>95% presence):")
mostly = [field for field, pct in sorted_fields if 95 <= pct < 100]
for field in mostly:
    pct = field_presence[field]
    print(f"  ✓ {field:<30} NULLABLE (but almost always present)")

print()
print("SPARSE FIELDS (<50% presence):")
sparse = [field for field, pct in sorted_fields if pct < 50]
for field in sparse:
    pct = field_presence[field]
    print(f"  ⚠️  {field:<30} NULLABLE (only {pct:.1f}% of records)")

print()

# ============================================================================
# SAVE ANALYSIS RESULTS
# ============================================================================

print("=" * 80)
print("EXPORTING RESULTS")
print("=" * 80)
print()

# Save field presence to CSV
presence_df = pd.DataFrame(
    [(field, pct) for field, pct in sorted_fields],
    columns=['field', 'presence_pct']
)
presence_df.to_csv('field_presence_analysis.csv', index=False)
print(f"✅ Saved: field_presence_analysis.csv")

# Save temporal patterns to CSV
temporal_df = pd.DataFrame(temporal_patterns)
temporal_df.to_csv('temporal_patterns.csv', index=False)
print(f"✅ Saved: temporal_patterns.csv")

# Save sample records
with open('sample_records_all_types.ndjson', 'w') as f:
    for record in records[:1000]:  # First 1000 records
        f.write(json.dumps(record, default=str) + '\n')
print(f"✅ Saved: sample_records_all_types.ndjson (first 1000 records)")

print()
print("=" * 80)
print("✅ ANALYSIS COMPLETE")
print("=" * 80)
