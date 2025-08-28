# City Import Bug Fix - Working Plan

## Problem Summary
When importing jobs from CSV, the `city` field is consistently `null` in the database, even though city information exists in the source CSV files.

## Root Cause Analysis

### Primary Issues:
1. **Multiple CSV Format Support**: The system handles two different CSV formats:
   - Format A: Uses `מקום עבודה` (work_location) 
   - Format B: Uses `שם ישוב` (city)

2. **Incomplete Field Mapping**: In `import_csv_enriched.py`, the script:
   - Correctly extracts city data from `work_location` 
   - Sets `city_canonical` field
   - **BUT NEVER sets the `city` field itself**

3. **Document Structure Inconsistency**: Database documents expect both:
   - `city`: The original city name (with spaces, readable format)
   - `city_canonical`: The canonical/normalized city name (lowercase, underscores)

### Technical Details:
- Line 157 in `import_csv_enriched.py`: `raw_city = (crow.get('work_location') or '').strip().replace('_',' ')`
- Line 159: `city_can = canonical_city(cleaned_city) if cleaned_city else None`
- Line 233: Document only sets `'city_canonical': city_can` 
- **Missing**: Document never sets `'city': cleaned_city`

## Solution Strategy

### Phase 1: Immediate Fix (Core Problem)
1. **Fix document creation in `import_csv_enriched.py`**:
   - Add `'city': cleaned_city` to the document structure
   - Ensure both `city` and `city_canonical` are set properly

2. **Test with current CSV format** to verify fix works

### Phase 2: Enhanced CSV Support 
1. **Improve header detection logic**:
   - Support both `מקום עבודה` and `שם ישוב` as city sources
   - Add fallback logic for different CSV formats

2. **Update header mapping** for comprehensive city field support

### Phase 3: Data Backfill & Validation
1. **Create backfill script** for existing records with missing city data
2. **Add validation** to prevent future occurrences
3. **Database integrity checks**

### Phase 4: Best Practices Implementation
1. **Unit tests** for city import functionality
2. **Documentation updates** for supported CSV formats
3. **Error handling** improvements for missing city data

## Implementation Steps

### Step 1: Core Fix
**File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/import_csv_enriched.py`

**Changes needed around line 233**:
```python
doc: Dict[str, Any] = {
    '_content_hash': content_hash,
    'title': title,
    'city': cleaned_city,  # ADD THIS LINE
    'city_canonical': city_can,
    # ... rest of fields
}
```

### Step 2: Enhanced City Field Resolution 
**Enhance the city extraction logic** to support multiple CSV formats:

```python
# Enhanced city field resolution
raw_city = ''
# Try multiple possible city field names
for city_field in ['city', 'work_location']:
    if crow.get(city_field):
        raw_city = crow.get(city_field, '').strip().replace('_',' ')
        break

if not raw_city:
    # Log warning for missing city data
    print(json.dumps({
        'level': 'warn', 'stage': 'row', 'error': 'missing_city_data',
        'order_id': order_id, 'title': title
    }, ensure_ascii=False))
```

### Step 3: Backfill Existing Data
**Create maintenance script** to fix existing records:

```python
def backfill_job_cities():
    """Fix existing jobs with missing city data"""
    coll = db['jobs']
    
    # Find jobs with missing city but have city_canonical or location data
    cursor = coll.find({
        '$or': [
            {'city': None}, 
            {'city': {'$exists': False}}
        ],
        '$or': [
            {'city_canonical': {'$ne': None}},
            {'text_blob': {'$regex': r'Location:', '$options': 'i'}}
        ]
    })
    
    updated = 0
    for job in cursor:
        # Extract city from various sources
        city = None
        
        # Try city_canonical first
        if job.get('city_canonical'):
            city = job['city_canonical'].replace('_', ' ').title()
        
        # Try extracting from text_blob
        elif job.get('text_blob'):
            location_match = re.search(r'Location:\s*([^\n]+)', job['text_blob'])
            if location_match:
                city = location_match.group(1).strip()
        
        if city:
            coll.update_one(
                {'_id': job['_id']}, 
                {'$set': {'city': city}}
            )
            updated += 1
    
    return updated
```

### Step 4: Validation & Testing
1. **Unit Tests**: Test both CSV formats
2. **Integration Tests**: Verify complete import pipeline
3. **Data Validation**: Ensure city data integrity

## Best Practices Applied

### 1. Defensive Programming
- Handle multiple CSV formats gracefully
- Fallback mechanisms for missing data
- Comprehensive error logging

### 2. Data Integrity
- Both `city` and `city_canonical` fields maintained
- Consistent city normalization
- Validation of city data sources

### 3. Backwards Compatibility  
- Support existing CSV formats
- Gradual migration approach
- No breaking changes to existing code

### 4. Monitoring & Observability
- Structured logging for debugging
- Metrics for city import success rates
- Clear error messages for troubleshooting

## Risk Mitigation

### 1. Data Loss Prevention
- Test on small datasets first
- Backup before bulk operations
- Rollback procedures documented

### 2. Performance Considerations
- Batch processing for large datasets
- Efficient database queries
- Memory usage optimization

### 3. Future-Proofing
- Flexible header mapping system
- Extensible CSV format support
- Modular city processing logic

## Success Criteria

1. ✅ All new job imports have valid `city` field
2. ✅ Existing jobs backfilled with city data where possible  
3. ✅ Support for multiple CSV formats (מקום עבודה and שם ישוב)
4. ✅ Comprehensive test coverage
5. ✅ Clear documentation and error handling
6. ✅ No regression in existing functionality

## Next Actions

1. **Immediate**: Implement core fix in `import_csv_enriched.py`
2. **Short-term**: Create and run backfill script for existing data
3. **Medium-term**: Add comprehensive testing and validation
4. **Long-term**: Enhance CSV format detection and support
