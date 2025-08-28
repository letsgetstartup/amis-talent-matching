# Job Import Testing Plan - Comprehensive Analysis & Debugging

## 📋 Overview
This is a comprehensive testing plan for the job import functionality in the TalentDB system. The plan covers testing the first 10 jobs from the CSV import, identifying broken imports, and investigating root causes using the system's complete logging infrastructure.

## 🔧 System Components Under Test

### Import Scripts
1. **`import_csv_enriched.py`** - Enhanced CSV job importer with:
   - PII scrubbing and full text preservation
   - Skill extraction (must_have/nice_to_have)
   - Synthetic enrichment targeting ≥12 skills
   - Mandatory requirements detection
   - External order ID upsert + versioning

2. **`import_jobs_csv.py`** - Basic CSV importer that:
   - Creates text files in samples/jobs
   - Handles Hebrew headers mapping
   - Supports ingestion flag

### Logging & Monitoring Systems
1. **Security Audit Logging** (`security_audit.py`)
2. **Outreach Failure Logging** (`api.py`)
3. **Failure Analysis** (`analyze_failures.py`)
4. **MongoDB Collections**:
   - `jobs` - Main job documents
   - `jobs_versions` - Version snapshots
   - `outreach_failures` - Import failure logs
   - `security_audit` - Security events
   - `_meta` - Import metrics

## 📊 Test Scenarios

### Phase 1: Pre-Import Analysis
- [ ] Validate CSV file structure and headers
- [ ] Check first 10 job records for data quality
- [ ] Verify Hebrew encoding and special characters
- [ ] Test PII detection patterns

### Phase 2: Individual Job Import Testing
- [ ] Test each of the first 10 jobs individually
- [ ] Monitor logging for each import attempt
- [ ] Capture skill extraction results
- [ ] Verify synthetic enrichment behavior
- [ ] Check mandatory requirement detection

### Phase 3: Batch Import Testing
- [ ] Import first 10 jobs as a batch
- [ ] Compare with individual results
- [ ] Monitor performance metrics
- [ ] Check for race conditions or conflicts

### Phase 4: Error Investigation
- [ ] Identify failed imports
- [ ] Analyze error patterns
- [ ] Check database state consistency
- [ ] Investigate root causes

### Phase 5: Data Quality Validation
- [ ] Verify job document structure
- [ ] Check skill set completeness
- [ ] Validate requirement categorization
- [ ] Confirm PII scrubbing effectiveness

## 🎯 Success Criteria

### Import Success Metrics
- ✅ All 10 jobs successfully imported to MongoDB
- ✅ Average skills per job ≥ 8
- ✅ Synthetic skill ratio within 20-40%
- ✅ Mandatory requirement detection rate ≥ 80%
- ✅ Zero PII leakage in stored documents

### Quality Metrics
- ✅ All jobs have valid Hebrew titles
- ✅ Job descriptions properly formatted
- ✅ Requirements properly categorized (must/nice)
- ✅ External order IDs preserved
- ✅ City names normalized (spaces, not underscores)

### Error Handling
- ✅ Graceful handling of malformed data
- ✅ Proper error logging to appropriate collections
- ✅ Rollback capability for failed imports
- ✅ Detailed error reporting with context

## 🔍 Key Test Cases

### Test Case 1: Header Mapping Validation
```python
Expected headers: מספר משרה, שם משרה, מקום עבודה, תאור תפקיד, דרישות התפקיד, מקצוע
Test: Verify all headers correctly mapped to English fields
```

### Test Case 2: Skill Extraction Accuracy
```python
Test job: 405627 (עובד/ת הרכבה)
Expected skills: assembly, mechanical_skills, technical_understanding, etc.
Verify: must_have vs nice_to_have categorization
```

### Test Case 3: Synthetic Enrichment Logic
```python
Test: Jobs with <12 initial skills get synthetic enrichment
Verify: Role pattern matching (מזכיר -> admin skills)
Check: Synthetic skill reasons are logged
```

### Test Case 4: PII Scrubbing
```python
Test: Email and phone number detection
Verify: [EMAIL] and [PHONE] replacements
Check: Original data not stored in database
```

### Test Case 5: Mandatory Detection
```python
Test keywords: חובה, דרישות חובה, must, required, mandatory
Verify: Lines containing these trigger mandatory categorization
Check: Mandatory requirements stored separately
```

## 🚨 Error Investigation Framework

### Error Categories
1. **Data Validation Errors**
   - Missing required fields
   - Invalid encoding
   - Malformed CSV structure

2. **Processing Errors**
   - Skill extraction failures
   - Synthetic enrichment issues
   - PII scrubbing problems

3. **Database Errors**
   - Connection issues
   - Index conflicts
   - Document validation failures

4. **Performance Issues**
   - Memory usage spikes
   - Processing timeouts
   - Resource conflicts

### Logging Analysis Points
- Import start/end timestamps
- Per-job processing time
- Memory usage patterns
- Error frequency and clustering
- Skill extraction statistics

## 📈 Metrics & Reporting

### Import Metrics (from _meta collection)
```json
{
  "jobs_ingested": 10,
  "avg_skills": 12.5,
  "synthetic_ratio": 0.30,
  "mandatory_detect_rate": 0.80,
  "duration_sec": 5.2
}
```

### Quality Flags Monitoring
- `low_quality_skills` - Jobs with <2 skills
- `mandatory_without_must_skills` - Logic inconsistency
- `over_generation` - >35 skills generated

### Performance Benchmarks
- Processing time: <1 second per job
- Memory usage: <100MB peak
- Database writes: <5 operations per job
- Error rate: <5% of total imports

## 🔧 Implementation Strategy

1. **Setup Phase**: Configure test environment and logging
2. **Data Preparation**: Extract first 10 jobs to test CSV
3. **Individual Testing**: Test each job separately with full logging
4. **Batch Testing**: Test all 10 jobs together
5. **Analysis Phase**: Investigate failures and patterns
6. **Reporting**: Generate comprehensive test report

This plan ensures thorough testing of the job import functionality while leveraging all available logging and monitoring systems to identify and resolve any import issues.
