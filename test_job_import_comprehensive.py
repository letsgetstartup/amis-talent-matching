#!/usr/bin/env python3
"""
Comprehensive Job Import Test Suite
==================================
Tests the first 10 jobs from jobs_import.csv with full logging and error analysis.

Usage:
    python test_job_import_comprehensive.py [--verbose] [--single-job=JOB_ID]

Features:
    - Individual job testing with detailed logging
    - Batch import testing
    - Error pattern analysis
    - Performance monitoring
    - Data quality validation
    - PII scrubbing verification
    - Skill extraction analysis
    - Synthetic enrichment testing
"""

import sys
import os
import csv
import time
import json
import tempfile
import traceback
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add talentdb to path
ROOT = Path(__file__).resolve().parent
TALENTDB_PATH = ROOT / "talentdb"
sys.path.insert(0, str(TALENTDB_PATH))

# Import system modules
try:
    from scripts.ingest_agent import db
    from scripts.import_csv_enriched import main as import_enriched_main
    from scripts.import_jobs_csv import import_csv as import_basic_csv
    from scripts.security_audit import audit_log, log_data_access
except ImportError as e:
    print(f"⚠️ Warning: Could not import some modules: {e}")
    print("Make sure you're running from the project root directory")
    # Create mock functions for testing
    db = None
    import_enriched_main = lambda x: 0
    import_basic_csv = lambda x, y: []
    audit_log = lambda **kwargs: None
    log_data_access = lambda **kwargs: None

class JobImportTester:
    """Comprehensive job import testing framework"""
    
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.test_results = {}
        self.error_log = []
        self.performance_metrics = {}
        self.quality_issues = []
        
        # Setup test logging
        self.start_time = time.time()
        self.log(f"🔧 Job Import Tester initialized at {datetime.now().isoformat()}")
        
        # Clear previous test artifacts
        self._cleanup_test_data()
    
    def log(self, message: str, level: str = "INFO"):
        """Enhanced logging with timestamps and levels"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = {"INFO": "ℹ️", "WARN": "⚠️", "ERROR": "🚨", "SUCCESS": "✅"}
        print(f"[{timestamp}] {prefix.get(level, '📝')} {message}")
        
        if self.verbose and level in ["WARN", "ERROR"]:
            # Also log to audit system
            try:
                audit_log(
                    tenant_id="test_tenant",
                    action="import_test_log",
                    resource="job_import",
                    resource_id="test_session",
                    success=level != "ERROR",
                    details={"message": message, "level": level}
                )
            except Exception:
                pass  # Don't let audit logging break tests
    
    def _cleanup_test_data(self):
        """Clean up any previous test data"""
        try:
            # Remove test jobs (with specific test markers)
            result = db['jobs'].delete_many({
                '$or': [
                    {'external_order_id': {'$in': ['405627', '405620', '405626']}},
                    {'title': {'$regex': 'TEST_IMPORT_.*'}}
                ]
            })
            if result.deleted_count > 0:
                self.log(f"Cleaned up {result.deleted_count} test jobs")
            
            # Clean test logs
            db['outreach_failures'].delete_many({'stage': 'test_import'})
            
        except Exception as e:
            self.log(f"Cleanup warning: {e}", "WARN")
    
    def extract_first_10_jobs(self, csv_path: str) -> str:
        """Extract first 10 jobs to a test CSV file"""
        self.log("📄 Extracting first 10 jobs from CSV...")
        
        test_csv_path = ROOT / "test_first_10_jobs.csv"
        
        with open(csv_path, 'r', encoding='utf-8') as source:
            reader = csv.reader(source)
            headers = next(reader)
            
            with open(test_csv_path, 'w', encoding='utf-8', newline='') as target:
                writer = csv.writer(target)
                writer.writerow(headers)
                
                job_count = 0
                for row in reader:
                    if job_count >= 10:
                        break
                    writer.writerow(row)
                    job_count += 1
        
        self.log(f"✅ Created test CSV with {job_count} jobs: {test_csv_path}")
        return str(test_csv_path)
    
    def analyze_csv_structure(self, csv_path: str) -> Dict[str, Any]:
        """Analyze CSV structure and data quality"""
        self.log("🔍 Analyzing CSV structure...")
        
        analysis = {
            'headers': [],
            'total_rows': 0,
            'empty_fields': defaultdict(int),
            'field_lengths': defaultdict(list),
            'encoding_issues': [],
            'pii_detected': []
        }
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                analysis['headers'] = reader.fieldnames
                
                for row_num, row in enumerate(reader, 1):
                    analysis['total_rows'] = row_num
                    
                    for field, value in row.items():
                        if not value or not value.strip():
                            analysis['empty_fields'][field] += 1
                        else:
                            analysis['field_lengths'][field].append(len(value))
                            
                            # Check for potential PII
                            if '@' in value and '.' in value:
                                analysis['pii_detected'].append(f"Row {row_num}, field {field}: potential email")
                            if any(char.isdigit() for char in value) and len([c for c in value if c.isdigit()]) >= 7:
                                analysis['pii_detected'].append(f"Row {row_num}, field {field}: potential phone")
                    
                    if row_num >= 10:  # Only analyze first 10 for this test
                        break
        
        except Exception as e:
            self.log(f"CSV analysis error: {e}", "ERROR")
            analysis['error'] = str(e)
        
        # Calculate averages
        for field, lengths in analysis['field_lengths'].items():
            if lengths:
                analysis['field_lengths'][field] = {
                    'avg': sum(lengths) / len(lengths),
                    'max': max(lengths),
                    'min': min(lengths)
                }
        
        return analysis
    
    def test_individual_job(self, job_data: Dict[str, str], job_index: int) -> Dict[str, Any]:
        """Test importing a single job with detailed monitoring"""
        job_id = job_data.get('מספר משרה', f'test_job_{job_index}')
        title = job_data.get('שם משרה', 'Unknown Title')
        
        self.log(f"🔬 Testing individual job: {job_id} - {title}")
        
        result = {
            'job_id': job_id,
            'title': title,
            'success': False,
            'errors': [],
            'warnings': [],
            'processing_time': 0,
            'skills_extracted': 0,
            'synthetic_skills': 0,
            'mandatory_detected': False,
            'pii_scrubbed': False,
            'db_document': None
        }
        
        start_time = time.time()
        
        try:
            # Create temporary single-job CSV
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.csv', delete=False) as tmp:
                writer = csv.DictWriter(tmp, fieldnames=job_data.keys())
                writer.writeheader()
                writer.writerow(job_data)
                tmp_path = tmp.name
            
            # Monitor before state
            before_count = db['jobs'].count_documents({})
            
            # Import using enriched importer
            try:
                import_result = import_enriched_main(tmp_path)
                if import_result == 0:  # Success return code
                    result['success'] = True
            except Exception as e:
                result['errors'].append(f"Import failed: {str(e)}")
                self.log(f"Import error for {job_id}: {e}", "ERROR")
            
            # Check database state
            after_count = db['jobs'].count_documents({})
            if after_count > before_count:
                # Find the new document
                new_doc = db['jobs'].find_one({'external_order_id': job_id})
                if new_doc:
                    result['db_document'] = new_doc
                    result['skills_extracted'] = len(new_doc.get('skill_set', []))
                    result['synthetic_skills'] = len(new_doc.get('synthetic_skills', []))
                    result['mandatory_detected'] = len(new_doc.get('mandatory_requirements', [])) > 0
                    result['pii_scrubbed'] = '[EMAIL]' in str(new_doc) or '[PHONE]' in str(new_doc)
                    
                    # Quality checks
                    if result['skills_extracted'] < 3:
                        result['warnings'].append(f"Low skill count: {result['skills_extracted']}")
                    
                    if not new_doc.get('title'):
                        result['errors'].append("Missing job title in DB")
                    
                    flags = new_doc.get('flags', [])
                    if flags:
                        result['warnings'].extend([f"Quality flag: {flag}" for flag in flags])
            
            # Clean up temp file
            os.unlink(tmp_path)
            
        except Exception as e:
            result['errors'].append(f"Test framework error: {str(e)}")
            self.log(f"Test framework error: {e}", "ERROR")
            if self.verbose:
                result['errors'].append(f"Traceback: {traceback.format_exc()}")
        
        result['processing_time'] = time.time() - start_time
        
        # Log results
        if result['success']:
            self.log(f"✅ Job {job_id} imported successfully in {result['processing_time']:.2f}s")
        else:
            self.log(f"❌ Job {job_id} import failed: {'; '.join(result['errors'])}", "ERROR")
        
        return result
    
    def test_batch_import(self, csv_path: str) -> Dict[str, Any]:
        """Test batch import of all jobs"""
        self.log("📦 Testing batch import...")
        
        result = {
            'success': False,
            'total_jobs': 0,
            'imported_jobs': 0,
            'processing_time': 0,
            'errors': [],
            'metrics': {}
        }
        
        start_time = time.time()
        before_count = db['jobs'].count_documents({})
        
        try:
            # Import using enriched importer
            import_result = import_enriched_main(csv_path)
            if import_result == 0:
                result['success'] = True
            
            # Check results
            after_count = db['jobs'].count_documents({})
            result['imported_jobs'] = after_count - before_count
            
            # Get metrics from _meta collection
            meta_doc = db['_meta'].find_one({'key': 'last_import_metrics'})
            if meta_doc:
                result['metrics'] = meta_doc.get('value', {})
        
        except Exception as e:
            result['errors'].append(str(e))
            self.log(f"Batch import error: {e}", "ERROR")
        
        result['processing_time'] = time.time() - start_time
        return result
    
    def analyze_import_failures(self) -> Dict[str, Any]:
        """Analyze failures using the existing failure analysis system"""
        self.log("🔍 Analyzing import failures...")
        
        # Get all failures
        failures = list(db['outreach_failures'].find().sort('ts', -1))
        
        analysis = {
            'total_failures': len(failures),
            'recent_failures': 0,
            'stage_breakdown': Counter(),
            'error_patterns': Counter(),
            'samples': []
        }
        
        cutoff_time = self.start_time
        
        for failure in failures:
            if failure.get('ts', 0) >= cutoff_time:
                analysis['recent_failures'] += 1
            
            stage = failure.get('stage', 'unknown')
            analysis['stage_breakdown'][stage] += 1
            
            error = failure.get('error', '')[:100]  # Truncate long errors
            analysis['error_patterns'][error] += 1
            
            if len(analysis['samples']) < 5:
                analysis['samples'].append({
                    'stage': stage,
                    'error': error,
                    'timestamp': failure.get('ts'),
                    'candidate_id': failure.get('candidate_id'),
                    'job_ids': failure.get('job_ids')
                })
        
        return analysis
    
    def generate_comprehensive_report(self, individual_results: List[Dict], batch_result: Dict, failure_analysis: Dict, csv_analysis: Dict):
        """Generate comprehensive test report"""
        self.log("📊 Generating comprehensive test report...")
        
        report = {
            'test_session': {
                'start_time': datetime.fromtimestamp(self.start_time).isoformat(),
                'end_time': datetime.now().isoformat(),
                'duration_seconds': time.time() - self.start_time,
                'total_tests': len(individual_results) + 1  # +1 for batch test
            },
            'csv_analysis': csv_analysis,
            'individual_job_results': individual_results,
            'batch_import_result': batch_result,
            'failure_analysis': failure_analysis,
            'summary': self._generate_summary(individual_results, batch_result),
            'recommendations': self._generate_recommendations(individual_results, batch_result, failure_analysis)
        }
        
        return report
    
    def _generate_summary(self, individual_results: List[Dict], batch_result: Dict) -> Dict[str, Any]:
        """Generate test summary statistics"""
        individual_success = sum(1 for r in individual_results if r['success'])
        
        return {
            'individual_tests': {
                'total': len(individual_results),
                'passed': individual_success,
                'failed': len(individual_results) - individual_success,
                'success_rate': individual_success / len(individual_results) if individual_results else 0
            },
            'batch_test': {
                'success': batch_result['success'],
                'jobs_imported': batch_result.get('imported_jobs', 0),
                'processing_time': batch_result.get('processing_time', 0)
            },
            'quality_metrics': {
                'avg_skills': sum(r.get('skills_extracted', 0) for r in individual_results) / len(individual_results) if individual_results else 0,
                'avg_synthetic': sum(r.get('synthetic_skills', 0) for r in individual_results) / len(individual_results) if individual_results else 0,
                'mandatory_detection_rate': sum(1 for r in individual_results if r.get('mandatory_detected')) / len(individual_results) if individual_results else 0,
                'pii_scrubbing_rate': sum(1 for r in individual_results if r.get('pii_scrubbed')) / len(individual_results) if individual_results else 0
            }
        }
    
    def _generate_recommendations(self, individual_results: List[Dict], batch_result: Dict, failure_analysis: Dict) -> List[str]:
        """Generate recommendations based on test results"""
        recommendations = []
        
        # Check success rates
        individual_success_rate = sum(1 for r in individual_results if r['success']) / len(individual_results) if individual_results else 0
        if individual_success_rate < 0.8:
            recommendations.append(f"🚨 Low individual import success rate ({individual_success_rate:.1%}). Review error patterns.")
        
        # Check quality metrics
        avg_skills = sum(r.get('skills_extracted', 0) for r in individual_results) / len(individual_results) if individual_results else 0
        if avg_skills < 8:
            recommendations.append(f"⚠️ Average skills per job ({avg_skills:.1f}) below target (8+). Review skill extraction logic.")
        
        # Check for common errors
        common_errors = Counter()
        for result in individual_results:
            for error in result.get('errors', []):
                common_errors[error[:50]] += 1
        
        if common_errors:
            most_common = common_errors.most_common(1)[0]
            if most_common[1] > 1:
                recommendations.append(f"🔧 Recurring error pattern: '{most_common[0]}' ({most_common[1]} occurrences)")
        
        # Performance recommendations
        avg_time = sum(r.get('processing_time', 0) for r in individual_results) / len(individual_results) if individual_results else 0
        if avg_time > 2.0:
            recommendations.append(f"⏱️ Average processing time ({avg_time:.2f}s) above target (1s). Consider optimization.")
        
        # Database state recommendations
        if failure_analysis['recent_failures'] > 0:
            recommendations.append(f"📋 {failure_analysis['recent_failures']} recent failures detected. Review failure logs.")
        
        if not recommendations:
            recommendations.append("✅ All tests passed with good quality metrics. System performing well.")
        
        return recommendations

def main():
    """Main test execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Comprehensive Job Import Testing")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--single-job", help="Test only specific job ID")
    parser.add_argument("--csv-path", default="jobs_import.csv", help="Path to CSV file")
    
    args = parser.parse_args()
    
    # Initialize tester
    tester = JobImportTester(verbose=args.verbose)
    
    try:
        # Check if CSV file exists
        csv_path = ROOT / args.csv_path
        if not csv_path.exists():
            tester.log(f"❌ CSV file not found: {csv_path}", "ERROR")
            return 1
        
        # Analyze CSV structure
        csv_analysis = tester.analyze_csv_structure(str(csv_path))
        tester.log(f"📋 CSV Analysis: {csv_analysis['total_rows']} rows, {len(csv_analysis['headers'])} columns")
        
        if csv_analysis.get('pii_detected'):
            tester.log(f"⚠️ Potential PII detected in {len(csv_analysis['pii_detected'])} locations", "WARN")
        
        # Extract first 10 jobs
        test_csv_path = tester.extract_first_10_jobs(str(csv_path))
        
        # Test individual jobs
        individual_results = []
        
        with open(test_csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for job_index, job_data in enumerate(reader):
                # Test single job if specified
                if args.single_job and job_data.get('מספר משרה') != args.single_job:
                    continue
                
                result = tester.test_individual_job(job_data, job_index)
                individual_results.append(result)
                
                if args.single_job:
                    break  # Only test the specified job
        
        # Test batch import (unless testing single job)
        batch_result = {}
        if not args.single_job:
            batch_result = tester.test_batch_import(test_csv_path)
        
        # Analyze failures
        failure_analysis = tester.analyze_import_failures()
        
        # Generate comprehensive report
        report = tester.generate_comprehensive_report(
            individual_results, batch_result, failure_analysis, csv_analysis
        )
        
        # Save report
        report_path = ROOT / f"job_import_test_report_{int(time.time())}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        tester.log(f"📄 Comprehensive report saved: {report_path}")
        
        # Print summary
        print("\n" + "="*80)
        print("🎯 TEST SUMMARY")
        print("="*80)
        
        summary = report['summary']
        print(f"Individual Tests: {summary['individual_tests']['passed']}/{summary['individual_tests']['total']} passed "
              f"({summary['individual_tests']['success_rate']:.1%})")
        
        if batch_result:
            print(f"Batch Import: {'✅ Success' if summary['batch_test']['success'] else '❌ Failed'} "
                  f"({summary['batch_test']['jobs_imported']} jobs imported)")
        
        print(f"Average Skills: {summary['quality_metrics']['avg_skills']:.1f}")
        print(f"Mandatory Detection: {summary['quality_metrics']['mandatory_detection_rate']:.1%}")
        print(f"PII Scrubbing: {summary['quality_metrics']['pii_scrubbing_rate']:.1%}")
        
        print("\n🔧 RECOMMENDATIONS:")
        for rec in report['recommendations']:
            print(f"  {rec}")
        
        # Clean up test CSV
        os.unlink(test_csv_path)
        
        return 0 if summary['individual_tests']['success_rate'] >= 0.8 else 1
        
    except Exception as e:
        tester.log(f"💥 Test execution failed: {e}", "ERROR")
        if args.verbose:
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
