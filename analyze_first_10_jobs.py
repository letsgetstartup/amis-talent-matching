#!/usr/bin/env python3
"""
Job Import Analysis - First 10 Jobs Investigation
===============================================
Analyzes the first 10 jobs from jobs_import.csv, tests import functionality,
and investigates any broken imports with detailed error analysis.

This script provides comprehensive testing and debugging for job imports.
"""

import csv
import json
import sys
import os
import tempfile
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import subprocess

# Project root
PROJECT_ROOT = Path(__file__).resolve().parent

def log(message: str, level: str = "INFO"):
    """Simple logging function"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    prefix = {"INFO": "ℹ️", "WARN": "⚠️", "ERROR": "🚨", "SUCCESS": "✅"}
    print(f"[{timestamp}] {prefix.get(level, '📝')} {message}")

def extract_first_10_jobs(csv_path: str) -> List[Dict[str, str]]:
    """Extract the first 10 jobs from the CSV file"""
    log("📄 Extracting first 10 jobs from CSV...")
    
    jobs = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            log(f"CSV headers detected: {headers}")
            
            for i, row in enumerate(reader):
                if i >= 10:
                    break
                jobs.append(dict(row))
        
        log(f"✅ Extracted {len(jobs)} jobs")
        return jobs
        
    except Exception as e:
        log(f"❌ Error extracting jobs: {e}", "ERROR")
        return []

def analyze_job_data_quality(jobs: List[Dict[str, str]]) -> Dict[str, Any]:
    """Analyze data quality of the extracted jobs"""
    log("🔍 Analyzing job data quality...")
    
    analysis = {
        'total_jobs': len(jobs),
        'empty_fields': {},
        'encoding_issues': [],
        'potential_issues': [],
        'job_summaries': []
    }
    
    required_fields = ['מספר משרה', 'שם משרה', 'מקום עבודה', 'תאור תפקיד', 'דרישות התפקיד']
    
    for i, job in enumerate(jobs):
        job_summary = {
            'index': i,
            'job_id': job.get('מספר משרה', 'N/A'),
            'title': job.get('שם משרה', 'N/A'),
            'city': job.get('מקום עבודה', 'N/A'),
            'issues': []
        }
        
        # Check for empty required fields
        for field in required_fields:
            if not job.get(field, '').strip():
                if field not in analysis['empty_fields']:
                    analysis['empty_fields'][field] = 0
                analysis['empty_fields'][field] += 1
                job_summary['issues'].append(f"Empty {field}")
        
        # Check for encoding issues
        for field, value in job.items():
            if value and '�' in value:
                analysis['encoding_issues'].append(f"Job {i}: {field}")
                job_summary['issues'].append("Encoding issue")
        
        # Check for very short or very long fields
        desc = job.get('תאור תפקיד', '')
        req = job.get('דרישות התפקיד', '')
        
        if desc and len(desc) < 50:
            job_summary['issues'].append("Very short description")
        if desc and len(desc) > 5000:
            job_summary['issues'].append("Very long description")
        if req and len(req) < 20:
            job_summary['issues'].append("Very short requirements")
        
        analysis['job_summaries'].append(job_summary)
    
    return analysis

def test_single_job_import(job_data: Dict[str, str], job_index: int) -> Dict[str, Any]:
    """Test importing a single job and capture all output"""
    job_id = job_data.get('מספר משרה', f'job_{job_index}')
    title = job_data.get('שם משרה', 'Unknown')[:50]
    
    log(f"🧪 Testing job {job_index + 1}: {job_id} - {title}")
    
    result = {
        'job_index': job_index,
        'job_id': job_id,
        'title': title,
        'success': False,
        'error_message': None,
        'stdout': '',
        'stderr': '',
        'exit_code': None,
        'processing_notes': []
    }
    
    try:
        # Create temporary CSV file with just this job
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.csv', delete=False) as tmp_file:
            writer = csv.DictWriter(tmp_file, fieldnames=job_data.keys())
            writer.writeheader()
            writer.writerow(job_data)
            tmp_csv_path = tmp_file.name
        
        # Test with the enriched importer
        cmd = [
            sys.executable,
            'talentdb/scripts/import_csv_enriched.py',
            tmp_csv_path
        ]
        
        try:
            log(f"  Running: {' '.join(cmd)}")
            process = subprocess.run(
                cmd,
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=30  # 30 second timeout
            )
            
            result['exit_code'] = process.returncode
            result['stdout'] = process.stdout
            result['stderr'] = process.stderr
            result['success'] = process.returncode == 0
            
            if process.returncode == 0:
                log(f"  ✅ Job {job_id} imported successfully")
            else:
                log(f"  ❌ Job {job_id} import failed (exit code: {process.returncode})", "ERROR")
                if process.stderr:
                    log(f"     Error: {process.stderr.strip()}", "ERROR")
                    result['error_message'] = process.stderr.strip()
            
        except subprocess.TimeoutExpired:
            result['error_message'] = "Import timed out after 30 seconds"
            log(f"  ⏰ Job {job_id} import timed out", "ERROR")
        
        except Exception as e:
            result['error_message'] = f"Subprocess error: {str(e)}"
            log(f"  💥 Subprocess error for job {job_id}: {e}", "ERROR")
        
        # Clean up temp file
        try:
            os.unlink(tmp_csv_path)
        except:
            pass
    
    except Exception as e:
        result['error_message'] = f"Test setup error: {str(e)}"
        log(f"  💥 Test setup error for job {job_id}: {e}", "ERROR")
    
    return result

def analyze_import_patterns(test_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze patterns in import successes and failures"""
    log("📊 Analyzing import patterns...")
    
    analysis = {
        'total_tests': len(test_results),
        'successful': 0,
        'failed': 0,
        'error_patterns': {},
        'success_rate': 0,
        'common_errors': [],
        'problematic_jobs': []
    }
    
    error_messages = []
    
    for result in test_results:
        if result['success']:
            analysis['successful'] += 1
        else:
            analysis['failed'] += 1
            
            # Collect error patterns
            error_msg = result.get('error_message', 'Unknown error')
            error_messages.append(error_msg)
            
            # Track specific error patterns
            if 'not found' in error_msg.lower():
                analysis['error_patterns']['file_not_found'] = analysis['error_patterns'].get('file_not_found', 0) + 1
            elif 'import' in error_msg.lower():
                analysis['error_patterns']['import_error'] = analysis['error_patterns'].get('import_error', 0) + 1
            elif 'encoding' in error_msg.lower():
                analysis['error_patterns']['encoding_error'] = analysis['error_patterns'].get('encoding_error', 0) + 1
            else:
                analysis['error_patterns']['other'] = analysis['error_patterns'].get('other', 0) + 1
            
            analysis['problematic_jobs'].append({
                'job_id': result['job_id'],
                'title': result['title'],
                'error': error_msg
            })
    
    analysis['success_rate'] = analysis['successful'] / analysis['total_tests'] if analysis['total_tests'] > 0 else 0
    
    # Find most common errors
    from collections import Counter
    error_counter = Counter(error_messages)
    analysis['common_errors'] = [
        {'error': error, 'count': count}
        for error, count in error_counter.most_common(5)
    ]
    
    return analysis

def investigate_broken_imports(test_results: List[Dict[str, Any]], job_data: List[Dict[str, str]]) -> Dict[str, Any]:
    """Deep dive investigation into broken imports"""
    log("🔬 Investigating broken imports...")
    
    failed_results = [r for r in test_results if not r['success']]
    
    investigation = {
        'failed_count': len(failed_results),
        'detailed_analysis': [],
        'potential_causes': set(),
        'recommendations': []
    }
    
    for result in failed_results:
        job_index = result['job_index']
        original_job = job_data[job_index] if job_index < len(job_data) else {}
        
        detailed = {
            'job_id': result['job_id'],
            'title': result['title'],
            'error_analysis': {},
            'data_analysis': {},
            'potential_issues': []
        }
        
        # Analyze the error
        error_msg = result.get('error_message', '')
        stderr = result.get('stderr', '')
        
        if 'ModuleNotFoundError' in stderr or 'ImportError' in stderr:
            detailed['potential_issues'].append("Python import/dependency issue")
            investigation['potential_causes'].add("missing_dependencies")
        
        if 'CSV' in stderr or 'csv' in stderr:
            detailed['potential_issues'].append("CSV parsing issue")
            investigation['potential_causes'].add("csv_format_error")
        
        if 'encoding' in stderr.lower() or 'unicode' in stderr.lower():
            detailed['potential_issues'].append("Text encoding issue")
            investigation['potential_causes'].add("encoding_error")
        
        if 'database' in stderr.lower() or 'mongo' in stderr.lower():
            detailed['potential_issues'].append("Database connection issue")
            investigation['potential_causes'].add("database_error")
        
        # Analyze the job data
        if original_job:
            detailed['data_analysis'] = {
                'has_all_fields': all(original_job.get(field) for field in ['מספר משרה', 'שם משרה']),
                'title_length': len(original_job.get('שם משרה', '')),
                'description_length': len(original_job.get('תאור תפקיד', '')),
                'requirements_length': len(original_job.get('דרישות התפקיד', '')),
                'has_special_chars': any('₪' in str(value) or '"' in str(value) for value in original_job.values())
            }
        
        detailed['error_analysis'] = {
            'error_message': error_msg,
            'stderr_excerpt': stderr[:200] if stderr else '',
            'exit_code': result.get('exit_code')
        }
        
        investigation['detailed_analysis'].append(detailed)
    
    # Generate recommendations
    if 'missing_dependencies' in investigation['potential_causes']:
        investigation['recommendations'].append("Check Python environment and install missing dependencies")
    
    if 'csv_format_error' in investigation['potential_causes']:
        investigation['recommendations'].append("Validate CSV format and encoding")
    
    if 'database_error' in investigation['potential_causes']:
        investigation['recommendations'].append("Check MongoDB connection and configuration")
    
    if 'encoding_error' in investigation['potential_causes']:
        investigation['recommendations'].append("Ensure UTF-8 encoding is used consistently")
    
    if len(failed_results) > len(test_results) * 0.5:
        investigation['recommendations'].append("High failure rate suggests systematic issue - check environment setup")
    
    return investigation

def generate_full_report(jobs: List[Dict[str, str]], quality_analysis: Dict, test_results: List[Dict], 
                        pattern_analysis: Dict, investigation: Dict) -> Dict[str, Any]:
    """Generate comprehensive report"""
    log("📋 Generating comprehensive report...")
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_jobs_analyzed': len(jobs),
            'data_quality_score': calculate_quality_score(quality_analysis),
            'import_success_rate': pattern_analysis['success_rate'],
            'critical_issues_found': len(investigation['potential_causes']),
            'overall_status': 'PASS' if pattern_analysis['success_rate'] > 0.8 else 'FAIL'
        },
        'data_quality_analysis': quality_analysis,
        'import_test_results': test_results,
        'pattern_analysis': pattern_analysis,
        'failure_investigation': investigation,
        'job_details': [
            {
                'index': i,
                'job_id': job.get('מספר משרה'),
                'title': job.get('שם משרה'),
                'city': job.get('מקום עבודה'),
                'description_length': len(job.get('תאור תפקיד', '')),
                'requirements_length': len(job.get('דרישות התפקיד', ''))
            }
            for i, job in enumerate(jobs)
        ]
    }
    
    return report

def calculate_quality_score(quality_analysis: Dict) -> float:
    """Calculate a simple data quality score (0-100)"""
    total_jobs = quality_analysis['total_jobs']
    if total_jobs == 0:
        return 0
    
    score = 100
    
    # Deduct points for empty fields
    for field, count in quality_analysis['empty_fields'].items():
        penalty = (count / total_jobs) * 20  # Up to 20 points per field
        score -= penalty
    
    # Deduct points for encoding issues
    if quality_analysis['encoding_issues']:
        score -= len(quality_analysis['encoding_issues']) * 5
    
    # Deduct points for jobs with issues
    jobs_with_issues = sum(1 for job in quality_analysis['job_summaries'] if job['issues'])
    score -= (jobs_with_issues / total_jobs) * 30
    
    return max(0, score)

def main():
    """Main execution function"""
    log("🚀 Starting Job Import Analysis for First 10 Jobs")
    
    # Check if CSV file exists
    csv_path = PROJECT_ROOT / "jobs_import.csv"
    if not csv_path.exists():
        log(f"❌ CSV file not found: {csv_path}", "ERROR")
        return 1
    
    try:
        # Step 1: Extract first 10 jobs
        jobs = extract_first_10_jobs(str(csv_path))
        if not jobs:
            log("❌ No jobs extracted", "ERROR")
            return 1
        
        # Step 2: Analyze data quality
        quality_analysis = analyze_job_data_quality(jobs)
        log(f"📊 Data quality score: {calculate_quality_score(quality_analysis):.1f}/100")
        
        # Step 3: Test each job import
        test_results = []
        for i, job in enumerate(jobs):
            result = test_single_job_import(job, i)
            test_results.append(result)
        
        # Step 4: Analyze patterns
        pattern_analysis = analyze_import_patterns(test_results)
        log(f"📈 Import success rate: {pattern_analysis['success_rate']:.1%}")
        
        # Step 5: Investigate failures
        investigation = investigate_broken_imports(test_results, jobs)
        if investigation['failed_count'] > 0:
            log(f"🔍 {investigation['failed_count']} imports failed - investigating...", "WARN")
        
        # Step 6: Generate report
        report = generate_full_report(jobs, quality_analysis, test_results, pattern_analysis, investigation)
        
        # Save report
        report_path = PROJECT_ROOT / f"job_import_analysis_{int(datetime.now().timestamp())}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        log(f"📄 Full report saved to: {report_path}")
        
        # Print summary
        print("\n" + "="*80)
        print("🎯 ANALYSIS SUMMARY")
        print("="*80)
        
        summary = report['summary']
        print(f"Overall Status: {summary['overall_status']}")
        print(f"Jobs Analyzed: {summary['total_jobs_analyzed']}")
        print(f"Data Quality Score: {summary['data_quality_score']:.1f}/100")
        print(f"Import Success Rate: {summary['import_success_rate']:.1%}")
        print(f"Critical Issues: {summary['critical_issues_found']}")
        
        if pattern_analysis['failed'] > 0:
            print(f"\n❌ FAILED IMPORTS ({pattern_analysis['failed']}):")
            for job in investigation['detailed_analysis']:
                print(f"  • {job['job_id']}: {job['title'][:50]}...")
                print(f"    Error: {job['error_analysis']['error_message'][:100]}...")
        
        if investigation['recommendations']:
            print(f"\n🔧 RECOMMENDATIONS:")
            for rec in investigation['recommendations']:
                print(f"  • {rec}")
        
        print(f"\n📊 Most Common Errors:")
        for error_info in pattern_analysis['common_errors'][:3]:
            print(f"  • {error_info['error'][:80]}... ({error_info['count']} times)")
        
        return 0 if summary['overall_status'] == 'PASS' else 1
        
    except Exception as e:
        log(f"💥 Analysis failed: {e}", "ERROR")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
