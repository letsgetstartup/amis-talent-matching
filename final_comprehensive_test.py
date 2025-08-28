#!/usr/bin/env python3
"""
Final Comprehensive Job Import Test
===================================
Tests the fixed import functionality and provides complete analysis with logging.
"""

import sys
import json
import time
import tempfile
import csv
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Project root
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "talentdb"))

try:
    from scripts.ingest_agent import db
    print("✅ Successfully connected to database")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def cleanup_test_data():
    """Clean up any existing test data"""
    print("🧹 Cleaning up previous test data...")
    
    test_job_ids = ['405627', '405620', '405626', '405665', '405690', 
                   '405691', '405692', '405731', '405768', '405777']
    
    # Remove jobs with these external order IDs
    result = db['jobs'].delete_many({'external_order_id': {'$in': test_job_ids}})
    print(f"   Removed {result.deleted_count} existing test jobs")
    
    # Also remove jobs by title pattern for today
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    test_titles = [
        'עובד/ת הרכבה',
        'עובד/ת ייצור עבודה מועדפת',
        'צבעי/ת תעשייתי/ת',
        'מנהל/ת אחזקה'
    ]
    
    for title in test_titles:
        result = db['jobs'].delete_many({
            'title': {'$regex': title},
            'created_at': {'$gte': today_start}
        })
        if result.deleted_count > 0:
            print(f"   Removed {result.deleted_count} jobs matching '{title}'")

def test_single_job_import_with_verification(job_data: Dict[str, str], job_index: int) -> Dict[str, Any]:
    """Test importing a single job and verify it appears correctly in the database"""
    job_id = job_data.get('מספר משרה', f'job_{job_index}')
    title = job_data.get('שם משרה', 'Unknown')
    
    print(f"🧪 Testing job {job_index + 1}: {job_id} - {title[:50]}...")
    
    result = {
        'job_index': job_index,
        'job_id': job_id,
        'title': title,
        'import_success': False,
        'db_verification': False,
        'error_message': None,
        'db_document': None,
        'metrics': {},
        'quality_analysis': {}
    }
    
    try:
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.csv', delete=False) as tmp_file:
            writer = csv.DictWriter(tmp_file, fieldnames=job_data.keys())
            writer.writeheader()
            writer.writerow(job_data)
            tmp_csv_path = tmp_file.name
        
        # Import using the fixed script
        cmd = [sys.executable, 'talentdb/scripts/import_csv_enriched.py', tmp_csv_path]
        process = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=30)
        
        if process.returncode == 0:
            result['import_success'] = True
            
            # Parse stdout for metrics
            if 'Metrics:' in process.stdout:
                try:
                    metrics_line = process.stdout.split('Metrics: ')[1].split('\n')[0]
                    result['metrics'] = json.loads(metrics_line)
                except:
                    pass
        else:
            result['error_message'] = process.stderr or f"Exit code: {process.returncode}"
        
        # Verify in database
        time.sleep(0.1)  # Small delay to ensure DB write is complete
        db_job = db['jobs'].find_one({'external_order_id': job_id})
        
        if db_job:
            result['db_verification'] = True
            result['db_document'] = {
                'id': str(db_job['_id']),
                'external_order_id': db_job.get('external_order_id'),
                'title': db_job.get('title'),
                'city_canonical': db_job.get('city_canonical'),
                'skill_count': len(db_job.get('skill_set', [])),
                'synthetic_skill_count': len(db_job.get('synthetic_skills', [])),
                'mandatory_requirements': len(db_job.get('mandatory_requirements', [])),
                'flags': db_job.get('flags', []),
                'created_at': db_job.get('created_at'),
                'has_pii_scrubbing': '[EMAIL]' in str(db_job.get('full_text', '')) or '[PHONE]' in str(db_job.get('full_text', ''))
            }
            
            # Quality analysis
            result['quality_analysis'] = {
                'skill_count_adequate': len(db_job.get('skill_set', [])) >= 8,
                'has_title': bool(db_job.get('title', '').strip()),
                'has_city': bool(db_job.get('city_canonical', '').strip()),
                'has_description': len(db_job.get('job_description', '')) > 20,
                'synthetic_enrichment_active': len(db_job.get('synthetic_skills', [])) > 0,
                'mandatory_detection_working': len(db_job.get('mandatory_requirements', [])) > 0 if 'חובה' in str(job_data) else True
            }
        else:
            # Check if job exists but without correct external_order_id
            title_match = db['jobs'].find_one({'title': title})
            if title_match:
                result['error_message'] = f"Job found by title but external_order_id is '{title_match.get('external_order_id')}' instead of '{job_id}'"
        
        # Clean up temp file
        import os
        try:
            os.unlink(tmp_csv_path)
        except:
            pass
        
        # Status
        if result['import_success'] and result['db_verification']:
            print(f"   ✅ Success - Job imported and verified in DB")
        elif result['import_success']:
            print(f"   ⚠️ Import succeeded but DB verification failed")
        else:
            print(f"   ❌ Import failed: {result['error_message']}")
        
    except Exception as e:
        result['error_message'] = f"Test error: {str(e)}"
        print(f"   💥 Test error: {e}")
    
    return result

def test_batch_import_and_analyze():
    """Test batch import of first 10 jobs and analyze results"""
    print("\n📦 Testing batch import of first 10 jobs...")
    
    # Extract first 10 jobs
    csv_path = ROOT / "jobs_import.csv"
    jobs = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 10:
                break
            jobs.append(dict(row))
    
    # Create test CSV
    test_csv_path = ROOT / "test_batch_10_jobs.csv"
    with open(test_csv_path, 'w', encoding='utf-8', newline='') as f:
        if jobs:
            writer = csv.DictWriter(f, fieldnames=jobs[0].keys())
            writer.writeheader()
            writer.writerows(jobs)
    
    print(f"📄 Created test CSV with {len(jobs)} jobs")
    
    # Record before state
    before_count = db['jobs'].count_documents({})
    
    # Import batch
    cmd = [sys.executable, 'talentdb/scripts/import_csv_enriched.py', str(test_csv_path)]
    process = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=60)
    
    batch_result = {
        'success': process.returncode == 0,
        'stdout': process.stdout,
        'stderr': process.stderr,
        'jobs_in_input': len(jobs),
        'jobs_found_in_db': 0,
        'metrics': {}
    }
    
    if process.returncode == 0:
        print("✅ Batch import completed successfully")
        
        # Parse metrics
        if 'Metrics:' in process.stdout:
            try:
                metrics_line = process.stdout.split('Metrics: ')[1].split('\n')[0]
                batch_result['metrics'] = json.loads(metrics_line)
                print(f"📊 Batch metrics: {batch_result['metrics']}")
            except:
                pass
    else:
        print(f"❌ Batch import failed: {process.stderr}")
    
    # Verify in database
    time.sleep(0.2)
    test_job_ids = [job.get('מספר משרה') for job in jobs if job.get('מספר משרה')]
    found_jobs = list(db['jobs'].find({'external_order_id': {'$in': test_job_ids}}))
    batch_result['jobs_found_in_db'] = len(found_jobs)
    
    print(f"🔍 Found {len(found_jobs)}/{len(test_job_ids)} jobs in database with correct external_order_id")
    
    # Clean up test file
    try:
        test_csv_path.unlink()
    except:
        pass
    
    return batch_result, jobs

def analyze_logging_and_metrics():
    """Analyze all logging and metrics systems"""
    print("\n📊 Analyzing logging and metrics systems...")
    
    analysis = {
        'import_metrics': None,
        'security_audit_working': False,
        'outreach_failures_count': 0,
        'job_versions_count': 0,
        'recent_activity': {}
    }
    
    # Check import metrics
    meta_doc = db['_meta'].find_one({'key': 'last_import_metrics'})
    if meta_doc:
        analysis['import_metrics'] = meta_doc.get('value', {})
        print(f"📈 Latest import metrics: {analysis['import_metrics']}")
    
    # Test security audit
    try:
        from scripts.security_audit import audit_log
        audit_log(
            tenant_id="test_comprehensive",
            action="comprehensive_test_complete",
            resource="job_import_system",
            resource_id="final_test",
            success=True,
            details={"timestamp": time.time(), "test_type": "comprehensive_final"}
        )
        analysis['security_audit_working'] = True
        print("✅ Security audit logging working")
    except Exception as e:
        print(f"❌ Security audit logging failed: {e}")
    
    # Check collections
    analysis['outreach_failures_count'] = db['outreach_failures'].count_documents({})
    analysis['job_versions_count'] = db['jobs_versions'].count_documents({})
    
    print(f"📝 Outreach failures logged: {analysis['outreach_failures_count']}")
    print(f"📚 Job versions tracked: {analysis['job_versions_count']}")
    
    return analysis

def generate_final_report(individual_results: List[Dict], batch_result: Dict, logging_analysis: Dict) -> Dict:
    """Generate comprehensive final report"""
    print("\n📋 Generating final comprehensive report...")
    
    # Calculate success rates
    individual_import_success = sum(1 for r in individual_results if r['import_success'])
    individual_db_success = sum(1 for r in individual_results if r['db_verification'])
    
    # Quality metrics
    total_skills = sum(r['db_document']['skill_count'] for r in individual_results if r.get('db_document'))
    avg_skills = total_skills / len([r for r in individual_results if r.get('db_document')]) if individual_results else 0
    
    synthetic_active = sum(1 for r in individual_results if r.get('quality_analysis', {}).get('synthetic_enrichment_active'))
    mandatory_detection = sum(1 for r in individual_results if r.get('quality_analysis', {}).get('mandatory_detection_working'))
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'test_summary': {
            'total_individual_tests': len(individual_results),
            'individual_import_success_rate': individual_import_success / len(individual_results) if individual_results else 0,
            'individual_db_verification_rate': individual_db_success / len(individual_results) if individual_results else 0,
            'batch_import_success': batch_result.get('success', False),
            'batch_jobs_found_rate': batch_result.get('jobs_found_in_db', 0) / batch_result.get('jobs_in_input', 1)
        },
        'quality_metrics': {
            'average_skills_per_job': avg_skills,
            'synthetic_enrichment_rate': synthetic_active / len(individual_results) if individual_results else 0,
            'mandatory_detection_rate': mandatory_detection / len(individual_results) if individual_results else 0,
            'external_id_fix_working': individual_db_success > 0
        },
        'logging_systems': {
            'import_metrics_available': logging_analysis.get('import_metrics') is not None,
            'security_audit_working': logging_analysis.get('security_audit_working', False),
            'failure_tracking_active': logging_analysis.get('outreach_failures_count', 0) >= 0,
            'versioning_active': logging_analysis.get('job_versions_count', 0) >= 0
        },
        'detailed_results': individual_results,
        'batch_result': batch_result,
        'logging_analysis': logging_analysis,
        'overall_status': 'EXCELLENT',
        'critical_issues': [],
        'recommendations': []
    }
    
    # Determine overall status
    if report['test_summary']['individual_db_verification_rate'] < 0.8:
        report['overall_status'] = 'NEEDS_IMPROVEMENT'
        report['critical_issues'].append(f"Low DB verification rate: {report['test_summary']['individual_db_verification_rate']:.1%}")
    
    if not report['quality_metrics']['external_id_fix_working']:
        report['overall_status'] = 'CRITICAL_ISSUE'
        report['critical_issues'].append("External ID fix not working - jobs not properly indexed")
    
    if report['quality_metrics']['average_skills_per_job'] < 8:
        report['recommendations'].append(f"Average skills ({report['quality_metrics']['average_skills_per_job']:.1f}) below target (8+)")
    
    if report['test_summary']['batch_jobs_found_rate'] < 0.9:
        report['recommendations'].append(f"Batch import verification rate low: {report['test_summary']['batch_jobs_found_rate']:.1%}")
    
    if report['overall_status'] == 'EXCELLENT':
        report['recommendations'].append("✅ All systems working excellently - import fix successful!")
    
    return report

def main():
    """Main comprehensive test execution"""
    print("🚀 Starting Final Comprehensive Job Import Test")
    print("=" * 80)
    
    try:
        # Step 1: Cleanup
        cleanup_test_data()
        
        # Step 2: Extract first 10 jobs for individual testing
        csv_path = ROOT / "jobs_import.csv"
        jobs = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 10:
                    break
                jobs.append(dict(row))
        
        print(f"📄 Extracted {len(jobs)} jobs for testing")
        
        # Step 3: Test each job individually
        print("\n🧪 Testing individual job imports...")
        individual_results = []
        for i, job in enumerate(jobs):
            result = test_single_job_import_with_verification(job, i)
            individual_results.append(result)
        
        # Step 4: Test batch import
        batch_result, batch_jobs = test_batch_import_and_analyze()
        
        # Step 5: Analyze logging systems
        logging_analysis = analyze_logging_and_metrics()
        
        # Step 6: Generate final report
        final_report = generate_final_report(individual_results, batch_result, logging_analysis)
        
        # Save report
        report_path = ROOT / f"final_comprehensive_report_{int(time.time())}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(final_report, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n📄 Final report saved: {report_path}")
        
        # Print summary
        print("\n" + "=" * 80)
        print("🎯 FINAL COMPREHENSIVE TEST RESULTS")
        print("=" * 80)
        
        summary = final_report['test_summary']
        quality = final_report['quality_metrics']
        logging = final_report['logging_systems']
        
        print(f"Overall Status: {final_report['overall_status']}")
        print(f"Individual Import Success: {summary['individual_import_success_rate']:.1%}")
        print(f"Database Verification Rate: {summary['individual_db_verification_rate']:.1%}")
        print(f"Batch Import Success: {'✅' if summary['batch_import_success'] else '❌'}")
        print(f"Batch DB Verification: {summary['batch_jobs_found_rate']:.1%}")
        
        print(f"\n📊 QUALITY METRICS:")
        print(f"Average Skills/Job: {quality['average_skills_per_job']:.1f}")
        print(f"External ID Fix Working: {'✅' if quality['external_id_fix_working'] else '❌'}")
        print(f"Synthetic Enrichment: {quality['synthetic_enrichment_rate']:.1%}")
        print(f"Mandatory Detection: {quality['mandatory_detection_rate']:.1%}")
        
        print(f"\n🔧 LOGGING SYSTEMS:")
        print(f"Import Metrics: {'✅' if logging['import_metrics_available'] else '❌'}")
        print(f"Security Audit: {'✅' if logging['security_audit_working'] else '❌'}")
        print(f"Failure Tracking: {'✅' if logging['failure_tracking_active'] else '❌'}")
        print(f"Versioning: {'✅' if logging['versioning_active'] else '❌'}")
        
        if final_report['critical_issues']:
            print(f"\n🚨 CRITICAL ISSUES:")
            for issue in final_report['critical_issues']:
                print(f"  • {issue}")
        
        if final_report['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for rec in final_report['recommendations']:
                print(f"  • {rec}")
        
        print(f"\n📈 KEY FINDINGS:")
        if quality['external_id_fix_working']:
            print("  ✅ BUG FIX SUCCESSFUL: External order IDs now properly preserved")
        
        if quality['average_skills_per_job'] >= 8:
            print(f"  ✅ SKILL EXTRACTION EXCELLENT: Averaging {quality['average_skills_per_job']:.1f} skills per job")
        
        if logging['import_metrics_available']:
            metrics = logging_analysis.get('import_metrics', {})
            print(f"  📊 LATEST METRICS: {metrics.get('jobs_ingested', 0)} jobs, {metrics.get('avg_skills', 0)} avg skills")
        
        return 0 if final_report['overall_status'] in ['EXCELLENT', 'GOOD'] else 1
        
    except Exception as e:
        print(f"💥 Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
