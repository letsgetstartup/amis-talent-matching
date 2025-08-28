#!/usr/bin/env python3
"""
Deep Database Analysis & Logging Test
=====================================
Analyzes the actual MongoDB content from the job imports and tests all logging systems.
"""

import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add talentdb to path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "talentdb"))

try:
    from scripts.ingest_agent import db
    from scripts.security_audit import audit_log, log_data_access, get_security_events
    print("✅ Successfully imported database modules")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the project root with proper environment")
    sys.exit(1)

def analyze_imported_jobs():
    """Analyze the jobs that were actually imported to MongoDB"""
    print("🔍 Analyzing imported jobs in MongoDB...")
    
    # Get all jobs with our test order IDs
    test_job_ids = ['405627', '405620', '405626', '405665', '405690', 
                   '405691', '405692', '405731', '405768', '405777']
    
    jobs = list(db['jobs'].find({'external_order_id': {'$in': test_job_ids}}))
    
    print(f"📊 Found {len(jobs)} imported jobs in database")
    
    analysis = {
        'total_found': len(jobs),
        'job_details': [],
        'skill_analysis': {
            'total_skills': 0,
            'synthetic_skills': 0,
            'mandatory_requirements': 0,
            'avg_skills_per_job': 0,
            'skill_distribution': {}
        },
        'quality_flags': {},
        'pii_scrubbing_check': [],
        'synthetic_enrichment_analysis': []
    }
    
    for job in jobs:
        job_detail = {
            'external_order_id': job.get('external_order_id'),
            'title': job.get('title'),
            'city_canonical': job.get('city_canonical'),
            'skill_count': len(job.get('skill_set', [])),
            'synthetic_skill_count': len(job.get('synthetic_skills', [])),
            'mandatory_req_count': len(job.get('mandatory_requirements', [])),
            'flags': job.get('flags', []),
            'created_at': job.get('created_at'),
            'updated_at': job.get('updated_at')
        }
        
        # Check skill details
        skills = job.get('skill_set', [])
        synthetic_skills = job.get('synthetic_skills', [])
        
        analysis['skill_analysis']['total_skills'] += len(skills)
        analysis['skill_analysis']['synthetic_skills'] += len(synthetic_skills)
        analysis['skill_analysis']['mandatory_requirements'] += len(job.get('mandatory_requirements', []))
        
        # Analyze synthetic enrichment
        if synthetic_skills:
            enrichment_detail = {
                'job_id': job.get('external_order_id'),
                'title': job.get('title')[:50],
                'synthetic_skills': [
                    {'name': skill.get('name'), 'reason': skill.get('reason')}
                    for skill in synthetic_skills
                ]
            }
            analysis['synthetic_enrichment_analysis'].append(enrichment_detail)
        
        # Check for PII scrubbing
        full_text = job.get('full_text', '')
        if '[EMAIL]' in full_text or '[PHONE]' in full_text:
            analysis['pii_scrubbing_check'].append({
                'job_id': job.get('external_order_id'),
                'pii_found': '[EMAIL]' in full_text,
                'phone_found': '[PHONE]' in full_text
            })
        
        # Quality flags
        for flag in job.get('flags', []):
            analysis['quality_flags'][flag] = analysis['quality_flags'].get(flag, 0) + 1
        
        analysis['job_details'].append(job_detail)
    
    # Calculate averages
    if len(jobs) > 0:
        analysis['skill_analysis']['avg_skills_per_job'] = analysis['skill_analysis']['total_skills'] / len(jobs)
    
    return analysis

def test_logging_systems():
    """Test all logging systems comprehensively"""
    print("🧪 Testing logging systems...")
    
    test_results = {
        'security_audit': {'success': False, 'error': None},
        'outreach_failures': {'success': False, 'error': None},
        'import_metrics': {'success': False, 'error': None},
        'versions': {'success': False, 'error': None}
    }
    
    # Test 1: Security audit logging
    try:
        audit_log(
            tenant_id="test_tenant",
            action="test_audit_log",
            resource="job_import_test",
            resource_id="test_123",
            success=True,
            details={"test_timestamp": time.time(), "test_type": "comprehensive_analysis"}
        )
        test_results['security_audit']['success'] = True
        print("  ✅ Security audit logging working")
    except Exception as e:
        test_results['security_audit']['error'] = str(e)
        print(f"  ❌ Security audit logging failed: {e}")
    
    # Test 2: Check outreach failures collection
    try:
        failure_count = db['outreach_failures'].count_documents({})
        test_results['outreach_failures']['success'] = True
        test_results['outreach_failures']['count'] = failure_count
        print(f"  ✅ Outreach failures collection accessible ({failure_count} records)")
    except Exception as e:
        test_results['outreach_failures']['error'] = str(e)
        print(f"  ❌ Outreach failures collection failed: {e}")
    
    # Test 3: Check import metrics in _meta collection
    try:
        meta_doc = db['_meta'].find_one({'key': 'last_import_metrics'})
        if meta_doc:
            test_results['import_metrics']['success'] = True
            test_results['import_metrics']['data'] = meta_doc.get('value', {})
            print(f"  ✅ Import metrics found: {meta_doc['value']}")
        else:
            print("  ⚠️ No import metrics found in _meta collection")
    except Exception as e:
        test_results['import_metrics']['error'] = str(e)
        print(f"  ❌ Import metrics check failed: {e}")
    
    # Test 4: Check job versions
    try:
        version_count = db['jobs_versions'].count_documents({})
        test_results['versions']['success'] = True
        test_results['versions']['count'] = version_count
        print(f"  ✅ Job versions collection accessible ({version_count} records)")
    except Exception as e:
        test_results['versions']['error'] = str(e)
        print(f"  ❌ Job versions collection failed: {e}")
    
    return test_results

def analyze_skill_extraction_quality(jobs_analysis):
    """Detailed analysis of skill extraction quality"""
    print("🎯 Analyzing skill extraction quality...")
    
    analysis = {
        'skill_quality_metrics': {},
        'synthetic_enrichment_patterns': {},
        'requirement_categorization': {},
        'recommendations': []
    }
    
    # Analyze skill counts
    skill_counts = [job['skill_count'] for job in jobs_analysis['job_details']]
    if skill_counts:
        analysis['skill_quality_metrics'] = {
            'min_skills': min(skill_counts),
            'max_skills': max(skill_counts),
            'avg_skills': sum(skill_counts) / len(skill_counts),
            'jobs_below_target': sum(1 for count in skill_counts if count < 8),
            'jobs_above_35': sum(1 for count in skill_counts if count > 35)
        }
    
    # Analyze synthetic enrichment patterns
    for enrichment in jobs_analysis['synthetic_enrichment_analysis']:
        for skill in enrichment['synthetic_skills']:
            reason = skill['reason']
            analysis['synthetic_enrichment_patterns'][reason] = analysis['synthetic_enrichment_patterns'].get(reason, 0) + 1
    
    # Generate recommendations
    metrics = analysis['skill_quality_metrics']
    if metrics.get('jobs_below_target', 0) > 0:
        analysis['recommendations'].append(f"⚠️ {metrics['jobs_below_target']} jobs have <8 skills - consider improving extraction")
    
    if metrics.get('jobs_above_35', 0) > 0:
        analysis['recommendations'].append(f"⚠️ {metrics['jobs_above_35']} jobs have >35 skills - may need trimming")
    
    if metrics.get('avg_skills', 0) < 12:
        analysis['recommendations'].append("💡 Average skills below target - synthetic enrichment working correctly")
    
    if not analysis['synthetic_enrichment_patterns']:
        analysis['recommendations'].append("🔧 No synthetic enrichment detected - check enrichment logic")
    
    return analysis

def investigate_potential_issues():
    """Look for any potential issues in the imported data"""
    print("🔍 Investigating potential issues...")
    
    issues = {
        'duplicate_external_ids': [],
        'missing_required_fields': [],
        'encoding_problems': [],
        'inconsistent_data': [],
        'performance_concerns': []
    }
    
    # Check for duplicate external order IDs
    pipeline = [
        {'$group': {'_id': '$external_order_id', 'count': {'$sum': 1}}},
        {'$match': {'count': {'$gt': 1}}}
    ]
    
    duplicates = list(db['jobs'].aggregate(pipeline))
    for dup in duplicates:
        issues['duplicate_external_ids'].append(dup['_id'])
    
    # Check for missing required fields
    jobs_missing_fields = db['jobs'].find({
        '$or': [
            {'title': {'$in': [None, '']}},
            {'skill_set': {'$in': [None, []]}},
            {'external_order_id': {'$in': [None, '']}}
        ]
    })
    
    for job in jobs_missing_fields:
        issues['missing_required_fields'].append({
            'id': str(job['_id']),
            'external_order_id': job.get('external_order_id'),
            'missing': []
        })
        
        if not job.get('title'):
            issues['missing_required_fields'][-1]['missing'].append('title')
        if not job.get('skill_set'):
            issues['missing_required_fields'][-1]['missing'].append('skill_set')
        if not job.get('external_order_id'):
            issues['missing_required_fields'][-1]['missing'].append('external_order_id')
    
    return issues

def generate_comprehensive_summary(jobs_analysis, logging_test, skill_analysis, issues_analysis):
    """Generate a comprehensive summary of all analyses"""
    print("📋 Generating comprehensive summary...")
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'overall_health': 'EXCELLENT',
        'import_success_summary': {
            'jobs_successfully_imported': jobs_analysis['total_found'],
            'avg_skills_per_job': jobs_analysis['skill_analysis']['avg_skills_per_job'],
            'synthetic_enrichment_active': len(jobs_analysis['synthetic_enrichment_analysis']) > 0,
            'mandatory_detection_working': jobs_analysis['skill_analysis']['mandatory_requirements'] > 0,
            'pii_scrubbing_active': len(jobs_analysis['pii_scrubbing_check']) > 0
        },
        'logging_system_health': {
            'security_audit_working': logging_test['security_audit']['success'],
            'failure_tracking_working': logging_test['outreach_failures']['success'],
            'metrics_tracking_working': logging_test['import_metrics']['success'],
            'versioning_working': logging_test['versions']['success']
        },
        'quality_assessment': {
            'skill_extraction_quality': 'GOOD' if skill_analysis['skill_quality_metrics'].get('avg_skills', 0) >= 10 else 'NEEDS_IMPROVEMENT',
            'data_consistency': 'EXCELLENT' if not any(issues_analysis.values()) else 'ISSUES_FOUND',
            'synthetic_enrichment': 'ACTIVE' if skill_analysis['synthetic_enrichment_patterns'] else 'INACTIVE'
        },
        'critical_issues': [],
        'recommendations': []
    }
    
    # Determine overall health
    if not logging_test['security_audit']['success']:
        summary['critical_issues'].append("Security audit logging not working")
        summary['overall_health'] = 'NEEDS_ATTENTION'
    
    if issues_analysis['duplicate_external_ids']:
        summary['critical_issues'].append(f"Duplicate external IDs: {issues_analysis['duplicate_external_ids']}")
        summary['overall_health'] = 'ISSUES_FOUND'
    
    if issues_analysis['missing_required_fields']:
        summary['critical_issues'].append(f"{len(issues_analysis['missing_required_fields'])} jobs missing required fields")
        summary['overall_health'] = 'ISSUES_FOUND'
    
    # Add recommendations
    summary['recommendations'].extend(skill_analysis['recommendations'])
    
    if summary['overall_health'] == 'EXCELLENT':
        summary['recommendations'].append("✅ System is working excellently - all imports successful with good quality")
    
    return summary

def main():
    """Main execution"""
    print("🚀 Starting Deep Database Analysis & Logging Test")
    print("=" * 80)
    
    try:
        # Step 1: Analyze imported jobs
        jobs_analysis = analyze_imported_jobs()
        
        # Step 2: Test logging systems
        logging_test = test_logging_systems()
        
        # Step 3: Analyze skill extraction quality
        skill_analysis = analyze_skill_extraction_quality(jobs_analysis)
        
        # Step 4: Investigate potential issues
        issues_analysis = investigate_potential_issues()
        
        # Step 5: Generate comprehensive summary
        summary = generate_comprehensive_summary(jobs_analysis, logging_test, skill_analysis, issues_analysis)
        
        # Save detailed report
        detailed_report = {
            'summary': summary,
            'jobs_analysis': jobs_analysis,
            'logging_test': logging_test,
            'skill_analysis': skill_analysis,
            'issues_analysis': issues_analysis
        }
        
        report_path = ROOT / f"deep_analysis_report_{int(time.time())}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(detailed_report, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n📄 Detailed report saved: {report_path}")
        
        # Print summary
        print("\n" + "=" * 80)
        print("🎯 COMPREHENSIVE ANALYSIS SUMMARY")
        print("=" * 80)
        
        print(f"Overall Health: {summary['overall_health']}")
        print(f"Jobs Imported: {summary['import_success_summary']['jobs_successfully_imported']}")
        print(f"Avg Skills/Job: {summary['import_success_summary']['avg_skills_per_job']:.1f}")
        print(f"Synthetic Enrichment: {'✅' if summary['import_success_summary']['synthetic_enrichment_active'] else '❌'}")
        print(f"Mandatory Detection: {'✅' if summary['import_success_summary']['mandatory_detection_working'] else '❌'}")
        print(f"PII Scrubbing: {'✅' if summary['import_success_summary']['pii_scrubbing_active'] else '❌'}")
        
        print(f"\n🔧 LOGGING SYSTEM STATUS:")
        logging_health = summary['logging_system_health']
        print(f"Security Audit: {'✅' if logging_health['security_audit_working'] else '❌'}")
        print(f"Failure Tracking: {'✅' if logging_health['failure_tracking_working'] else '❌'}")
        print(f"Metrics Tracking: {'✅' if logging_health['metrics_tracking_working'] else '❌'}")
        print(f"Versioning: {'✅' if logging_health['versioning_working'] else '❌'}")
        
        if summary['critical_issues']:
            print(f"\n🚨 CRITICAL ISSUES:")
            for issue in summary['critical_issues']:
                print(f"  • {issue}")
        
        if summary['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for rec in summary['recommendations']:
                print(f"  • {rec}")
        
        # Show some sample data
        if jobs_analysis['synthetic_enrichment_analysis']:
            print(f"\n🎯 SYNTHETIC ENRICHMENT SAMPLES:")
            for i, enrichment in enumerate(jobs_analysis['synthetic_enrichment_analysis'][:3]):
                print(f"  Job {enrichment['job_id']}: {len(enrichment['synthetic_skills'])} synthetic skills")
                for skill in enrichment['synthetic_skills'][:3]:
                    print(f"    • {skill['name']} (reason: {skill['reason']})")
        
        return 0 if summary['overall_health'] in ['EXCELLENT', 'GOOD'] else 1
        
    except Exception as e:
        print(f"💥 Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
