#!/usr/bin/env python3
"""
Job enrichment script to prepare CSV-imported jobs for matching readiness.

This script:
1. Normalizes skills to canonical form
2. Splits compound skills (e.g., "Python ו-JavaScript" -> ["python", "javascript"])
3. Generates synthetic skills for better matching
4. Classifies skills into must/needed categories
5. Ensures minimum skill count for effective matching
"""

import os
import sys
import re
from typing import Dict, List, Set, Any

# When running as a script, ensure package path is set; for package import use relatives
pkg_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if pkg_root not in sys.path:
    sys.path.insert(0, pkg_root)

from .db import get_db
from .ingest_agent import canonical_skill, _materialize_skill_set

def normalize_compound_skills(skill_text: str) -> List[str]:
    """
    Split compound Hebrew/English skills into individual canonical skills.
    
    Examples:
    "Python ו-JavaScript" -> ["python", "javascript"]
    "Photoshop ו-Illustrator" -> ["photoshop", "illustrator"]
    """
    # Hebrew connectors: ו (and), עם (with), או (or)
    # English connectors: and, with, or, &, +, /
    connectors = r'\s*(?:ו-?|עם|או|and|with|or|\&|\+|/|,)\s*'
    
    # Split by connectors and clean each part
    parts = re.split(connectors, skill_text, flags=re.IGNORECASE)
    normalized = []
    
    for part in parts:
        cleaned = part.strip().strip('-').strip()
        if len(cleaned) >= 2:  # Minimum skill name length
            normalized.append(canonical_skill(cleaned))
    
    return normalized

def generate_role_based_synthetic_skills(title: str, existing_skills: Set[str]) -> List[str]:
    """
    Generate synthetic skills based on job title patterns.
    """
    title_lower = title.lower()
    synthetic = []
    
    # Common skill mappings by role type
    role_skills = {
        'מפתח': ['git', 'api', 'database', 'testing', 'debugging'],
        'תוכנה': ['algorithms', 'data_structures', 'version_control', 'code_review'],
        'developer': ['git', 'api', 'database', 'testing', 'debugging'],
        'software': ['algorithms', 'data_structures', 'version_control', 'code_review'],
        'מעצב': ['creativity', 'color_theory', 'typography', 'user_interface'],
        'גרפי': ['adobe_creative_suite', 'layout_design', 'branding', 'visual_design'],
        'designer': ['creativity', 'color_theory', 'typography', 'user_interface'],
        'graphic': ['adobe_creative_suite', 'layout_design', 'branding', 'visual_design'],
        'qa': ['test_automation', 'bug_tracking', 'quality_assurance', 'regression_testing'],
        'engineer': ['problem_solving', 'technical_documentation', 'system_design'],
        'הנדס': ['problem_solving', 'technical_documentation', 'system_design'],
        'מנהל': ['project_management', 'team_leadership', 'communication', 'planning'],
        'manager': ['project_management', 'team_leadership', 'communication', 'planning']
    }
    
    # Find matching role patterns
    for role_pattern, skills in role_skills.items():
        if role_pattern in title_lower:
            for skill in skills:
                if skill not in existing_skills and len(synthetic) < 8:
                    synthetic.append(skill)
    
    # Add general tech skills if this looks like a tech role
    tech_indicators = ['מפתח', 'תוכנה', 'developer', 'software', 'engineer', 'הנדס', 'qa', 'tech']
    if any(indicator in title_lower for indicator in tech_indicators):
        general_tech = ['computer_science', 'software_development', 'technical_skills', 'problem_solving']
        for skill in general_tech:
            if skill not in existing_skills and len(synthetic) < 12:
                synthetic.append(skill)
    
    return synthetic[:10]  # Limit to 10 synthetic skills

def enrich_job(job_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich a job document with proper skill normalization and categorization.
    """
    print(f"Enriching job: {job_doc.get('title', 'Unknown')} (ID: {job_doc.get('_id')})")
    
    # Step 1: Normalize and expand compound skills
    raw_requirements = job_doc.get('job_requirements', [])
    must_skills = []
    
    for req in raw_requirements:
        if isinstance(req, str):
            normalized_skills = normalize_compound_skills(req)
            for skill in normalized_skills:
                if skill not in [s['name'] for s in must_skills]:
                    must_skills.append({'name': skill, '_source': 'csv_normalized'})
    
    print(f"  Normalized skills: {[s['name'] for s in must_skills]}")
    
    # Step 2: Generate synthetic skills
    existing_skill_names = set(s['name'] for s in must_skills)
    synthetic_skills = generate_role_based_synthetic_skills(
        job_doc.get('title', ''), 
        existing_skill_names
    )
    
    print(f"  Added synthetic skills: {synthetic_skills}")
    
    # Step 3: Build requirements structure
    nice_to_have = []
    synthetic_objs = []
    
    for skill in synthetic_skills:
        nice_to_have.append({'name': skill, '_source': 'synthetic'})
        synthetic_objs.append({'name': skill, 'reason': 'role_pattern'})
    
    requirements = {
        'must_have_skills': must_skills,
        'nice_to_have_skills': nice_to_have
    }
    
    # Step 4: Build skills_detailed with proper categorization
    skills_detailed = []
    
    # Must-have skills (high weight)
    for skill_obj in must_skills:
        skills_detailed.append({
            'name': skill_obj['name'],
            'category': 'must',
            'source': skill_obj.get('_source', 'extracted'),
            'confidence': 0.9,
            'weight': 1.0,
            'level': None,
            'years_experience': None,
            'last_used_year': None,
            'evidence': 'job_requirements'
        })
    
    # Nice-to-have skills (lower weight)
    for skill_obj in nice_to_have:
        skills_detailed.append({
            'name': skill_obj['name'],
            'category': 'needed',
            'source': skill_obj.get('_source', 'synthetic'),
            'confidence': 0.6,
            'weight': 0.7,
            'level': None,
            'years_experience': None,
            'last_used_year': None,
            'evidence': 'synthetic_generation'
        })
    
    # Step 5: Update job document
    all_skills = existing_skill_names.union(set(synthetic_skills))
    
    updates = {
        'requirements': requirements,
        'synthetic_skills': synthetic_objs,
        'skills_detailed': skills_detailed,
        'skill_set': sorted(list(all_skills)),
        'synthetic_skills_generated': len(synthetic_skills),
        'enrichment_status': 'completed',
        'enrichment_version': '1.0'
    }
    
    return updates

def main():
    """
    Main enrichment process for tenant jobs.
    """
    db = get_db()
    
    # Find jobs that need enrichment (have tenant_id but missing skills_detailed)
    query = {
        'tenant_id': {'$ne': None},
        'skills_detailed': {'$exists': False}
    }
    
    jobs_to_enrich = list(db['jobs'].find(query))
    
    if not jobs_to_enrich:
        print("No jobs found that need enrichment.")
        return
    
    print(f"Found {len(jobs_to_enrich)} jobs to enrich")
    
    for job in jobs_to_enrich:
        try:
            updates = enrich_job(job)
            
            # Update the database
            result = db['jobs'].update_one(
                {'_id': job['_id']},
                {'$set': updates}
            )
            
            if result.modified_count > 0:
                print(f"  ✅ Successfully enriched job {job['_id']}")
            else:
                print(f"  ⚠️ No changes made to job {job['_id']}")
                
        except Exception as e:
            print(f"  ❌ Error enriching job {job['_id']}: {e}")
    
    print(f"\nEnrichment complete. Processed {len(jobs_to_enrich)} jobs.")
    
    # Verify results
    enriched_count = db['jobs'].count_documents({
        'tenant_id': {'$ne': None},
        'skills_detailed': {'$exists': True}
    })
    print(f"Total enriched tenant jobs: {enriched_count}")

if __name__ == '__main__':
    main()
