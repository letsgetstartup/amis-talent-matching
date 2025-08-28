#!/usr/bin/env python3
"""
Backfill city data for existing job records.

This script fixes job records that have missing city field data but may have 
city information available in other fields like city_canonical or text_blob.

Usage:
    python scripts/backfill_job_cities.py [--dry-run] [--limit N]
"""

import sys
import re
import json
import time
from pathlib import Path

# Add the talentdb directory to Python path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.ingest_agent import db, canonical_city


def extract_city_from_text_blob(text_blob: str) -> str | None:
    """Extract city from text_blob Location: line"""
    if not text_blob:
        return None
    
    # Look for "Location: City Name" pattern
    location_match = re.search(r'Location:\s*([^\n]+)', text_blob, re.IGNORECASE)
    if location_match:
        city = location_match.group(1).strip()
        # Remove common prefixes
        city = re.sub(r"^\s*(סניף|branch)\s+", "", city, flags=re.IGNORECASE).strip()
        return city if city else None
    
    return None


def backfill_job_cities(dry_run: bool = True, limit: int = None) -> dict:
    """
    Fix existing jobs with missing city data
    
    Args:
        dry_run: If True, only simulate changes without updating database
        limit: Maximum number of records to process (None for all)
    
    Returns:
        Dict with statistics about the operation
    """
    coll = db['jobs']
    
    # Find jobs with missing city but potentially recoverable city data
    query = {
        '$or': [
            {'city': None}, 
            {'city': {'$exists': False}},
            {'city': ''}
        ]
    }
    
    cursor = coll.find(query)
    if limit:
        cursor = cursor.limit(limit)
    
    stats = {
        'processed': 0,
        'updated': 0,
        'no_city_data': 0,
        'already_has_city': 0,
        'sources': {
            'city_canonical': 0,
            'text_blob': 0,
            'job_description': 0
        },
        'sample_updates': []
    }
    
    for job in cursor:
        stats['processed'] += 1
        
        # Skip if already has city
        if job.get('city') and str(job.get('city')).strip():
            stats['already_has_city'] += 1
            continue
        
        city_found = None
        source = None
        
        # Strategy 1: Convert from city_canonical 
        if job.get('city_canonical'):
            city_canonical = job['city_canonical']
            if isinstance(city_canonical, str) and city_canonical.strip():
                # Convert canonical format back to readable format
                city_found = city_canonical.replace('_', ' ').title()
                source = 'city_canonical'
        
        # Strategy 2: Extract from text_blob
        if not city_found and job.get('text_blob'):
            city_found = extract_city_from_text_blob(job['text_blob'])
            if city_found:
                source = 'text_blob'
        
        # Strategy 3: Look in job_description for city mentions
        if not city_found and job.get('job_description'):
            desc = job['job_description']
            # Look for Israeli cities mentioned after "ב" (in/at) prefix
            # Pattern: word boundary + "ב" + Hebrew city name + word boundary or specific delimiters
            city_matches = re.findall(r'\bב([א-ת][א-ת\s]{1,20})(?=\s+דרוש|\s+[א-ת]+/[א-ת]+|\s|,|\.|\n|$)', desc)
            
            for potential_city in city_matches:
                potential_city = potential_city.strip()
                # Filter out common non-city terms and too short/long names
                excluded_terms = ['החברה', 'המפעל', 'הארגון', 'התחום', 'הפארמה', 'הביטוח', 'התחבורה', 
                                'תחום', 'מחלקה', 'אזור', 'המחלקה', 'המשמרת', 'העבודה', 'התפקיד', 'הייצור',
                                'תעשייתית מובילה', 'רה תעשייתית מובילה']
                
                if (3 <= len(potential_city) <= 25 and 
                    potential_city not in excluded_terms and
                    not any(term in potential_city for term in ['דרוש', 'תפקיד', 'עבודה', 'משרה', 'חברה', 'מפעל', 'תעשייתית', 'מובילה'])):
                    city_found = potential_city
                    source = 'job_description'
                    break
        
        if city_found:
            # Validate and normalize the found city
            city_found = city_found.strip()
            if len(city_found) >= 2:
                stats['updated'] += 1
                stats['sources'][source] += 1
                
                # Add to sample for verification (limit to 20 samples)
                if len(stats['sample_updates']) < 20:
                    stats['sample_updates'].append({
                        'job_id': str(job['_id']),
                        'title': job.get('title', '')[:50],
                        'original_city': job.get('city'),
                        'found_city': city_found,
                        'source': source,
                        'city_canonical': job.get('city_canonical')
                    })
                
                if not dry_run:
                    # Actually update the database
                    coll.update_one(
                        {'_id': job['_id']}, 
                        {
                            '$set': {
                                'city': city_found,
                                'updated_at': int(time.time())
                            }
                        }
                    )
            else:
                stats['no_city_data'] += 1
        else:
            stats['no_city_data'] += 1
    
    return stats


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Backfill missing city data for job records')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Simulate changes without updating database (default)')
    parser.add_argument('--execute', action='store_true', 
                        help='Actually perform database updates')
    parser.add_argument('--limit', type=int, 
                        help='Limit number of records to process')
    
    args = parser.parse_args()
    
    # Determine if this is a dry run
    dry_run = not args.execute
    
    if dry_run:
        print("🔍 DRY RUN MODE - No database changes will be made")
        print("Use --execute flag to perform actual updates")
    else:
        print("⚠️  EXECUTE MODE - Database will be updated")
        confirm = input("Are you sure you want to proceed? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Operation cancelled")
            return
    
    print(f"\n🚀 Starting city backfill process...")
    print(f"Limit: {args.limit if args.limit else 'No limit'}")
    
    try:
        stats = backfill_job_cities(dry_run=dry_run, limit=args.limit)
        
        print(f"\n📊 Backfill Results:")
        print(f"Processed: {stats['processed']}")
        print(f"Updated: {stats['updated']}")
        print(f"No city data found: {stats['no_city_data']}")
        print(f"Already had city: {stats['already_has_city']}")
        
        print(f"\n📍 Sources used:")
        for source, count in stats['sources'].items():
            if count > 0:
                print(f"  {source}: {count}")
        
        if stats['sample_updates']:
            print(f"\n🔍 Sample updates:")
            for sample in stats['sample_updates']:
                print(f"  {sample['job_id'][:8]}... | {sample['title']} | {sample['found_city']} (from {sample['source']})")
        
        print(f"\n✅ Backfill completed {'(DRY RUN)' if dry_run else '(EXECUTED)'}")
        
    except Exception as e:
        print(f"❌ Error during backfill: {e}")
        raise


if __name__ == '__main__':
    main()
