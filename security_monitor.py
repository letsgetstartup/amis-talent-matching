#!/usr/bin/env python3
"""
Security Monitor Script
Monitors and reports any unauthorized database modifications
"""
import time
import logging
import sys
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).resolve().parent
if str(ROOT / "talentdb") not in sys.path:
    sys.path.insert(0, str(ROOT / "talentdb"))

from scripts.ingest_agent import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def monitor_collections():
    """Monitor key collections for unexpected changes"""
    collections_to_monitor = ['jobs', 'candidates', '_vocab_skills', '_vocab_titles']
    
    # Get initial counts
    initial_counts = {}
    for coll_name in collections_to_monitor:
        try:
            count = db[coll_name].count_documents({})
            initial_counts[coll_name] = count
            logging.info(f"📊 Initial {coll_name} count: {count}")
        except Exception as e:
            logging.error(f"❌ Error checking {coll_name}: {e}")
            initial_counts[coll_name] = 0
    
    return initial_counts

def check_for_changes(baseline_counts):
    """Check if any monitored collections have changed"""
    changes_detected = False
    
    for coll_name, baseline in baseline_counts.items():
        try:
            current_count = db[coll_name].count_documents({})
            if current_count != baseline:
                logging.warning(f"🚨 SECURITY ALERT: {coll_name} changed from {baseline} to {current_count}")
                changes_detected = True
                
                # Update baseline for next check
                baseline_counts[coll_name] = current_count
                
        except Exception as e:
            logging.error(f"❌ Error monitoring {coll_name}: {e}")
    
    return changes_detected

def main():
    logging.info("🔒 Security Monitor Starting...")
    
    # Establish baseline
    baseline = monitor_collections()
    
    if baseline['jobs'] > 0:
        logging.warning(f"⚠️  Database not empty - {baseline['jobs']} jobs already present")
    else:
        logging.info("✅ Database is empty - monitoring for unauthorized changes")
    
    try:
        while True:
            time.sleep(10)  # Check every 10 seconds
            if check_for_changes(baseline):
                logging.warning("🔍 Investigate above changes immediately!")
            
    except KeyboardInterrupt:
        logging.info("🛑 Security Monitor stopped by user")

if __name__ == "__main__":
    main()
