#!/usr/bin/env python3
"""
Outreach Failure Analysis Script
Analyzes failure patterns in the outreach generation system
"""
import sys
import os
from collections import defaultdict, Counter
from datetime import datetime

# Add the talentdb directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'talentdb'))

from scripts.ingest_agent import db

def analyze_failures():
    """Analyze all outreach failures and generate diagnostic report"""
    
    print("=== מערכת ניתוח כשלונות יצירת הודעות וואטסאפ ===")
    print("Outreach Failure Analysis System")
    print()
    
    # Get all failures
    failures = list(db.outreach_failures.find().sort('ts', -1))
    total_failures = len(failures)
    
    if total_failures == 0:
        print("✅ אין כשלונות ביומן - המערכת עובדת בצורה מושלמת!")
        print("No failures logged - system working perfectly!")
        return
    
    print(f"📊 סך הכל כשלונות: {total_failures}")
    print(f"Total failures: {total_failures}")
    print()
    
    # Analyze by stage
    stage_stats = Counter(f['stage'] for f in failures)
    print("🔍 ניתוח כשלונות לפי שלב:")
    print("Failure breakdown by stage:")
    for stage, count in stage_stats.most_common():
        percentage = (count / total_failures) * 100
        print(f"  {stage}: {count} כשלונות ({percentage:.1f}%)")
    print()
    
    # Analyze by error type  
    error_stats = Counter(f['error'][:100] for f in failures)  # Truncate long errors
    print("🚨 ניתוח כשלונות לפי סוג שגיאה:")
    print("Failure breakdown by error type:")
    for error, count in error_stats.most_common(10):
        percentage = (count / total_failures) * 100
        print(f"  {error[:80]}{'...' if len(error) > 80 else ''}: {count} ({percentage:.1f}%)")
    print()
    
    # Analyze by candidate
    candidate_stats = Counter(f['candidate_id'] for f in failures)
    print("👤 מועמדים עם הכי הרבה כשלונות:")
    print("Candidates with most failures:")
    for candidate_id, count in candidate_stats.most_common(5):
        print(f"  {candidate_id}: {count} כשלונות")
    print()
    
    # Recent failures (last 10)
    print("🕒 10 הכשלונות האחרונות:")
    print("Last 10 failures:")
    for i, failure in enumerate(failures[:10], 1):
        ts = datetime.fromtimestamp(failure['ts'])
        print(f"  {i}. {ts.strftime('%Y-%m-%d %H:%M:%S')} - {failure['stage']} - {failure['candidate_id'][:12]}...")
        print(f"     שגיאה: {failure['error'][:100]}")
    print()
    
    # Recommendations
    print("💡 המלצות לתיקון:")
    print("Recommendations:")
    
    if stage_stats.get('fetch_candidate', 0) > total_failures * 0.3:
        print("  - בדוק תקינות של מזהי מועמדים (ObjectId format)")
        print("  - Check candidate ID validity (ObjectId format)")
    
    if stage_stats.get('llm_call', 0) > 0:
        print("  - בדוק חיבור ל-OpenAI API")
        print("  - Check OpenAI API connection")
        
    if stage_stats.get('json_parse', 0) > 0:
        print("  - בדוק את תגובות ה-LLM ושפר את ה-prompts")
        print("  - Check LLM responses and improve prompts")
    
    print()
    print("📝 ליצירת דוח מפורט יותר הפעל: python analyze_failures.py --detailed")

def main():
    if '--detailed' in sys.argv:
        analyze_failures_detailed()
    else:
        analyze_failures()

def analyze_failures_detailed():
    """Detailed analysis with raw data samples"""
    failures = list(db.outreach_failures.find().sort('ts', -1))
    
    print("=== דוח מפורט - דגימות נתונים גולמיים ===")
    print("Detailed Report - Raw Data Samples")
    print()
    
    # Group by stage and show samples
    stage_groups = defaultdict(list)
    for failure in failures:
        stage_groups[failure['stage']].append(failure)
    
    for stage, stage_failures in stage_groups.items():
        print(f"🔧 שלב: {stage} ({len(stage_failures)} כשלונות)")
        print(f"Stage: {stage} ({len(stage_failures)} failures)")
        
        # Show unique error samples
        unique_errors = {}
        for f in stage_failures[:5]:  # Max 5 samples per stage
            error_key = f['error'][:50]
            if error_key not in unique_errors:
                unique_errors[error_key] = f
        
        for error_key, sample in unique_errors.items():
            print(f"  דוגמה: {sample['error']}")
            if sample.get('raw_response'):
                print(f"  תגובה גולמית: {str(sample['raw_response'])[:200]}...")
            if sample.get('prompt'):
                print(f"  Prompt: {str(sample['prompt'])[:200]}...")
            print("  ---")
        print()

if __name__ == "__main__":
    main()
