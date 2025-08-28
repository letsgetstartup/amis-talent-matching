#!/usr/bin/env python3
"""
Database Cleanup and Test Script
Safely cleans database and prepares for testing
"""
import sys
import os
import subprocess
import time
from pathlib import Path

# Project setup
ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

def connect_to_db():
    """Connect to MongoDB"""
    try:
        sys.path.insert(0, str(ROOT / "talentdb"))
        from scripts.ingest_agent import db
        return db
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def clean_database(db):
    """Safely clean all collections"""
    collections_to_clean = ['jobs', 'candidates', 'jobs_versions']
    
    print("🧹 Cleaning database...")
    for coll_name in collections_to_clean:
        try:
            count_before = db[coll_name].count_documents({})
            if count_before > 0:
                result = db[coll_name].delete_many({})
                print(f"   ✅ Deleted {result.deleted_count} documents from {coll_name}")
            else:
                print(f"   ✅ {coll_name} already empty")
        except Exception as e:
            print(f"   ❌ Error cleaning {coll_name}: {e}")

def verify_clean_state(db):
    """Verify database is completely clean"""
    collections = ['jobs', 'candidates']
    all_clean = True
    
    print("\n🔍 Verifying clean state...")
    for coll_name in collections:
        try:
            count = db[coll_name].count_documents({})
            if count == 0:
                print(f"   ✅ {coll_name}: empty ({count} documents)")
            else:
                print(f"   ❌ {coll_name}: NOT EMPTY ({count} documents)")
                all_clean = False
        except Exception as e:
            print(f"   ❌ Error checking {coll_name}: {e}")
            all_clean = False
    
    return all_clean

def check_server_status():
    """Check if server is running"""
    try:
        import requests
        response = requests.get("http://localhost:8085/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def restart_server():
    """Restart the server"""
    print("\n🔄 Restarting server...")
    
    # Kill existing server if running
    try:
        subprocess.run(["pkill", "-f", "run_server.py"], check=False)
        time.sleep(2)
    except:
        pass
    
    # Start new server
    try:
        server_script = ROOT / "run_server.py"
        if server_script.exists():
            print("   🚀 Starting server...")
            subprocess.Popen([sys.executable, str(server_script)], 
                           stdout=subprocess.DEVNULL, 
                           stderr=subprocess.DEVNULL)
            time.sleep(3)
            
            if check_server_status():
                print("   ✅ Server started successfully on port 8085")
                return True
            else:
                print("   ❌ Server failed to start or not responding")
                return False
        else:
            print("   ❌ Server script not found")
            return False
    except Exception as e:
        print(f"   ❌ Error starting server: {e}")
        return False

def main():
    print("🔒 DATABASE CLEANUP AND TEST PREPARATION")
    print("=" * 60)
    
    # Connect to database
    db = connect_to_db()
    if not db:
        print("❌ Cannot proceed without database connection")
        return 1
    
    # Clean database
    clean_database(db)
    
    # Verify clean state
    if not verify_clean_state(db):
        print("\n❌ Database cleanup failed!")
        return 1
    
    # Restart server
    if not restart_server():
        print("\n❌ Server restart failed!")
        return 1
    
    print("\n" + "=" * 60)
    print("✅ CLEANUP COMPLETE - READY FOR TESTING")
    print("\nNext steps:")
    print("1. Database is completely empty")
    print("2. Server is running on port 8085")
    print("3. Try uploading jobs via the UI")
    print("4. Verify NO sample data auto-loads")
    print("5. Check logs for any 🚨 security alerts")
    print("\n🔍 Monitor with: python security_check.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
