#!/usr/bin/env python3
"""
Simple script to run the API server from the correct directory with proper imports
"""
import sys
import os

# Add the talentdb directory to the Python path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'talentdb'))

# Now start the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("scripts.api:app", host="0.0.0.0", port=8000, reload=True)
