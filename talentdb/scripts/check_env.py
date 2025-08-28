"""Validate required environment secrets without printing their values."""
import os, sys
REQUIRED = ["OPENAI_API_KEY"]
missing=[k for k in REQUIRED if not os.getenv(k)]
if missing:
    print("MISSING:"+",".join(missing))
    sys.exit(1)
print("ENV_OK")
