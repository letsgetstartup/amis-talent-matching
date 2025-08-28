"""
Script to remove all job data from the MongoDB 'jobs' collection.
"""
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "talent_match")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

result = db.jobs.delete_many({})
print(f"Deleted {result.deleted_count} documents from 'jobs' collection.")
