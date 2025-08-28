import sys, os, json
from bson import ObjectId
from .db import get_db

kind, oid = sys.argv[1], sys.argv[2]

db=get_db()
col=db["candidates"] if kind=="candidate" else db["jobs"]

doc=col.find_one({"_id": ObjectId(oid)}) or col.find_one({"_id": oid})
assert doc
out={"canonical": doc.get("canonical") or {
    "title": doc.get("title"),
    "requirements": doc.get("requirements")
}, "context": (doc.get("text_blob") or doc.get("description") or "")[:4000]}
print(json.dumps(out, separators=(",",":")))
