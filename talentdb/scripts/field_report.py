import collections
from .db import get_db

db=get_db()

def report(coll):
    docs=list(db[coll].find({}, {"canonical":1,"title":1,"skills":1}).limit(500))
    cov=collections.Counter()
    for d in docs:
        c=d.get("canonical",{})
        if isinstance(c, dict):
            for k in c.keys():
                cov[k]+=1
    print(coll, "coverage:", dict(cov))

if __name__ == "__main__":
    report("candidates")
    report("jobs")
