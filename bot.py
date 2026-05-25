import os
import pandas as pd
from fastapi import FastAPI, Query
from pymongo import MongoClient
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
import uvicorn

# ---------------- CONFIG ----------------
CSV_FILE = "master_cluster_list.csv"
COLUMN_NAME = "Python Connection String"

ACTIVE_COLLECTIONS = []


# ---------------- CONNECT CLUSTER ----------------
def init_cluster(url):
    try:
        client = MongoClient(
            url,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            maxPoolSize=5
        )

        dbs = client.list_database_names()
        dbs = [d for d in dbs if d not in ["admin", "local", "config"]]

        if not dbs:
            return None

        db = client[dbs[0]]
        cols = db.list_collection_names()

        if not cols:
            return None

        return {
            "collection": db[cols[0]],
            "name": url.split('@')[-1].split('.')[0]
        }

    except:
        return None


# ---------------- LIFESPAN STARTUP ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):

    global ACTIVE_COLLECTIONS

    print("🚀 Loading clusters...")

    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip()

    urls = df[COLUMN_NAME].dropna().tolist()

    # safe concurrency for Render
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(init_cluster, url) for url in urls]

        for f in as_completed(futures):
            res = f.result()
            if res:
                ACTIVE_COLLECTIONS.append(res)

    print(f"✅ Clusters loaded: {len(ACTIVE_COLLECTIONS)}")

    yield

    print("🛑 Shutdown")


# ---------------- APP ----------------
app = FastAPI(lifespan=lifespan)


# ---------------- SEARCH BY PHONE ----------------
@app.get("/search")
def search(q: str = Query(..., min_length=3)):

    results = []

    for cluster in ACTIVE_COLLECTIONS:
        try:
            coll = cluster["collection"]

            query_val = int(q) if q.isdigit() else q

            docs = list(
                coll.find(
                    {
                        "$or": [
                            {"phone_number": query_val},
                            {"phone": query_val}
                        ]
                    },
                    {"_id": 0, "phone_number": 1}
                ).limit(5)
            )

            for d in docs:
                d["cluster"] = cluster["name"]

            results.extend(docs)

        except:
            continue

    return {
        "total": len(results),
        "results": results
    }


# ---------------- SEARCH BY _ID ----------------
@app.get("/search-id")
def search_by_id(q: str):

    try:
        oid = ObjectId(q)
    except:
        return {"error": "Invalid ObjectId"}

    results = []

    for cluster in ACTIVE_COLLECTIONS:
        try:
            coll = cluster["collection"]

            doc = coll.find_one(
                {"_id": oid},
                {"_id": 1, "phone_number": 1}
            )

            if doc:
                doc["_id"] = str(doc["_id"])
                doc["cluster"] = cluster["name"]
                results.append(doc)

        except:
            continue

    return {
        "total": len(results),
        "results": results
    }


# ---------------- CLUSTER EXPLORER ----------------
@app.get("/cluster-explore")
def cluster_explore():

    data = []

    for cluster in ACTIVE_COLLECTIONS:
        try:
            coll = cluster["collection"]

            first = list(
                coll.find({}, {"_id": 1, "phone_number": 1}).limit(5)
            )

            last = list(
                coll.find({}, {"_id": 1, "phone_number": 1})
                .sort("_id", -1)
                .limit(5)
            )

            for d in first + last:
                d["_id"] = str(d["_id"])

            data.append({
                "cluster": cluster["name"],
                "first_5": first,
                "last_5": last
            })

        except:
            continue

    return {
        "clusters": len(data),
        "data": data
    }


# ---------------- HEALTH ----------------
@app.get("/")
def home():
    return {
        "status": "running",
        "clusters_loaded": len(ACTIVE_COLLECTIONS)
    }


# ---------------- RUN FOR RENDER ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    uvicorn.run(
        "bot:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
