import pandas as pd
from fastapi import FastAPI, Query
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import uvicorn
import os

app = FastAPI()

# ---------------- CONFIG ----------------
CSV_FILE = "master_cluster_list.csv"
COLUMN_NAME = "Python Connection String"

ACTIVE_COLLECTIONS = []


# ---------------- CONNECT SINGLE CLUSTER ----------------
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


# ---------------- STARTUP ----------------
@app.on_event("startup")
def startup():
    global ACTIVE_COLLECTIONS

    print("🚀 Loading clusters...")

    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip()

    urls = df[COLUMN_NAME].dropna().tolist()

    # IMPORTANT: limit concurrency to avoid Render crash
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(init_cluster, url) for url in urls]

        for f in as_completed(futures):
            res = f.result()
            if res:
                ACTIVE_COLLECTIONS.append(res)

    print(f"✅ Connected clusters: {len(ACTIVE_COLLECTIONS)}")


# ---------------- SEARCH WORKER ----------------
def search_cluster(cluster, q):
    try:
        coll = cluster["collection"]

        query_val = int(q) if q.isdigit() else q

        results = list(coll.find(
            {
                "$or": [
                    {"phone_number": query_val},
                    {"client_id": query_val}
                ]
            },
            {"_id": 0}
        ).limit(5))

        for r in results:
            r["cluster"] = cluster["name"]

        return results

    except:
        return []


# ---------------- API ----------------
@app.get("/search")
def search(q: str = Query(..., min_length=3)):
    if not ACTIVE_COLLECTIONS:
        return {"error": "No clusters loaded"}

    all_results = []

    # LIMIT threads for Render stability
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [
            executor.submit(search_cluster, c, q)
            for c in ACTIVE_COLLECTIONS
        ]

        for f in as_completed(futures):
            res = f.result()
            if res:
                all_results.extend(res)

    return {
        "total": len(all_results),
        "results": all_results
    }


# ---------------- HEALTH ----------------
@app.get("/")
def home():
    return {
        "status": "running",
        "clusters_loaded": len(ACTIVE_COLLECTIONS)
    }


# ---------------- RUN ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
