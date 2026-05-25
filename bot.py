import os
import pandas as pd
from fastapi import FastAPI, Query
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
import uvicorn

# ---------------- CONFIG ----------------
CSV_FILE = "master_cluster_list.csv"
COLUMN_NAME = "Python Connection String"

ACTIVE_COLLECTIONS = []

# ---------------- CLUSTER INIT ----------------
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


# ---------------- LIFESPAN (NEW FASTAPI WAY) ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global ACTIVE_COLLECTIONS

    print("🚀 Starting cluster load...")

    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip()

    urls = df[COLUMN_NAME].dropna().tolist()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(init_cluster, url) for url in urls]

        for f in as_completed(futures):
            res = f.result()
            if res:
                ACTIVE_COLLECTIONS.append(res)

    print(f"✅ Loaded clusters: {len(ACTIVE_COLLECTIONS)}")

    yield

    print("🛑 Shutting down...")


# ---------------- APP ----------------
app = FastAPI(lifespan=lifespan)


# ---------------- SEARCH ----------------
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


@app.get("/search")
def search(q: str = Query(..., min_length=3)):
    if not ACTIVE_COLLECTIONS:
        return {"error": "Clusters not loaded yet"}

    all_results = []

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


@app.get("/")
def health():
    return {
        "status": "running",
        "clusters": len(ACTIVE_COLLECTIONS)
    }


# ---------------- RUN (IMPORTANT FOR RENDER) ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
