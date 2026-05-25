import os
import pandas as pd
from fastapi import FastAPI, Query
from pymongo import MongoClient, ASCENDING
import uvicorn

app = FastAPI()

# ---------------- CONFIG ----------------
CSV_FILE = "master_cluster_list.csv"
COLUMN_NAME = "Python Connection String"

MONGO_URI = os.environ.get("MONGO_URI")  # set in Render env
DB_NAME = "central_db"
COLLECTION_NAME = "clients"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


# ---------------- INDEX SETUP ----------------
def create_indexes():
    collection.create_index([("phone_number", ASCENDING)])
    collection.create_index([("client_id", ASCENDING)])
    print("✅ Indexes created")


# ---------------- LOAD CSV TO MONGO ----------------
def load_csv_to_mongo():
    if collection.estimated_document_count() > 0:
        print("⚡ Data already exists, skipping load")
        return

    df = pd.read_csv(CSV_FILE)
    df.columns = df.columns.str.strip()

    records = []

    for _, row in df.iterrows():
        records.append({
            "client_id": str(row.get("client_id", "")).strip(),
            "phone_number": str(row.get("phone_number", "")).strip(),
            "cluster_name": str(row.get(COLUMN_NAME, "")).strip(),
            "raw_data": row.to_dict()
        })

    if records:
        collection.insert_many(records)
        print(f"✅ Inserted {len(records)} records")


# ---------------- STARTUP EVENT ----------------
@app.on_event("startup")
def startup():
    print("🚀 Starting API...")
    create_indexes()
    load_csv_to_mongo()
    print("✅ Ready on Render")


# ---------------- SEARCH API ----------------
@app.get("/search")
def search(
    q: str = Query(..., min_length=3),
    mode: str = "auto"  # auto | phone | id
):
    query = {}

    if mode == "phone" or q.isdigit():
        query = {"phone_number": q}

    elif mode == "id":
        query = {"client_id": q}

    else:
        query = {
            "$or": [
                {"phone_number": q},
                {"client_id": q}
            ]
        }

    results = list(collection.find(query, {"_id": 0}).limit(50))

    return {
        "total": len(results),
        "results": results
    }


# ---------------- HEALTH CHECK ----------------
@app.get("/")
def health():
    return {
        "status": "online",
        "records": collection.estimated_document_count()
    }


# ---------------- RUN ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
