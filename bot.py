import pandas as pd
from fastapi import FastAPI, Query
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import uvicorn
import os

app = FastAPI()

# --- CONFIGURATION ---
CSV_FILE = 'master_cluster_list.csv'
COLUMN_NAME = 'Python Connection String'

# We will store the actual database objects here
# This stays in memory so search is instant
ACTIVE_COLLECTIONS = []

def initialize_single_cluster(url):
    """Connects to a cluster and finds the data collection once"""
    try:
        # We use a long-lived client
        client = MongoClient(url, 
                             serverSelectionTimeoutMS=5000, 
                             maxPoolSize=10, 
                             minPoolSize=1)
        
        dbs = client.list_database_names()
        user_dbs = [d for d in dbs if d not in ['admin', 'local', 'config']]
        if not user_dbs: return None
        
        db = client[user_dbs[0]]
        colls = db.list_collection_names()
        if not colls: return None
        
        # Return the collection object directly
        return {
            "collection": db[colls[0]],
            "name": url.split('@')[-1].split('.')[0]
        }
    except:
        return None

@app.on_event("startup")
def startup_event():
    """Warms up connections. Render friendly: lower concurrency during boot."""
    global ACTIVE_COLLECTIONS
    print("🚀 Connecting to clusters...")
    try:
        df = pd.read_csv(CSV_FILE)
        df.columns = df.columns.str.strip()
        urls = df[COLUMN_NAME].dropna().tolist()
        
        # Use only 20 workers during startup to prevent Render 'port' error
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(initialize_single_cluster, url) for url in urls]
            for future in as_completed(futures):
                res = future.result()
                if res:
                    ACTIVE_COLLECTIONS.append(res)
                    
        print(f"✅ Successfully connected to {len(ACTIVE_COLLECTIONS)} clusters.")
    except Exception as e:
        print(f"❌ Startup Error: {e}")

def search_worker(cluster_data, q):
    """Searches using an already open connection"""
    try:
        coll = cluster_data['collection']
        
        # Search for exact string or exact integer (FASTEST with index)
        query_val = q
        if q.isdigit():
            query_val = int(q)
            
        # Standard query (No Regex for speed)
        results = list(coll.find({"phone": query_val}).limit(5))
        
        processed = []
        for r in results:
            r['_id'] = str(r['_id'])
            r['cluster'] = cluster_data['name']
            processed.append(r)
        return processed
    except:
        return []

@app.get("/search")
def search_api(q: str = Query(..., min_length=3)):
    """The actual search is now extremely fast because connections are already open"""
    if not ACTIVE_COLLECTIONS:
        return {"error": "Clusters not connected yet."}

    all_results = []
    
    # max_workers=50 is the "sweet spot" for Render to prevent crashes
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(search_worker, cluster, q) for cluster in ACTIVE_COLLECTIONS]
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_results.extend(res)
                
    return {"total": len(all_results), "results": all_results}

@app.get("/")
def health():
    return {"status": "online", "clusters": len(ACTIVE_COLLECTIONS)}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
