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

def load_all_clusters():
    """Reads all 150 URLs from your CSV file"""
    try:
        df = pd.read_csv(CSV_FILE)
        df.columns = df.columns.str.strip() # Remove hidden spaces in headers
        if COLUMN_NAME in df.columns:
            urls = df[COLUMN_NAME].dropna().tolist()
            print(f"✅ Loaded {len(urls)} clusters.")
            return urls
        else:
            print(f"❌ Error: Could not find column '{COLUMN_NAME}'")
            return []
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return []

CLUSTER_URLS = load_all_clusters()

def search_single_cluster(url, search_query):
    """Searches one cluster automatically finding DBs and Collections"""
    client = None
    try:
        # 3-second timeout so offline clusters don't slow us down
        client = MongoClient(url, serverSelectionTimeoutMS=3000)
        
        # 1. Get all databases (ignoring system ones)
        all_dbs = client.list_database_names()
        user_dbs = [d for d in all_dbs if d not in ['admin', 'local', 'config']]
        
        cluster_results = []

        for db_name in user_dbs:
            db = client[db_name]
            # 2. Get all collections in this database
            collections = db.list_collection_names()
            
            for coll_name in collections:
                collection = db[coll_name]
                
                # 3. Search logic for the 'phone' field
                # Checks for exact string, exact integer, and partial match
                conditions = [{"phone": search_query}]
                
                try:
                    # If query is a number, search for integer version too
                    conditions.append({"phone": int(search_query)})
                except: pass
                
                # Search for partial matches (regex)
                conditions.append({"phone": {"$regex": str(search_query), "$options": "i"}})

                results = list(collection.find({"$or": conditions}).limit(5))
                
                for res in results:
                    res['_id'] = str(res['_id']) # Convert ObjectId to string
                    res['found_in_db'] = db_name
                    res['found_in_coll'] = coll_name
                    res['source_cluster'] = url.split('@')[-1].split('/')[0]
                    cluster_results.append(res)

        return cluster_results
    except Exception:
        return [] # Ignore errors for dead clusters
    finally:
        if client: client.close()

@app.get("/search")
def search_api(q: str = Query(..., min_length=3)):
    """
    Search endpoint: /search?q=7678685516
    """
    if not CLUSTER_URLS:
        return {"error": "CSV file not found or empty."}

    all_results = []
    
    # max_workers=150 ensures we hit every cluster at once
    with ThreadPoolExecutor(max_workers=len(CLUSTER_URLS)) as executor:
        futures = [executor.submit(search_single_cluster, url, q) for url in CLUSTER_URLS]
        
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_results.extend(res)
                
    return {
        "search_term": q,
        "clusters_searched": len(CLUSTER_URLS),
        "total_results": len(all_results),
        "results": all_results
    }

@app.get("/")
def home():
    return {"status": "Bot is Online", "total_clusters": len(CLUSTER_URLS)}

if __name__ == "__main__":
    # Get port for Render deployment
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
