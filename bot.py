import pandas as pd
from fastapi import FastAPI, Query
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import uvicorn

app = FastAPI()

# Load your 150 URLs from the CSV
# Make sure this file exists in your project root
try:
    df = pd.read_csv('master_cluster_list.csv')
    CLUSTER_URLS = df['url'].tolist() 
except Exception as e:
    print(f"Error loading CSV: {e}")
    CLUSTER_URLS = []

def search_single_cluster(url, search_query):
    """Function to search a single MongoDB cluster"""
    try:
        # Set a short timeout (3-5 seconds) so one dead cluster doesn't hang the whole API
        client = MongoClient(url, serverSelectionTimeoutMS=3000)
        
        # IMPORTANT: Change these to your actual Database and Collection names
        db = client['your_database_name'] 
        collection = db['your_collection_name']
        
        # Note: This requires a Text Index in MongoDB to work.
        # If you don't have one, use: collection.find({"field_name": {"$regex": search_query}})
        results = list(collection.find({"$text": {"$search": search_query}}).limit(5))
        
        # Clean up IDs for JSON serialization
        for res in results:
            res['_id'] = str(res['_id'])
            res['source_cluster'] = url[:25] + "..." # Identify source cluster safely
            
        client.close()
        return results
    except Exception:
        return [] # Return empty if cluster is down or login fails

# FIX: Changed @get to @app.get
@app.get("/search")
def search_all(q: str = Query(..., min_length=3)):
    all_results = []
    
    # Use ThreadPoolExecutor to search all clusters at the same time
    # max_workers=50 allows 50 clusters to be queried simultaneously 
    with ThreadPoolExecutor(max_workers=50) as executor:
        # Create a list of tasks
        futures = [executor.submit(search_single_cluster, url, q) for url in CLUSTER_URLS]
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_results.extend(result)
                
    return {"query": q, "total_found": len(all_results), "results": all_results}

# FIX: Changed name to __name__
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
