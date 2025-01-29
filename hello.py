from fastapi import FastAPI, HTTPException,status
from pymongo import MongoClient
from typing import List
from pydantic import BaseModel
import os
from bson import ObjectId


app = FastAPI()

client = MongoClient("mongodb+srv://javidrahman999:AAw2RmuYLoPBjXUB@cluster0.3h8fxfm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")  # Replace with your MongoDB URI

# client = MongoClient("mongodb+srv://hello:TIFVqEeBuSHT9GLd@cluster0.2z0cwqc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")  # Replace with your MongoDB URI

db = client["corporate_caters_v2"]


@app.get("/collection/{collection_name}")
async def get_all_documents(collection_name: str):
    collection = db[collection_name]
    
   
    documents = list(collection.find({}))
    
    if not documents:
        raise HTTPException(status_code=404, detail="Collection not found or is empty.")
    
    
    result = []
    for document in documents:
        
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
        result.append(document)
    print(result)
    return result




@app.delete("/collection/{collection_name}")
async def delete_collection(collection_name: str):
    
    if collection_name not in db.list_collection_names():
        raise HTTPException(status_code=404, detail="Collection not found.")
    
    
    db.drop_collection(collection_name)
    
    return {"message": f"Collection '{collection_name}' has been deleted successfully."}


@app.delete("/delete_category")
def delete_category():
    db.category.delete_many({"name": {"$ne": "General"}})  
    return {"message": "All categories except 'General' have been deleted"}

@app.put('/updatemenu')
def update_menu():
    db.menus.update_many({}, {"$set": {"categoryId":"672c965b5a8baf6275cf34d0"}})
    return {"message": "All categories have been updated to 'General'"}


class CopyCollectionRequest(BaseModel):
    source_db: str
    source_collection: str
    target_db: str
    target_collection: str

@app.post("/copy-collection", summary="Copy documents from a collection in one database to another collection in a different database")
async def copy_collection(data: CopyCollectionRequest):
    try:
        
        source_db_name = data.source_db
        source_collection_name = data.source_collection
        target_db_name = data.target_db
        target_collection_name = data.target_collection

        
        if source_collection_name not in client[source_db_name].list_collection_names():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source collection not found")

        
        client[source_db_name][source_collection_name].aggregate([
            {"$match": {}},  
            {"$merge": {
                "into": {
                    "db": target_db_name,
                    "coll": target_collection_name
                },
                "whenMatched": "keepExisting",  
                "whenNotMatched": "insert"      
            }}
        ])

        return {"message": f"Documents copied successfully from {source_db_name}.{source_collection_name} to {target_db_name}.{target_collection_name}"}
    
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)









