import os
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo import errors
from pinecone import Pinecone

load_dotenv()

def getting_data():
    dataset=load_dataset("MongoDB/embedded_movies")
    data=pd.DataFrame(dataset["train"])
    data=data.dropna(subset=["fullplot"])
    data=data.drop(columns=["plot_embedding"])
    data=data.sample(300)
    document=data.to_dict("records")
    return document

def mongodb_ingestion(user_name, password, documents):
    # MongoDB connection URI
    uri = f"mongodb+srv://{user_name}:{password}@cluster0.qgta0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    
    # Create a new client and connect to the server
    client = MongoClient(uri)
    
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print("Connection Error:", e)
        return None
    
    # Access database and collection
    db = client["moviemydb"]
    collection = db["moviemycollection"]
    
    # Remove duplicates in 'title' field before creating unique index
    try:
        pipeline = [
            {"$group": {"_id": "$title", "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        
        duplicates = collection.aggregate(pipeline)
        for duplicate in duplicates:
            # Keep one document and delete the rest
            ids_to_delete = duplicate["ids"][1:]  # Keep the first document, delete others
            collection.delete_many({"_id": {"$in": ids_to_delete}})
        print("Duplicates removed successfully.")
    except Exception as e:
        pass

    # Now, create a unique index on the 'title' field
    try:
        collection.create_index("title", unique=True)
        print("Unique index on 'title' created successfully.")
    except errors.OperationFailure as e:
        pass
    
    # Insert documents with duplicate handling
    try:
        collection.insert_many(documents, ordered=False)  # ordered=False skips duplicates and continues
        print("Documents inserted successfully.")
    except errors.BulkWriteError as e:
        print("Some documents were duplicates and were skipped.")
        for error in e.details["writeErrors"]:
            pass  # Print duplicate error details
    
    return collection


def pinecone_connection():
    pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    return pc.Index("mongodb")

def get_index_collection():
    user_name = "karbaaz169"
    password = os.getenv('MONGODB_API_KEY')
    document = getting_data()
    collection = mongodb_ingestion(user_name, password, document)
    index = pinecone_connection()
    return index, collection
