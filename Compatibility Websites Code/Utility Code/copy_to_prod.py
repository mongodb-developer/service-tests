#!/usr/bin/env python
from pymongo import MongoClient

def main():
    # Connection string for the Atlas cluster
    connection_str = "mongodb+srv://rahulverma:09Ph2007@competitive36.tsnei.mongodb.net/?retryWrites=true&w=majority&appName=Competitive36"
    
    # Connect to the cluster
    client = MongoClient(connection_str)
    
    # Define the target and source databases
    db_target = client["cosmos_web_prod"]
    db_source = client["cosmos_web_prod_fixed"]

    # Collections to process
    collections = ["correctness", "summary"]

    for coll in collections:
        print(f"\nProcessing collection: {coll}")
        
        target_collection = db_target[coll]
        source_collection = db_source[coll]
        
        # Delete all documents in the target collection
        delete_result = target_collection.delete_many({})
        print(f"Deleted {delete_result.deleted_count} documents from '{coll}' in 'cosmos_web_prod'.")
        
        # Retrieve documents from the source collection
        documents = list(source_collection.find({}))
        print(f"Found {len(documents)} documents in '{coll}' in 'cosmos_web_prod_new'.")
        
        # Insert the documents into the target collection if there are any
        if documents:
            insert_result = target_collection.insert_many(documents)
            print(f"Inserted {len(insert_result.inserted_ids)} documents into '{coll}' in 'cosmos_web_prod'.")
        else:
            print(f"No documents to insert for '{coll}'.")

if __name__ == "__main__":
    main()
