from pymongo import MongoClient
import os

# MongoDB connection URI (replace <db_password> with your actual password)
MONGO_URI = "mongodb+srv://rahulverma:09Ph2007@competitive36.tsnei.mongodb.net/?retryWrites=true&w=majority&appName=Competitive36"

# Connect to MongoDB Atlas
client = MongoClient(MONGO_URI)

# Define source and destination databases
source_db_name = "cosmos_web_prod"
target_db_name = "cosmos_web_prod_old_results"

# Get the source and target databases
source_db = client[source_db_name]
target_db = client[target_db_name]

# Copy all collections
for collection_name in source_db.list_collection_names():
    source_collection = source_db[collection_name]
    target_collection = target_db[collection_name]
    
    # Copy documents
    documents = list(source_collection.find({}))  # Fetch all documents
    if documents:  # Insert only if there are documents
        target_collection.insert_many(documents)
    
    print(f"Copied collection: {collection_name} ({len(documents)} documents)")

print("Database copy completed successfully!")

# Close the connection
client.close()
