# test_connections.py

from pymongo import MongoClient
import config

def test_connection(uri, db_name, description):
    try:
        client = MongoClient(uri)
        db = client[db_name]
        # Attempt to list collections to verify connection
        collections = db.list_collection_names()
        print(f"Connected to {description} '{db_name}'. Collections: {collections}")
        client.close()
    except Exception as e:
        print(f"Failed to connect to {description} '{db_name}': {e}")

if __name__ == '__main__':
    print("Testing MongoDB Atlas Connection:")
    test_connection(config.MONGODB_URI, config.MONGODB_DB_NAME, "MongoDB Atlas")
    
    print("\nTesting Amazon DocumentDB Connection:")
    test_connection(config.DOCDB_URI, config.DOCDB_DB_NAME, "Amazon DocumentDB")
    
    print("\nTesting Results DB Connection:")
    test_connection(config.RESULT_DB_URI, config.RESULT_DB_NAME, "Results MongoDB Atlas")
