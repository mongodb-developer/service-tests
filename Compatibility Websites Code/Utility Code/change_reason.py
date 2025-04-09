#!/usr/bin/env python
from pymongo import MongoClient

def update_description_prefix():
    connection_str = (
        "mongodb+srv://rahulverma:09Ph2007@competitive36.tsnei.mongodb.net/"
        "?retryWrites=true&w=majority&appName=Competitive36"
    )
    client = MongoClient(connection_str)
    db = client["cosmos_web_prod"]
    collection = db["correctness"]

    # The old prefix to remove
    old_prefix = '"errmsg" : "["errmsg" : "errmsg: ['
    
    modified_count = 0
    for doc in collection.find({}):
        if "description" not in doc or not doc["description"]:
            continue

        # Retrieve the description value (if stored as a list, take the first element)
        if isinstance(doc["description"], list):
            desc_val = doc["description"][0]
        elif isinstance(doc["description"], str):
            desc_val = doc["description"]
        else:
            continue

        if not isinstance(desc_val, str):
            continue

        # Check if the description starts with the old prefix.
        if desc_val.startswith(old_prefix):
            # Remove the old prefix.
            remainder = desc_val[len(old_prefix):]
            # Build the new prefix: test_file value enclosed in [] followed by a tab and then "errmsg" : 
            if "test_file" in doc and isinstance(doc["test_file"], str):
                new_prefix = f'[{doc["test_file"]}]\t"errmsg" : '
                new_desc = new_prefix + remainder
                # Update the description field accordingly.
                if isinstance(doc["description"], list):
                    doc["description"][0] = new_desc
                else:
                    doc["description"] = new_desc
                collection.replace_one({"_id": doc["_id"]}, doc)
                modified_count += 1

    print(f"Updated {modified_count} documents.")

if __name__ == "__main__":
    update_description_prefix()
