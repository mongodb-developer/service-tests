#!/usr/bin/env python
import unittest
import sys
from pymongo import MongoClient
import config
import logging
import pandas as pd
import time
from bson import ObjectId
from datetime import datetime
import os
import glob
import shutil
import subprocess
import re

# Import the generate_compatibility_report function
from compatibility_score import generate_compatibility_report

FIELDS_TO_KEEP = {
    "_id": 1,
    "test_file": 1,
    "status": 1,
    "exit_code": 1,
    "start": 1,
    "end": 1,
    "elapsed": 1,
    "suite": 1,
    "platform": 1,
    "version": 1,
    "run": 1,
    "processed": 1,
    "log_lines": 1,
    "description": 1,
    "reason": 1
}

def run_tests():
    """
    Discover and run all tests in the 'tests' directory.
    """
    loader = unittest.TestLoader()
    suite = loader.discover('tests', pattern='test_*.py')
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result

def save_report_to_excel(run_timestamp):
    """
    Connect to the results database, retrieve test results, and save the report to an Excel file
    in the Results folder with a subfolder named after the current date and time.
    """
    try:
        client = MongoClient(config.RESULT_DB_URI)
        db = client[config.RESULT_DB_NAME]
        collection = db[config.RESULT_COLLECTION_NAME]
    except Exception as e:
        print(f"Error connecting to the results database: {e}")
        sys.exit(1)

    results = list(collection.find())
    if not results:
        print("No test results found in the 'correctness' collection.")
        sys.exit(1)

    df = pd.DataFrame(results)

    if 'test_name' in df.columns:
        df['Test Name'] = df['test_name']
    elif 'test_file' in df.columns:
        df['Test Name'] = df['test_file']
    else:
        df['Test Name'] = 'Unknown'

    df['Errors'] = df['description'].apply(lambda x: '; '.join(x) if isinstance(x, list) else str(x))
    display_df = df[['Test Name', 'reason', 'Errors']].rename(columns={'reason': 'Status'})

    # Create the Results folder and subfolder with current timestamp
    results_dir = "Results"
    destination_folder = os.path.join(results_dir, run_timestamp)
    os.makedirs(destination_folder, exist_ok=True)
    output_filename = os.path.join(destination_folder, 'compatibility_report.xlsx')
    display_df.to_excel(output_filename, index=False)
    print(f"\nCompatibility report saved to {output_filename}")

def create_summary_document():
    """
    Compute summary statistics from test results and insert a summary document into the database.
    """
    try:
        client = MongoClient(config.RESULT_DB_URI)
        db = client[config.RESULT_DB_NAME]
        collection = db[config.RESULT_COLLECTION_NAME]
    except Exception as e:
        print(f"Error connecting to the results database: {e}")
        sys.exit(1)

    results = list(collection.find())
    if not results:
        print("No test results found in the correctness collection.")
        sys.exit(1)

    df = pd.DataFrame(results)

    if 'suite' not in df.columns or 'status' not in df.columns:
        print("The test data is missing required fields: 'suite' or 'status'.")
        sys.exit(1)

    suite_summary = []
    for suite_id in df['suite'].unique():
        suite_data = df[df['suite'] == suite_id]
        passing_tests = suite_data[suite_data['status'] == 'pass'].shape[0]
        failing_tests = suite_data[suite_data['status'] == 'fail'].shape[0]
        total_tests = suite_data.shape[0]

        suite_summary.append({
            '_id': suite_id,
            'passing_tests': passing_tests,
            'failing_tests': failing_tests,
            'total_tests': total_tests
        })

    passing_tests = df[df['status'] == 'pass'].shape[0]
    failing_tests = df[df['status'] == 'fail'].shape[0]
    total_tests = df.shape[0]
    passing_percentage = (passing_tests / total_tests) * 100
    failing_percentage = (failing_tests / total_tests) * 100

    timestamp = time.time()
    version = 'v8.0'
    platform = config.PLATFORM

    try:
        web_client = MongoClient(config.RESULT_DB_URI)
        web_db = web_client[config.RESULT_DB_NAME]
        summary_collection = web_db['summary']
    except Exception as e:
        print(f"Error connecting to the docdb_web_prod database: {e}")
        sys.exit(1)

    last_run_doc = summary_collection.find_one(sort=[('run', -1)])
    run = last_run_doc['run'] + 1 if last_run_doc and 'run' in last_run_doc else 1

    summary_document = {
        'suites': suite_summary,
        'timestamp': timestamp,
        'passing_tests': passing_tests,
        'failing_tests': failing_tests,
        'total_tests': total_tests,
        'version': version,
        'run': run,
        'platform': platform,
        'failing_percentage': round(failing_percentage, 2),
        'passing_percentage': round(passing_percentage, 2)
    }

    try:
        summary_collection.insert_one(summary_document)
        print("Summary document inserted successfully.")
    except Exception as e:
        print(f"Error inserting summary document: {e}")

def apply_changes_to_correctness_collection():
    """Apply all changes to the correctness collection."""
    client = MongoClient(config.RESULT_DB_URI)
    db = client["docdb_web_prod"]
    correctness_collection = db["correctness"]

    # Rename fields: test_name -> test_file
    correctness_collection.update_many(
        {"test_name": {"$exists": True}},
        {"$rename": {"test_name": "test_file"}}
    )

    # Adjust description field for documents that have a string type description
    documents = correctness_collection.find({"description": {"$exists": True, "$type": "string"}})
    for doc in documents:
        correctness_collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"description": [f'"errmsg" : "{doc["description"]}"']}}
        )

    # Convert and update start, end, and elapsed fields
    for doc in correctness_collection.find({}):
        updates = {}
        if isinstance(doc.get("start"), str):
            updates["start"] = datetime.fromisoformat(doc["start"]).timestamp()
        if isinstance(doc.get("end"), str):
            updates["end"] = datetime.fromisoformat(doc["end"]).timestamp()
        if "start" in updates and "end" in updates:
            updates["elapsed"] = updates["end"] - updates["start"]
        if updates:
            correctness_collection.update_one({"_id": doc["_id"]}, {"$set": updates})

    # Update reason field: "FAILED" becomes "UNSUPPORTED"
    correctness_collection.update_many(
        {"reason": "FAILED"},
        {"$set": {"reason": "UNSUPPORTED"}}
    )

    # Retain specified fields only
    for doc in correctness_collection.find():
        updated_doc = {key: doc[key] for key in FIELDS_TO_KEEP if key in doc}
        correctness_collection.replace_one({"_id": doc["_id"]}, updated_doc)

#############################################################
#  Final Step: Incorporate changes from change_reason.py
#############################################################

def normalize_description(desc):
    """
    Normalize the description field so that it has exactly the structure:
    
        "errmsg" : "['"errmsg" : "errmsg: [<error message>]"']"
    
    This function extracts the error message using the last occurrence of "errmsg:".
    It then builds a new string with the required prefix and suffix.
    """
    if not desc:
        return None

    # Get string from list if necessary
    if isinstance(desc, list) and len(desc) > 0:
        s = desc[0]
    elif isinstance(desc, str):
        s = desc
    else:
        s = str(desc)
    s = s.strip()

    # Remove any outer square brackets and extraneous quotes
    while s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    s = s.strip('\'"')

    # Find the last occurrence of "errmsg:" and take text after it, if available.
    pos = s.rfind("errmsg:")
    if pos != -1:
        error_content = s[pos+len("errmsg:"):].strip()
    else:
        error_content = s
    error_content = error_content.strip('\'"')

    # Build the new string with the new desired prefix.
    # New desired prefix: "errmsg" : "['"errmsg" : "errmsg: [
    # and then close with ]"']"
    prefix = '"errmsg" : "['
    middle = '"errmsg" : "errmsg: ['
    suffix = ']"\']"'
    normalized = prefix + middle + error_content + suffix
    return [normalized]

def transform_document(doc):
    """
    Transform a document so that:
      - Only the required fields are kept.
      - 'test_name' is renamed to 'test_file'.
      - 'reason' is updated: if it is "FAILED", it becomes "UNSUPPORTED".
      - 'description' is normalized.
      - The 'platform' field is set to "CosmosDB".
    """
    new_doc = {}
    new_doc["_id"] = doc["_id"]

    if "test_name" in doc:
        new_doc["test_file"] = doc["test_name"]
    elif "test_file" in doc:
        new_doc["test_file"] = doc["test_file"]

    for field in ["status", "exit_code", "start", "end", "elapsed", "suite", "version", "run", "processed", "log_lines"]:
        if field in doc:
            new_doc[field] = doc[field]

    if "reason" in doc:
        new_doc["reason"] = "UNSUPPORTED" if doc["reason"] == "FAILED" else doc["reason"]

    if "description" in doc and doc["description"]:
        new_doc["description"] = normalize_description(doc["description"])

    new_doc["platform"] = "CosmosDB"

    return new_doc

def finalize_correctness_documents():
    """
    Finalize the structure of documents in the correctness collection using the change_reason.py logic.
    This step ensures:
      - The description field is normalized so that it contains only one proper "errmsg" prefix.
      - Other field changes (such as renaming and updating reason) are applied.
    """
    client = MongoClient(config.RESULT_DB_URI)
    db = client[config.RESULT_DB_NAME]
    correctness_collection = db[config.RESULT_COLLECTION_NAME]
    
    docs = list(correctness_collection.find({}))
    count = 0
    for doc in docs:
        new_doc = transform_document(doc)
        correctness_collection.replace_one({"_id": doc["_id"]}, new_doc)
        count += 1
    print(f"Finalized {count} documents in the '{config.RESULT_COLLECTION_NAME}' collection.")

#############################################################
#  End of change_reason.py integration
#############################################################

def remove_text_from_fields():
    """
    Remove the unwanted substring from the log_lines and description fields.
    The substring removed is:
        /Users/rahul.verma/Desktop/Updated Compatibility Test Suite/tests/
    """
    client = MongoClient(config.RESULT_DB_URI)
    db = client[config.RESULT_DB_NAME]
    collection = db[config.RESULT_COLLECTION_NAME]

    substring_to_remove = "/Users/rahul.verma/Desktop/Updated Compatibility Test Suite/tests/"

    docs = list(collection.find({}))
    modified_count = 0

    for doc in docs:
        update_needed = False

        # Process log_lines if it exists and is a list.
        if "log_lines" in doc and isinstance(doc["log_lines"], list):
            new_log_lines = []
            for line in doc["log_lines"]:
                if isinstance(line, str) and substring_to_remove in line:
                    new_line = line.replace(substring_to_remove, "")
                    new_log_lines.append(new_line)
                    update_needed = True
                else:
                    new_log_lines.append(line)
            if update_needed:
                doc["log_lines"] = new_log_lines

        # Process description field if it exists.
        if "description" in doc:
            if isinstance(doc["description"], list):
                new_description = []
                for d in doc["description"]:
                    if isinstance(d, str) and substring_to_remove in d:
                        new_d = d.replace(substring_to_remove, "")
                        new_description.append(new_d)
                        update_needed = True
                    else:
                        new_description.append(d)
                if update_needed:
                    doc["description"] = new_description
            elif isinstance(doc["description"], str):
                if substring_to_remove in doc["description"]:
                    doc["description"] = doc["description"].replace(substring_to_remove, "")
                    update_needed = True

        if update_needed:
            collection.replace_one({"_id": doc["_id"]}, doc)
            modified_count += 1

    print(f"Removed unwanted substring from {modified_count} documents.")

if __name__ == "__main__":
    # Install dependencies from requirements.txt before running tests
    try:
        print("Installing dependencies from requirements.txt...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Dependencies installed successfully.\n")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install dependencies: {e}")
        sys.exit(1)
    
    # Create a single timestamp for the run to be used for both Results and Logs folders
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Run the tests
    run_tests()

    # Generate the compatibility report
    generate_compatibility_report()

    # Save the report to an Excel file in the Results folder with the date and time of the run
    save_report_to_excel(run_timestamp)

    # Create the summary document
    create_summary_document()

    # Apply changes to the correctness collection
    apply_changes_to_correctness_collection()
    print("All changes to the correctness collection have been applied.")

    # === Log file post-processing ===
    logs_dir = "Logs"
    destination_folder = os.path.join(logs_dir, run_timestamp)
    os.makedirs(destination_folder, exist_ok=True)
    print(f"Log files will be moved to: {destination_folder}")

    log_files = glob.glob("*.log")
    for log_file in log_files:
        shutil.move(log_file, destination_folder)
        print(f"Moved log file: {log_file}")

    for log_file in glob.glob("*.log"):
        os.remove(log_file)
        print(f"Deleted leftover log file: {log_file}")

    # === Final Step: Finalize the description field structure in the correctness collection ===
    finalize_correctness_documents()
    print("Finalized correctness collection documents with updated description structure.")

    # === Remove unwanted substring from log_lines and description fields ===
    remove_text_from_fields()
    print("Removed unwanted substring from log_lines and description fields.")
