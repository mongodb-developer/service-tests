import unittest
import sys
from pymongo import MongoClient
import config
import logging
import pandas as pd
import time
from bson import ObjectId
from datetime import datetime

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

def save_report_to_excel():
    """
    Connect to the results database, retrieve test results, and save the report to an Excel file.
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

    output_filename = 'compatibility_report.xlsx'
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
    platform = 'DocumentDB'

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

    # Rename fields
    correctness_collection.update_many({"test_name": {"$exists": True}}, {"$rename": {"test_name": "test_file"}})

    # Adjust description field
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

    # Update reason field
    correctness_collection.update_many(
        {"reason": "FAILED"},
        {"$set": {"reason": "UNSUPPORTED"}}
    )

    # Retain specified fields
    for doc in correctness_collection.find():
        updated_doc = {key: doc[key] for key in FIELDS_TO_KEEP if key in doc}
        correctness_collection.replace_one({"_id": doc["_id"]}, updated_doc)

if __name__ == "__main__":
    # Run the tests
    run_tests()

    # Generate the compatibility report
    generate_compatibility_report()

    # Save the report to an Excel file
    save_report_to_excel()

    # Create the summary document
    create_summary_document()

    # Apply changes to the correctness collection
    apply_changes_to_correctness_collection()
    print("All changes to the correctness collection have been applied.")
