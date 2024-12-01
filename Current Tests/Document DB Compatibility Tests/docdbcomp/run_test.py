# run_test.py

import unittest
import sys
from pymongo import MongoClient
import config
import logging
import pandas as pd
import time
from datetime import datetime

# Import the generate_compatibility_report function
from compatibility_score import generate_compatibility_report

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
    # Connect to the results database
    try:
        client = MongoClient(config.RESULT_DB_URI)
        db = client[config.RESULT_DB_NAME]
        collection = db[config.RESULT_COLLECTION_NAME]
    except Exception as e:
        print(f"Error connecting to the results database: {e}")
        sys.exit(1)

    # Fetch all test results
    results = list(collection.find())

    if not results:
        print("No test results found in the 'correctness' collection.")
        sys.exit(1)

    # Create a DataFrame for easier manipulation
    df = pd.DataFrame(results)

    # Process the DataFrame similar to compatibility_score.py
    if 'test_name' in df.columns:
        df['Test Name'] = df['test_name']
    elif 'test_file' in df.columns:
        df['Test Name'] = df['test_file']
    else:
        df['Test Name'] = 'Unknown'

    # Process 'description' field to join array elements into a single string
    df['Errors'] = df['description'].apply(lambda x: '; '.join(x) if isinstance(x, list) else str(x))

    # Prepare the DataFrame
    display_df = df[['Test Name', 'reason', 'Errors']].rename(columns={
        'reason': 'Status'
    })

    # Save the DataFrame to an Excel file
    output_filename = 'compatibility_report.xlsx'
    display_df.to_excel(output_filename, index=False)
    print(f"\nCompatibility report saved to {output_filename}")

def create_summary_document():
    """
    Compute summary statistics from test results and insert a summary document into the database.
    """
    # Connect to the results database
    try:
        client = MongoClient(config.RESULT_DB_URI)
        db = client[config.RESULT_DB_NAME]
        collection = db[config.RESULT_COLLECTION_NAME]
    except Exception as e:
        print(f"Error connecting to the results database: {e}")
        sys.exit(1)

    # Fetch all test results
    results = list(collection.find())

    if not results:
        print("No test results found in the results collection.")
        sys.exit(1)

    # Create a DataFrame for easier manipulation
    df = pd.DataFrame(results)

    # Compute passing and failing tests
    passing_tests = df[df['status'] == 'pass'].shape[0]
    failing_tests = df[df['status'] == 'fail'].shape[0]
    total_tests = df.shape[0]

    passing_percentage = (passing_tests / total_tests) * 100
    failing_percentage = (failing_tests / total_tests) * 100

    # Get unique suites
    suites = df['suite'].unique().tolist()

    # Get current timestamp
    timestamp = time.time()

    # Version - assuming we can get this from somewhere, otherwise set default
    version = 'v8.0'  # Replace with actual version if available

    # Platform
    platform = 'DocumentDB'

    # Connect to docdb_web_prod database and summary collection
    try:
        web_client = MongoClient(config.RESULT_DB_URI)
        web_db = web_client['docdb_web_prod']
        summary_collection = web_db['summary']
    except Exception as e:
        print(f"Error connecting to the docdb_web_prod database: {e}")
        sys.exit(1)

    # Determine the next run number
    last_run_doc = summary_collection.find_one(sort=[('run', -1)])
    if last_run_doc and 'run' in last_run_doc:
        run = last_run_doc['run'] + 1
    else:
        run = 1

    # Prepare the summary document
    summary_document = {
        'suites': suites,
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

    # Insert the summary document into the collection
    try:
        summary_collection.insert_one(summary_document)
        print("Summary document inserted successfully.")
    except Exception as e:
        print(f"Error inserting summary document: {e}")

if __name__ == '__main__':
    # Run the tests
    run_tests()

    # After tests are run and data is stored, generate the report
    generate_compatibility_report()

    # Generate the Excel file with the output table
    save_report_to_excel()

    # Create the summary document
    create_summary_document()
