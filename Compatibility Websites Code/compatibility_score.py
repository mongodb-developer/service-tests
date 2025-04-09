# compatibility_score.py

from pymongo import MongoClient
import config
import pandas as pd

def generate_compatibility_report():
    """
    Generates a compatibility report based on the test results stored in the database.
    """
    # Connect to the results database
    client = MongoClient(config.RESULT_DB_URI)
    db = client[config.RESULT_DB_NAME]
    collection = db[config.RESULT_COLLECTION_NAME]

    # Fetch all test results
    results = list(collection.find())

    if not results:
        print("No test results found in the 'correctness' collection.")
        return

    # Create a DataFrame for easier manipulation
    df = pd.DataFrame(results)

    # Debugging: Print DataFrame columns and sample data
    print("DataFrame columns:", df.columns.tolist())
    print("Sample data:")
    print(df[['test_name', 'status', 'reason', 'description']].head())

    # Define pass conditions
    pass_conditions = df['status'].isin(['pass'])

    # Calculate pass counts
    pass_count = df[pass_conditions].shape[0]
    total_tests = df.shape[0]
    compatibility_percentage = (pass_count / total_tests) * 100 if total_tests > 0 else 0

    # Use 'test_name' for Test Name; fall back to 'test_file' if 'test_name' is not available
    if 'test_name' in df.columns:
        df['Test Name'] = df['test_name']
    elif 'test_file' in df.columns:
        df['Test Name'] = df['test_file']
    else:
        df['Test Name'] = 'Unknown'

    # Process 'description' field to join array elements into a single string
    df['Errors'] = df['description'].apply(lambda x: '; '.join(x) if isinstance(x, list) else str(x))

    # Display the results in a table format
    display_df = df[['Test Name', 'reason', 'Errors']].rename(columns={
        'reason': 'Status'
    })

    print("\nTest Results for DocumentDB:")
    print(display_df.to_markdown(index=False))

    # Display compatibility percentage
    print(f"\nCompatibility Percentage for {config.PLATFORM} is : {compatibility_percentage:.2f}%")

    # Close the client connection
    client.close()

if __name__ == '__main__':
    generate_compatibility_report()
