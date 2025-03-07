# Competitive Intelligence Test Suite for Database Compatibility with MongoDB API


# Reuired Scsipts

- `tests/`**: Contains individual Python test scripts.
- `config.py`**: Configuration file for setting up database connections.
- `requirements.txt`**: Lists all the required Python libraries.
- `run_test.py`**: Executes the test suite
- `compatibility_score.py`**: Executed as a part of the test suite which calculates the Pass/Total % or the compatibility score.
- `base_test.py`**: Required for the test setup.

# Setup Instructions

1. Configure `config.py`:
   - Set the connection string for the database to be tested.
   - Specify the Atlas cluster where the test results will be stored.

# Running the Test Suite

1. Execute the Test Suite:**
   - Run the following command:
     ```bash
     python run_test.py
     ```

2. Executing the above command will install all the required libraries from requirements.txt file present in the root folder.

# Output Details:

1. Results stored in Atlas cluster

Two collections created in your Atlas cluster:
       - **Individual Test Results:** Stores detailed outcomes for each test.
       - **Summary Results:** Provides an overall summary with the pass percentage.
     
2. Results Folder

An Excel file generated with the following details:
       - Test names.
       - Pass or fail status.

3. Logs Folder

All the log files for each of the test stored in folder with the date and time of the current execution.
