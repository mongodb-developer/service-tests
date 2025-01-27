# Test Suite for Database Compatibility Testing for MongoDB API

This repository contains Python scripts for testing database performance. Follow the steps below to set up and run the test suite.

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

2. Install Dependencies:**
   - Run the following command to install the required libraries:
     ```bash
     pip install -r requirements.txt
     ```

# Running the Test Suite

1. Execute the Test Suite:**
   - Run the following command:
     ```bash
     python run_test.py
     ```

2. Output Details:
   - The results will include:
     - Two collections created in your Atlas cluster:
       - **Individual Test Results:** Stores detailed outcomes for each test.
       - **Summary Results:** Provides an overall summary with the pass percentage.
     - An Excel file generated with the following details:
       - Test names.
       - Pass or fail status.

