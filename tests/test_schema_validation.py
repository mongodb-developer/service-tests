# tests/test_postgresql_schema_validation.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime, timedelta
import sys
import os
import decimal  # Importing the decimal module

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest
import config  # Ensure this module has PostgreSQL configurations

class TestPostgreSQLSchemaValidation(BaseTest):
    """
    Test suite for PostgreSQL schema validation.
    Translates MongoDB/DocumentDB schema validation tests to PostgreSQL using psycopg2.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_postgresql_schema_validation'

        # Configure logging
        cls.logger = logging.getLogger('TestPostgreSQLSchemaValidation')
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_postgresql_schema_validation.log'
        file_handler = logging.FileHandler('test_postgresql_schema_validation.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        # In-Memory Log Capture List
        cls.log_capture_list = []

        # Custom Handler to capture logs in memory
        class ListHandler(logging.Handler):
            def __init__(self, log_list):
                super().__init__()
                self.log_list = log_list

            def emit(self, record):
                log_entry = self.format(record)
                self.log_list.append(log_entry)

        # Initialize and add the custom ListHandler
        list_handler = ListHandler(cls.log_capture_list)
        list_handler.setFormatter(formatter)
        cls.logger.addHandler(list_handler)

        # Create table for schema validation tests
        try:
            with cls.test_conn.cursor() as cur:
                # Drop table if it exists to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table with schema constraints
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        _id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        age INTEGER NOT NULL CHECK (age >= 0 AND age <= 120)
                    );
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Created table '{cls.table_name}' with schema constraints.")
            cls.test_conn.commit()
            cls.logger.debug(f"Setup for '{cls.table_name}' completed successfully.")
            print(f"Table '{cls.table_name}' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error setting up table: {e}")
            print(f"Error setting up table: {e}")
            raise

    def setUp(self):
        """Reset table before each test"""
        try:
            with self.test_conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY;").format(sql.Identifier(self.table_name)))
            self.test_conn.commit()
            self.logger.debug("Truncated table before test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error truncating table in setUp: {e}")
            raise

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    @staticmethod
    def convert_decimals(obj):
        """Recursively convert Decimal objects to float."""
        if isinstance(obj, list):
            return [TestPostgreSQLSchemaValidation.convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: TestPostgreSQLSchemaValidation.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if precision is important
        else:
            return obj

    def get_test_data(self):
        """Generate test data for PostgreSQL schema validation"""
        return [
            {
                '_id': 'test_1',
                'name': 'Alice',
                'age': 30
            },
            {
                '_id': 'test_2',
                'name': 'Bob',
                'age': 25
            }
        ]

    def insert_test_data(self, test_data):
        """Inserts test data into the table."""
        try:
            with self.test_conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {} (_id, name, age)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (_id) DO NOTHING;
                """).format(sql.Identifier(self.table_name))
                for doc in test_data:
                    cur.execute(insert_query, (
                        doc['_id'],
                        doc['name'],
                        doc['age']
                    ))
            self.test_conn.commit()
            self.logger.debug("Inserted test data successfully.")
            return True, ""
        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Failed to insert test data: {e}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return False, error_msg

    def test_schema_validation(self):
        """Test PostgreSQL schema validation using table constraints"""
        collection = self.table_name
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'PostgreSQL Schema Validation Test',
            'platform': 'postgresql',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': self.table_name,
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            # Step 1: Insert Valid Data
            test_data = self.get_test_data()
            success, error_msg = self.insert_test_data(test_data)
            if not success:
                raise Exception(error_msg)
            result_document['log_lines'].append('Valid data inserted successfully.')
            self.logger.debug("Valid data inserted successfully.")

            # Step 2: Attempt to Insert Invalid Data (age as string)
            try:
                with self.test_conn.cursor() as cur:
                    invalid_insert_query = sql.SQL("""
                        INSERT INTO {} (_id, name, age)
                        VALUES (%s, %s, %s)
                    """).format(sql.Identifier(self.table_name))
                    cur.execute(invalid_insert_query, (
                        'test_3',
                        'Charlie',
                        'thirty'  # Invalid age: should be INTEGER
                    ))
                self.test_conn.commit()
                error_msg = 'Invalid data inserted without raising an error.'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
            except psycopg2.errors.InvalidTextRepresentation as itre:
                # Expected error: invalid input syntax for integer: "thirty"
                self.test_conn.rollback()
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Schema validation correctly rejected invalid data.')
                self.logger.debug("Schema validation correctly rejected invalid data.")
            except psycopg2.errors.CheckViolation as cv:
                # Expected error if age violates CHECK constraint
                self.test_conn.rollback()
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Schema validation correctly rejected invalid data (CHECK violation).')
                self.logger.debug("Schema validation correctly rejected invalid data (CHECK violation).")
            except Exception as e:
                self.test_conn.rollback()
                error_msg = f"Unexpected error during invalid data insertion: {e}"
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during PostgreSQL Schema Validation test: {e}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                with self.test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version_info = cur.fetchone()
                server_version = version_info[0] if version_info else 'unknown'
                result_document['version'] = server_version
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Convert all Decimal objects to float or string
            result_document = self.convert_decimals(result_document)

            # Ensure all fields in result_document are JSON serializable
            try:
                json.dumps(result_document)
            except TypeError as te:
                self.logger.error(f"JSON serialization error: {te}")
                result_document['reason'] += f" JSON serialization error: {te}"
                result_document['status'] = 'fail'

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
            cls.test_conn.commit()
            cls.logger.debug("Dropped table during teardown.")
            print(f"Table '{cls.table_name}' dropped successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error dropping table during teardown: {e}")
            print(f"Error dropping table during teardown: {e}")
        finally:
            super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
