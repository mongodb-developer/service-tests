# tests/test_change_streams.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime
import sys
import os
import decimal

import psycopg2
from psycopg2.extras import RealDictCursor

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest

class TestChangeStreams(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_change_streams'

        # Configure logging
        cls.logger = logging.getLogger('TestChangeStreams')
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_change_streams.log'
        file_handler = logging.FileHandler('test_change_streams.log')
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

        # Create table
        try:
            with cls.test_conn.cursor() as cur:
                # Drop table if exists
                cur.execute(f"DROP TABLE IF EXISTS {cls.table_name} CASCADE;")
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table
                cur.execute(f"""
                    CREATE TABLE {cls.table_name} (
                        id SERIAL PRIMARY KEY,
                        data TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cls.logger.debug(f"Created table '{cls.table_name}'.")
            cls.test_conn.commit()
            cls.logger.debug(f"Setup for '{cls.table_name}' completed successfully.")
            print(f"Table '{cls.table_name}' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error setting up table: {e}")
            print(f"Error setting up table: {e}")
            raise

    def setUp(self):
        # Assign class variables to instance variables
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    def convert_decimals(self, obj):
        """Recursively convert Decimal objects to float."""
        if isinstance(obj, list):
            return [self.convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if precision is important
        else:
            return obj

    def test_change_streams(self):
        """
        Test emulated Change Streams using PostgreSQL.
        Since PostgreSQL does not support Change Streams like MongoDB,
        this test will attempt to use an unsupported method, expecting it to fail.
        """
        table = self.table_name
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Change Streams Test',
            'platform': 'postgresql',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': table,
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'change_events': [],
        }

        try:
            # Attempt to use change streams in PostgreSQL
            # PostgreSQL does not support change streams like MongoDB
            # Hence, attempting to use a non-existent method to simulate the test
            # This should raise an AttributeError

            # Using a dummy method 'watch' which does not exist in psycopg2
            # Replace the following line with an actual method if PostgreSQL adds support in the future
            with table.watch() as stream:
                # If no exception is raised, change streams are supported
                result_document['description'].append("Change streams are supported on this platform.")
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append("Change stream test executed successfully.")
                self.logger.debug("Change stream test executed successfully.")
        except Exception as e:
            error_trace = traceback.format_exc()
            error_msg = f"Change streams not supported or error occurred: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.info(error_msg)
            self.logger.error(f"Error during change stream test: {e}\n{error_trace}")
        finally:
            # Capture elapsed time and end time
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            # Retrieve server version dynamically
            try:
                with self.test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version_info = cur.fetchone()
                server_version = version_info[0] if version_info else 'unknown'
                result_document['version'] = server_version
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

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        # Clean up: drop the table
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {cls.table_name} CASCADE;")
            cls.test_conn.commit()
            cls.logger.debug(f"Dropped table '{cls.table_name}' during teardown.")
            print(f"Table '{cls.table_name}' dropped successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error dropping table during teardown: {e}")
            print(f"Error dropping table during teardown: {e}")
        finally:
            super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
