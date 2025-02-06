# tests/test_data_types.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime
import sys
import os
import decimal
import uuid

import psycopg2
from psycopg2.extras import RealDictCursor

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest

class TestDataTypes(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_data_types'

        # Configure logging
        cls.logger = logging.getLogger('TestDataTypes')
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_data_types.log'
        file_handler = logging.FileHandler('test_data_types.log')
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

        # Create table for data types
        try:
            with cls.test_conn.cursor() as cur:
                # Drop table if it exists to start clean
                cur.execute(f"DROP TABLE IF EXISTS {cls.table_name} CASCADE;")
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table with appropriate columns
                cur.execute(f"""
                    CREATE TABLE {cls.table_name} (
                        id SERIAL PRIMARY KEY,
                        _id TEXT NOT NULL,
                        string TEXT,
                        int INTEGER,
                        float REAL,
                        decimal NUMERIC,
                        date TIMESTAMP,
                        object_id TEXT,
                        binary_data BYTEA,
                        integer_array INTEGER[],
                        nested_array INTEGER[][],
                        document JSONB,
                        nested_document JSONB,
                        regex TEXT,
                        javascript TEXT,
                        timestamp TIMESTAMP,
                        min_key TEXT,
                        max_key TEXT,
                        dbref JSONB,
                        null_field TEXT,
                        bool_true BOOLEAN,
                        bool_false BOOLEAN
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

    def insert_and_store_result(self, data, data_type_name):
        """
        Inserts data into the PostgreSQL table and records the result.
        """
        table = self.table_name
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Data Type Test - {data_type_name}",
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
            'insert_result': {},
        }

        # Prepare columns and values for INSERT
        columns = []
        values = []
        placeholders = []
        for key, value in data.items():
            if key == '_id':
                # Convert UUID to string
                value = str(value)
            elif isinstance(value, uuid.UUID):
                value = str(value)
            elif isinstance(value, decimal.Decimal):
                # Keep as Decimal for NUMERIC type
                pass
            elif isinstance(value, bytes):
                # Binary data for BYTEA
                pass
            elif isinstance(value, list):
                # Array types
                pass
            elif isinstance(value, dict):
                # JSONB types
                value = json.dumps(value)
            elif hasattr(value, 'pattern'):
                # Regex patterns, store pattern string
                value = value.pattern
            elif isinstance(value, str) and value.startswith('function'):
                # Code type, store as string
                pass  # Already a string
            elif isinstance(value, datetime):
                # Timestamp type, keep as datetime
                pass
            elif isinstance(value, str) and value in ['MinKey', 'MaxKey']:
                # MinKey and MaxKey, store as string
                pass
            # Add more type conversions as needed

            columns.append(key)
            values.append(value)
            placeholders.append('%s')

        insert_query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING id;
        """

        try:
            with self.test_conn.cursor() as cur:
                cur.execute(insert_query, values)
                inserted_id = cur.fetchone()[0]
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['insert_result'] = {'inserted_id': inserted_id}
                self.logger.debug(f"Data type '{data_type_name}' inserted successfully with id {inserted_id}.")
                result_document['log_lines'].append(f"Data type '{data_type_name}' inserted successfully with id {inserted_id}.")
            self.test_conn.commit()
        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Error inserting data type '{data_type_name}': {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
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

    # Define test methods for each data type
    def test_string(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'string': 'text'}, 'string')

    def test_integer(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'int': 42}, 'integer')

    def test_float(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'float': 3.14}, 'float')

    def test_decimal128(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'decimal': decimal.Decimal('123.45')}, 'decimal128')

    def test_datetime(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'date': datetime.utcnow()}, 'datetime')

    def test_object_id(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'object_id': uuid.uuid4()}, 'object_id')

    def test_binary(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'binary_data': b'binary data'}, 'binary')

    def test_array(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'integer_array': [1, 2, 3]}, 'array')

    def test_nested_array(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'nested_array': [[1, 2], [3, 4]]}, 'nested_array')

    def test_document(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'document': {'nested': 'value'}}, 'document')

    def test_nested_document(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'nested_document': {'level1': {'level2': {'level3': 'deep'}}}}, 'nested_document')

    def test_regex(self):
        # PostgreSQL does not have a native regex type; store pattern as string
        import re
        regex = re.compile(r'^pattern$', re.IGNORECASE)
        self.insert_and_store_result({'_id': uuid.uuid4(), 'regex': regex}, 'regex')

    def test_code(self):
        # PostgreSQL does not support JavaScript code; store as string
        code = "function() { return true; }"
        self.insert_and_store_result({'_id': uuid.uuid4(), 'javascript': code}, 'code')

    def test_code_with_scope(self):
        # PostgreSQL does not support code with scope; store as JSON
        code_with_scope = {
            'code': 'function() { return x; }',
            'scope': {'x': 42}
        }
        self.insert_and_store_result({'_id': uuid.uuid4(), 'javascript_with_scope': code_with_scope}, 'code_with_scope')

    def test_timestamp(self):
        # PostgreSQL's TIMESTAMP type is similar to MongoDB's Timestamp
        # Store as datetime
        timestamp = datetime.utcnow()
        self.insert_and_store_result({'_id': uuid.uuid4(), 'timestamp': timestamp}, 'timestamp')

    def test_minkey(self):
        # PostgreSQL does not have MinKey; store as string
        self.insert_and_store_result({'_id': uuid.uuid4(), 'min_key': 'MinKey'}, 'min_key')

    def test_maxkey(self):
        # PostgreSQL does not have MaxKey; store as string
        self.insert_and_store_result({'_id': uuid.uuid4(), 'max_key': 'MaxKey'}, 'max_key')

    def test_dbref(self):
        # PostgreSQL does not have DBRef; store as JSON
        dbref = {
            '$ref': 'collection',
            '$id': str(uuid.uuid4()),
            '$db': 'database'
        }
        self.insert_and_store_result({'_id': uuid.uuid4(), 'dbref': dbref}, 'dbref')

    def test_null(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'null_field': None}, 'null')

    def test_boolean_true(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'bool_true': True}, 'boolean_true')

    def test_boolean_false(self):
        self.insert_and_store_result({'_id': uuid.uuid4(), 'bool_false': False}, 'boolean_false')

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
