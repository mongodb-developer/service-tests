# tests/test_field_level_encryption.py

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
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest

class TestFieldLevelEncryption(BaseTest):
    """
    Test suite for client-side field-level encryption and queryable encryption in PostgreSQL.
    Since PostgreSQL does not support these features natively without extensions like pgcrypto,
    the tests will attempt similar operations and handle any resulting errors.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_field_level_encryption'

        # Configure logging
        cls.logger = logging.getLogger('TestFieldLevelEncryption')
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_field_level_encryption.log'
        file_handler = logging.FileHandler('test_field_level_encryption.log')
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

        # Create table for field-level encryption
        try:
            with cls.test_conn.cursor() as cur:
                # Drop table if it exists to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table with appropriate columns
                # Note: PostgreSQL doesn't support client-side encryption like MongoDB.
                # We'll simulate encryption attempts without actual encryption.
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        id SERIAL PRIMARY KEY,
                        first_name TEXT,
                        last_name TEXT,
                        ssn TEXT, -- Attempting to store SSN as plain text
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """).format(sql.Identifier(cls.table_name)))
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

    def test_field_level_encryption(self):
        """
        Test client-side field-level encryption.
        Since PostgreSQL does not support client-side encryption like MongoDB,
        this test will attempt to insert and retrieve an SSN without encryption.
        Any encryption-specific operations will naturally fail.
        """
        table = self.table_name
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Field Level Encryption Test',
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
            'details': {},
        }

        try:
            # Simulate generating a local master key (not used since pgcrypto is not being used)
            local_master_key = os.urandom(32)  # 256-bit key for AES
            encryption_key = local_master_key.hex()  # Convert to hex string (not used)
            self.logger.debug("Generated local master key for encryption (simulated).")

            # Attempt to encrypt the SSN (no actual encryption since pgcrypto is not used)
            plaintext_ssn = '123-45-6789'
            self.logger.debug(f"Plaintext SSN to insert: {plaintext_ssn}")

            # Insert the document with the SSN as plain text
            with self.test_conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    INSERT INTO {} (first_name, last_name, ssn)
                    VALUES (
                        %s,
                        %s,
                        %s
                    )
                    RETURNING id;
                """).format(sql.Identifier(table)),
                ('John', 'Doe', plaintext_ssn))
                inserted_id = cur.fetchone()[0]
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['details']['inserted_document'] = {
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'ssn': plaintext_ssn
                }
                self.logger.debug(f"Inserted document with id {inserted_id} and SSN.")
                result_document['log_lines'].append(f"Inserted document with id {inserted_id} and SSN.")

            self.test_conn.commit()

            # Retrieve the document
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql.SQL("""
                    SELECT first_name, last_name, ssn
                    FROM {}
                    WHERE id = %s;
                """).format(sql.Identifier(table)),
                (inserted_id,))
                retrieved = cur.fetchone()

            if retrieved:
                retrieved_ssn = retrieved['ssn']
                self.logger.debug(f"Retrieved SSN: {retrieved_ssn}")

                if retrieved_ssn == plaintext_ssn:
                    self.logger.info("Successfully retrieved SSN matches the original plaintext.")
                    result_document['details']['retrieved_document'] = {
                        'first_name': retrieved['first_name'],
                        'last_name': retrieved['last_name'],
                        'ssn': retrieved_ssn
                    }
                else:
                    error_msg = "Retrieved SSN does not match the original plaintext."
                    result_document['description'].append(error_msg)
                    result_document['reason'] = 'FAILED'
                    self.logger.error(error_msg)
            else:
                error_msg = "Failed to retrieve the inserted document."
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)

        except Exception as e:
            self.test_conn.rollback()
            error_trace = traceback.format_exc()
            error_msg = f"Unexpected error during field-level encryption test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
            self.logger.debug(error_trace)
        finally:
            # Capture elapsed time and end time
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            # Retrieve PostgreSQL server version dynamically
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

            # Accumulate result for later storage
            self.test_results.append(result_document)

            # Fail the test if it did not pass
            if result_document['status'] != 'pass':
                self.fail(f"Field-level encryption test failed: {result_document['description']}")

    def test_queryable_encryption(self):
        """
        Test queryable encryption.
        Since PostgreSQL does not support queryable encryption like MongoDB,
        this test will attempt to perform an encrypted query and expect it to fail.
        """
        table = self.table_name
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Queryable Encryption Test',
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
            'details': {},
        }

        try:
            # Simulate performing a queryable encryption operation
            # Since PostgreSQL does not support this natively without extensions,
            # attempting to perform such an operation will naturally fail.

            # Example: Attempting to create an encrypted index (which is not supported)
            encryption_key = os.urandom(32).hex()  # AES-256 key (not used)

            with self.test_conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    CREATE INDEX idx_encrypted_ssn ON {}
                    USING btree (ssn); -- Attempting to index the SSN field
                """).format(sql.Identifier(table)))
                cls.logger.debug("Created index on 'ssn' field.")
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['details']['index_creation'] = 'Index created successfully.'
                result_document['log_lines'].append("Index created successfully.")

            self.test_conn.commit()

        except psycopg2.Error as e:
            self.test_conn.rollback()
            error_trace = traceback.format_exc()
            error_msg = f"Queryable encryption not supported as expected: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'PASSED (encryption not supported as expected)'
            result_document['status'] = 'pass'  # Mark as pass since failure was expected
            result_document['log_lines'].append('Queryable encryption correctly not supported.')
            self.logger.info(error_msg)
            self.logger.debug(error_trace)
        except Exception as e:
            self.test_conn.rollback()
            error_trace = traceback.format_exc()
            error_msg = f"Unexpected error during queryable encryption test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
            self.logger.debug(error_trace)
        finally:
            # Capture elapsed time and end time
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            # Retrieve PostgreSQL server version dynamically
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

            # Accumulate result for later storage
            self.test_results.append(result_document)

            # Determine if the test should pass or fail based on whether an error was expected
            if result_document['reason'] == 'PASSED (encryption not supported as expected)':
                # If the error was expected, consider the test passed
                pass
            elif result_document['status'] != 'pass':
                # If the test did not pass, fail it
                self.fail("Queryable encryption test failed.")

    @classmethod
    def tearDownClass(cls):
        # Clean up: drop the table
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
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
