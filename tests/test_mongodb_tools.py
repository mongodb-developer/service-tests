# tests/test_postgresql_tools.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime, timedelta
import sys
import os
import decimal  # Importing the decimal module
import subprocess

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest
import config  # Ensure this module has PostgreSQL configurations

class TestPostgreSQLTools(BaseTest):
    """
    Test suite for PostgreSQL tools (pg_dump and pg_restore).
    Translates MongoDB/DocumentDB tools tests to PostgreSQL using psycopg2.
    Each tool is tested in a separate unittest method to provide granular results.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_postgresql_tools'

        # Configure logging
        cls.logger = logging.getLogger('TestPostgreSQLTools')
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_postgresql_tools.log'
        file_handler = logging.FileHandler('test_postgresql_tools.log')
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

        cls.dump_directory = 'dump_test_postgresql_tools'
        if os.path.exists(cls.dump_directory):
            subprocess.call(['rm', '-rf', cls.dump_directory])
            cls.logger.debug(f"Existing dump directory '{cls.dump_directory}' removed.")

        # Create table for testing
        try:
            with cls.test_conn.cursor() as cur:
                # Drop table if it exists to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table with appropriate columns
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        _id TEXT PRIMARY KEY,
                        value TEXT,
                        timestamp TIMESTAMP
                    );
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Created table '{cls.table_name}' with all necessary fields.")
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
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(self.table_name)))
                # Recreate the table for the test
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        _id TEXT PRIMARY KEY,
                        value TEXT,
                        timestamp TIMESTAMP
                    );
                """).format(sql.Identifier(self.table_name)))
            self.test_conn.commit()
            self.logger.debug("Reset table before test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error resetting table in setUp: {e}")
            raise

    @staticmethod
    def get_test_data():
        """Generate test data for PostgreSQL tools"""
        base_time = datetime.utcnow()
        return [
            {
                '_id': 'test_1',
                'value': 'data1',
                'timestamp': base_time
            },
            {
                '_id': 'test_2',
                'value': 'data2',
                'timestamp': base_time + timedelta(minutes=5)
            },
            {
                '_id': 'test_3',
                'value': 'data3',
                'timestamp': base_time + timedelta(minutes=10)
            }
        ]

    def insert_test_data(self, test_data):
        """Inserts test data into the table."""
        try:
            with self.test_conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {} (_id, value, timestamp)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (_id) DO NOTHING;
                """).format(sql.Identifier(self.table_name))
                for doc in test_data:
                    cur.execute(insert_query, (
                        doc['_id'],
                        doc['value'],
                        doc['timestamp']
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

    def run_pg_dump(self, dump_path):
        """
        Runs pg_dump to dump the specified table.
        """
        try:
            dump_cmd = [
                config.MONGODUMP_PATH,
                '--dbname', config.POSTGRESQL_URI,
                '--table', self.table_name,
                '--format', 'directory',
                '--file', dump_path
            ]
            self.logger.debug(f"Executing pg_dump command: {' '.join(dump_cmd)}")
            dump_result = subprocess.run(dump_cmd, capture_output=True, text=True)
            if dump_result.returncode != 0:
                error_msg = f"pg_dump failed: {dump_result.stderr}"
                self.logger.error(error_msg)
                return False, error_msg
            self.logger.debug("pg_dump executed successfully.")
            return True, ""
        except Exception as e:
            error_msg = f"Error running pg_dump: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return False, error_msg

    def run_pg_restore(self, dump_path, new_table_name):
        """
        Runs pg_restore to restore the dumped table to a new table.
        """
        try:
            restore_cmd = [
                config.MONGORESTORE_PATH,
                '--dbname', config.DB_HOST,
                '--table', new_table_name,
                '--create',
                '--clean',
                '--if-exists',
                dump_path
            ]
            self.logger.debug(f"Executing pg_restore command: {' '.join(restore_cmd)}")
            restore_result = subprocess.run(restore_cmd, capture_output=True, text=True)
            if restore_result.returncode != 0:
                error_msg = f"pg_restore failed: {restore_result.stderr}"
                self.logger.error(error_msg)
                return False, error_msg
            self.logger.debug("pg_restore executed successfully.")
            return True, ""
        except Exception as e:
            error_msg = f"Error running pg_restore: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return False, error_msg

    def check_restored_data(self, new_table_name, original_data):
        """
        Verifies that the restored data matches the original data.
        """
        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql.SQL("SELECT * FROM {} ORDER BY _id;").format(sql.Identifier(new_table_name)))
                restored_data = cur.fetchall()
            expected_data = [
                {
                    '_id': doc['_id'],
                    'value': doc['value'],
                    'timestamp': doc['timestamp'].isoformat()
                } for doc in original_data
            ]
            # Convert restored_data to comparable format
            restored_data_comparable = [
                {
                    '_id': row['_id'],
                    'value': row['value'],
                    'timestamp': row['timestamp'].isoformat()
                } for row in restored_data
            ]
            if restored_data_comparable == expected_data:
                self.logger.debug("Restored data matches original data.")
                return True, ""
            else:
                mismatch_msg = 'Restored data does not match original data.'
                self.logger.error(mismatch_msg)
                return False, mismatch_msg
        except Exception as e:
            error_msg = f"Error verifying restored data: {e}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return False, error_msg

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

    def test_postgresql_tools(self):
        """Test pg_dump and pg_restore tools"""
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'PostgreSQL Tools Test',
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
            # Step 1: Insert Test Data
            test_data = self.get_test_data()
            success, error_msg = self.insert_test_data(test_data)
            if not success:
                raise Exception(error_msg)
            result_document['log_lines'].append('Data inserted successfully.')
            self.logger.debug("Inserted test data successfully.")

            # Step 2: Run pg_dump
            success, error_msg = self.run_pg_dump(self.dump_directory)
            if not success:
                raise Exception(error_msg)
            result_document['log_lines'].append('pg_dump executed successfully.')
            self.logger.debug("pg_dump executed successfully.")

            # Step 3: Run pg_restore to restore to a new table
            new_table_name = 'restored_' + self.table_name
            success, error_msg = self.run_pg_restore(self.dump_directory, new_table_name)
            if not success:
                raise Exception(error_msg)
            result_document['log_lines'].append('pg_restore executed successfully.')
            self.logger.debug("pg_restore executed successfully.")

            # Step 4: Verify Restored Data
            success, error_msg = self.check_restored_data(new_table_name, test_data)
            if success:
                result_document['log_lines'].append('Restored data matches original data.')
                self.logger.debug("Restored data matches original data.")
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
            else:
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)

        except Exception as e:
            # Capture any exceptions, mark test as failed
            error_message = str(e)
            result_document['status'] = 'fail'
            result_document['exit_code'] = 1
            result_document['reason'] = 'FAILED'
            result_document['description'].append(error_message)
            self.logger.error(f"Error during PostgreSQL Tools test: {error_message}\n{traceback.format_exc()}")

        finally:
            # Step 5: Clean up - Remove dump directory and restored table
            if os.path.exists(self.dump_directory):
                subprocess.call(['rm', '-rf', self.dump_directory])
                result_document['log_lines'].append(f"Dump directory '{self.dump_directory}' removed.")
                self.logger.debug(f"Dump directory '{self.dump_directory}' removed.")

            try:
                with self.test_conn.cursor() as cur:
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(new_table_name)))
                self.test_conn.commit()
                self.logger.debug(f"Dropped restored table '{new_table_name}' after test.")
            except Exception as e_cleanup:
                self.test_conn.rollback()
                self.logger.error(f"Error dropping restored table after test: {e_cleanup}")

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
