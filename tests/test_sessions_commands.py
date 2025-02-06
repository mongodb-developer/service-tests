# tests/test_sessions_commands.py

import unittest
import logging
import json
import time
import traceback
from datetime import datetime
import sys
import os
import decimal

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest
import config  # Ensure this module has PostgreSQL configurations


class TestPostgreSQLSessionsCommands(BaseTest):
    """
    Test suite for PostgreSQL session and transaction commands.
    Translates MongoDB/DocumentDB sessions commands to PostgreSQL using psycopg2.
    Includes tests for starting sessions, aborting transactions, committing transactions,
    ending sessions, killing all sessions, killing specific sessions, and refreshing sessions.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_postgresql_sessions_commands'
        cls.logger_name = 'TestPostgreSQLSessionsCommands'

        # Configure logging
        cls.logger = logging.getLogger(cls.logger_name)
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_sessions_commands.log'
        file_handler = logging.FileHandler('test_sessions_commands.log')
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

        # Create table for testing (if needed)
        try:
            with cls.test_conn.cursor() as cur:
                # Drop table if it exists to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create a simple table for transaction tests
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        _id SERIAL PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Created table '{cls.table_name}' for transaction tests.")

            cls.test_conn.commit()
            cls.logger.debug(f"Setup for '{cls.table_name}' completed successfully.")
            print(f"Table '{cls.table_name}' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error setting up table: {e}")
            print(f"Error setting up table: {e}")
            raise

    def setUp(self):
        """Reset table and initialize connections before each test"""
        try:
            with self.test_conn.cursor() as cur:
                # Truncate table to remove existing data
                cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(sql.Identifier(self.table_name)))
            self.test_conn.commit()
            self.logger.debug("Truncated table before test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error truncating table in setUp: {e}")
            raise

        # Insert test data
        test_data = self.get_test_data()
        success, error = self.insert_test_data(test_data)
        if not success:
            self.fail(f"Failed to insert test data: {error}")

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

        # Initialize session (connection) to None
        self.session_conn = None

    @staticmethod
    def convert_decimals(obj):
        """Recursively convert Decimal objects to float."""
        if isinstance(obj, list):
            return [TestPostgreSQLSessionsCommands.convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: TestPostgreSQLSessionsCommands.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if precision is important
        else:
            return obj

    def get_test_data(self):
        """Generate sample data for transaction tests"""
        return [
            {'value': 'test1'},
            {'value': 'test2'},
            {'value': 'test3'},
            {'value': 'test4'},
        ]

    def insert_test_data(self, test_data):
        """Inserts test data into the table."""
        try:
            with self.test_conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {} (value)
                    VALUES (%s);
                """).format(sql.Identifier(self.table_name))
                for doc in test_data:
                    cur.execute(insert_query, (doc['value'],))
            self.test_conn.commit()
            self.logger.debug("Inserted test data successfully.")
            return True, ""
        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Failed to insert test data: {e}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return False, error_msg

    def initialize_result_document(self, test_name):
        start_time = time.time()
        return {
            'status': 'fail',
            'test_name': test_name,
            'platform': 'postgresql',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_sessions_commands',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'command_result': {},
        }

    def finalize_result_document(self, result_document, start_time):
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

    def execute_and_store_command(self, command_name):
        admin_db = self.test_conn  # Using test_conn as admin_db equivalent
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document(f"Sessions Command Test - {command_name}")

        try:
            if command_name == 'startSession':
                # Start a new session by creating a new connection
                self.session_conn = psycopg2.connect(config.POSTGRESQL_URI)
                self.session_conn.autocommit = False  # To manage transactions manually
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Session started successfully.')
                self.logger.debug("Session started successfully.")

            elif command_name == 'abortTransaction':
                if self.session_conn is None:
                    raise Exception("No session available to abort transaction.")
                with self.session_conn.cursor() as cur:
                    cur.execute("BEGIN;")
                    # Perform some operations
                    cur.execute(sql.SQL("INSERT INTO {} (value) VALUES (%s);").format(sql.Identifier(self.table_name)), ('transaction_abort',))
                    self.session_conn.rollback()
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Transaction aborted successfully.')
                self.logger.debug("Transaction aborted successfully.")

            elif command_name == 'commitTransaction':
                if self.session_conn is None:
                    raise Exception("No session available to commit transaction.")
                with self.session_conn.cursor() as cur:
                    cur.execute("BEGIN;")
                    # Perform some operations
                    cur.execute(sql.SQL("INSERT INTO {} (value) VALUES (%s);").format(sql.Identifier(self.table_name)), ('transaction_commit',))
                    self.session_conn.commit()
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Transaction committed successfully.')
                self.logger.debug("Transaction committed successfully.")

            elif command_name == 'endSessions':
                if self.session_conn is None:
                    raise Exception("No session available to end.")
                self.session_conn.close()
                self.session_conn = None
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Session ended successfully.')
                self.logger.debug("Session ended successfully.")

            elif command_name == 'killAllSessions':
                # Terminate all other backend connections except the current one
                with self.test_conn.cursor() as cur:
                    # Get current backend PID
                    cur.execute("SELECT pg_backend_pid();")
                    current_pid = cur.fetchone()[0]
                    # Terminate all other backends
                    cur.execute(sql.SQL("""
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE pid <> %s AND state = 'idle';
                    """), (current_pid,))
                    result = cur.fetchall()
                result_document['command_result'] = result
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('killAllSessions command executed successfully.')
                self.logger.debug("killAllSessions command executed successfully.")

            elif command_name == 'killSessions':
                # Terminate specific backend connections (for example, those idle)
                with self.test_conn.cursor() as cur:
                    # Fetch PIDs of idle connections
                    cur.execute(sql.SQL("""
                        SELECT pid
                        FROM pg_stat_activity
                        WHERE state = 'idle' AND pid <> pg_backend_pid();
                    """))
                    pids = cur.fetchall()
                    # Terminate each PID
                    results = []
                    for pid_tuple in pids:
                        pid = pid_tuple[0]
                        cur.execute(sql.SQL("SELECT pg_terminate_backend(%s);"), (pid,))
                        termination_result = cur.fetchone()[0]
                        results.append({'pid': pid, 'terminated': termination_result})
                result_document['command_result'] = results
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('killSessions command executed successfully.')
                self.logger.debug("killSessions command executed successfully.")

            elif command_name == 'refreshSessions':
                # No direct equivalent in PostgreSQL. We'll interpret this as ensuring session state is current.
                # For demonstration, we'll reconnect the session if it exists.
                if self.session_conn is None:
                    raise Exception("No session available to refresh.")
                self.session_conn.close()
                self.session_conn = psycopg2.connect(config.POSTGRESQL_URI)
                self.session_conn.autocommit = False
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Session refreshed successfully.')
                self.logger.debug("Session refreshed successfully.")

            else:
                raise ValueError(f"Unsupported or unknown command: {command_name}")

        except (psycopg2.OperationalError, psycopg2.Error) as e:
            error_msg = f"Error executing sessions command '{command_name}': {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Exception during sessions command '{command_name}': {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
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

    def test_startSession(self):
        self.execute_and_store_command('startSession')

    def test_abortTransaction(self):
        self.execute_and_store_command('abortTransaction')

    def test_commitTransaction(self):
        self.execute_and_store_command('commitTransaction')

    def test_endSessions(self):
        self.execute_and_store_command('endSessions')

    def test_killAllSessions(self):
        self.execute_and_store_command('killAllSessions')

    def test_killSessions(self):
        self.execute_and_store_command('killSessions')

    def test_refreshSessions(self):
        self.execute_and_store_command('refreshSessions')

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        # Close any active session connection
        if hasattr(cls, 'session_conn') and cls.session_conn is not None:
            cls.session_conn.close()
            cls.logger.debug("Active session connection closed during teardown.")

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
