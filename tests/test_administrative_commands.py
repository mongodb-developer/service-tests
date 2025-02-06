# tests/test_administrative_commands.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime
import sys
import os

from psycopg2.extras import RealDictCursor

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest

class TestAdministrativeCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.logger = logging.getLogger('TestAdministrativeCommands')
        cls.logger.setLevel(logging.DEBUG)

        # Set up file handler and list handler
        file_handler = logging.FileHandler('test_administrative_commands.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        cls.log_capture_list = []

        class ListHandler(logging.Handler):
            def __init__(self, log_list):
                super().__init__()
                self.log_list = log_list

            def emit(self, record):
                log_entry = self.format(record)
                self.log_list.append(log_entry)

        list_handler = ListHandler(cls.log_capture_list)
        list_handler.setFormatter(formatter)
        cls.logger.addHandler(list_handler)

        # Create a test table for administrative commands
        try:
            with cls.test_conn.cursor() as cur:
                # Drop if exists to start clean
                cur.execute("DROP TABLE IF EXISTS test_admin_commands;")
                # Create a table
                cur.execute("""
                    CREATE TABLE test_admin_commands (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(50),
                        age INTEGER
                    );
                """)
            cls.test_conn.commit()
            print("Table 'test_admin_commands' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            print(f"Error creating table 'test_admin_commands': {e}")
            raise

    def setUp(self):
        """Set up for each test method."""
        # Assign class variables to instance variables for easy access
        self.logger = self.__class__.logger
        self.test_conn = self.__class__.test_conn

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

        # Ensure the table is clean before each test
        try:
            with self.test_conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE test_admin_commands;")
                self.test_conn.commit()
            self.logger.debug("Truncated 'test_admin_commands' table before test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error truncating table: {e}")

        # Insert sample data
        sample_data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25},
            {'name': 'Charlie', 'age': 35}
        ]
        try:
            with self.test_conn.cursor() as cur:
                for data in sample_data:
                    cur.execute("""
                        INSERT INTO test_admin_commands (name, age)
                        VALUES (%s, %s)
                    """, (data['name'], data['age']))
            self.test_conn.commit()
            self.logger.debug("Inserted sample data into 'test_admin_commands' table.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error inserting data: {e}")

    def run_admin_command_test(self, command_name, command_body):
        """Helper method to run an administrative command test."""
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Administrative Command Test - {command_name}",
            'platform': 'postgresql',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_admin_commands',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'command_result': {},
        }

        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                for key, value in command_body.items():
                    if key == 'collMod':
                        # Simulate collMod by altering table
                        table_name = value
                        alter_commands = command_body.get('alter_commands', {})
                        for cmd, params in alter_commands.items():
                            if cmd == 'add_column':
                                column_name = params['name']
                                data_type = params['type']
                                cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type};")
                                self.logger.debug(f"Added column '{column_name}' of type '{data_type}' to '{table_name}'.")
                            elif cmd == 'drop_column':
                                column_name = params['name']
                                cur.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name};")
                                self.logger.debug(f"Dropped column '{column_name}' from '{table_name}'.")
                            # Add more alter commands as needed
                    elif key == 'create':
                        # Simulate create by creating a new table
                        new_table = value
                        cur.execute(f"""
                            CREATE TABLE {new_table} (
                                id SERIAL PRIMARY KEY,
                                description TEXT
                            );
                        """)
                        self.logger.debug(f"Created new table '{new_table}'.")
                    elif key == 'listDatabases':
                        # Simulate listDatabases by listing all databases
                        cur.execute("""
                            SELECT datname FROM pg_database WHERE datistemplate = false;
                        """)
                        databases = cur.fetchall()
                        result_document['command_result'] = {'databases': [db['datname'] for db in databases]}
                        self.logger.debug("Listed all databases.")
                    elif key == 'listIndexes':
                        # Simulate listIndexes by listing indexes on a table
                        table_name = value
                        cur.execute("""
                            SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s;
                        """, (table_name,))
                        indexes = cur.fetchall()
                        result_document['command_result'] = {'indexes': indexes}
                        self.logger.debug(f"Listed indexes for table '{table_name}'.")
                    elif key == 'validate':
                        # Simulate validate by checking table integrity using pg_class and pg_namespace
                        table_name = value
                        cur.execute("""
                            SELECT relname, relkind
                            FROM pg_class
                            WHERE relname = %s AND relkind = 'r';
                        """, (table_name,))
                        table = cur.fetchone()
                        if table:
                            result_document['command_result'] = {'table': table}
                            self.logger.debug(f"Validated existence of table '{table_name}'.")
                        else:
                            raise Exception(f"Table '{table_name}' does not exist.")
                    # Add more administrative commands as needed

            self.test_conn.commit()
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            self.logger.debug(f"Command '{command_name}' executed successfully.")
        except Exception as e:
            self.test_conn.rollback()
            error_trace = traceback.format_exc()
            error_msg = f"Exception executing command '{command_name}': {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(f"Error executing command '{command_name}': {e}\n{error_trace}")
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
                server_version = version_info['version'] if version_info else 'unknown'
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    # Implement tests for each administrative command
    def test_collMod_add_column(self):
        command_name = 'collMod - add_column'
        command_body = {
            'collMod': 'test_admin_commands',
            'alter_commands': {
                'add_column': {
                    'name': 'email',
                    'type': 'VARCHAR(100)'
                }
            }
        }
        self.run_admin_command_test(command_name, command_body)

    def test_collMod_drop_column(self):
        command_name = 'collMod - drop_column'
        command_body = {
            'collMod': 'test_admin_commands',
            'alter_commands': {
                'drop_column': {
                    'name': 'email'
                }
            }
        }
        self.run_admin_command_test(command_name, command_body)

    def test_create(self):
        command_name = 'create'
        command_body = {
            'create': 'test_admin_commands_new'
        }
        self.run_admin_command_test(command_name, command_body)

    def test_listDatabases(self):
        command_name = 'listDatabases'
        command_body = {
            'listDatabases': 1
        }
        self.run_admin_command_test(command_name, command_body)

    def test_listIndexes(self):
        command_name = 'listIndexes'
        command_body = {
            'listIndexes': 'test_admin_commands'
        }
        self.run_admin_command_test(command_name, command_body)

    def test_validate_existing_table(self):
        command_name = 'validate'
        command_body = {
            'validate': 'test_admin_commands'
        }
        self.run_admin_command_test(command_name, command_body)

    def test_validate_nonexistent_table(self):
        command_name = 'validate'
        command_body = {
            'validate': 'nonexistent_table'
        }
        self.run_admin_command_test(command_name, command_body)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_admin_commands;")
                cur.execute("DROP TABLE IF EXISTS test_admin_commands_new;")
            cls.test_conn.commit()
            cls.logger.debug("Dropped tables during teardown.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error in teardown: {str(e)}")
        finally:
            super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
