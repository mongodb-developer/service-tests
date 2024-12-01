# tests/test_administrative_commands.py

import unittest
from pymongo.errors import PyMongoError, OperationFailure
from datetime import datetime
import traceback
import logging
import time
import json
from base_test import BaseTest

class TestAdministrativeCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_admin_commands'

        # Define collection
        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        # Drop existing collection
        cls.docdb_coll.drop()

        # Configure logging
        cls.logger = logging.getLogger('TestAdministrativeCommands')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_administrative_commands.log'
        file_handler = logging.FileHandler('test_administrative_commands.log')
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

    def setUp(self):
        """Set up for each test method."""
        # Assign class variables to instance variables for easy access
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

        # Ensure the collection is clean before each test
        try:
            self.docdb_coll.drop()
            self.logger.debug("Dropped existing collection before test.")
        except Exception as e:
            self.logger.error(f"Error dropping DocumentDB collection: {e}")

        # Insert sample data
        sample_data = [
            {'_id': 1, 'name': 'Alice', 'age': 30},
            {'_id': 2, 'name': 'Bob', 'age': 25},
            {'_id': 3, 'name': 'Charlie', 'age': 35}
        ]
        try:
            self.docdb_coll.insert_many(sample_data)
            self.logger.debug("Inserted sample data into DocumentDB.")
        except Exception as e:
            self.logger.error(f"Error inserting data into DocumentDB: {e}")

    def run_admin_command_test(self, command_name, command_body):
        """Helper method to run an administrative command test."""
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Administrative Command Test - {command_name}",
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': self.collection_name,
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'command_result': {},
        }

        try:
            # Execute the command
            command_result = self.docdb_db.command(command_body)
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['command_result'] = command_result
            self.logger.debug(f"Command '{command_name}' executed successfully.")
        except Exception as e:
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
                server_info = self.docdb_db.client.server_info()
                server_version = server_info.get('version', 'unknown')
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
    def test_collMod(self):
        command_name = 'collMod'
        command_body = {
            'collMod': self.collection_name,
            'validator': {'age': {'$gte': 0}}
        }
        self.run_admin_command_test(command_name, command_body)

    def test_create(self):
        command_name = 'create'
        command_body = {
            'create': f'{self.collection_name}_new'
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
            'listIndexes': self.collection_name
        }
        self.run_admin_command_test(command_name, command_body)

    def test_validate(self):
        command_name = 'validate'
        command_body = {
            'validate': self.collection_name
        }
        self.run_admin_command_test(command_name, command_body)

    # You can add more administrative command tests as needed, following the same pattern.

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        try:
            cls.docdb_coll.drop()
            cls.logger.debug("Dropped collection during teardown.")
        except Exception as e:
            cls.logger.error(f"Error in teardown: {str(e)}")
        finally:
            super().tearDownClass()

if __name__ == '__main__':
    unittest.main()