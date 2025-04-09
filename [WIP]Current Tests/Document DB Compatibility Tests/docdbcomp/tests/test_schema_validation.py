# tests/test_schema_validation.py

import unittest
from pymongo.errors import WriteError
from datetime import datetime
import traceback
import logging
import json
import time
from base_test import BaseTest

class TestSchemaValidation(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_schema_validation'

        cls.docdb_coll = cls.docdb_db[cls.collection_name]
        cls.docdb_coll.drop()

        # Configure logging
        cls.logger = logging.getLogger('TestSchemaValidation')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_schema_validation.log'
        file_handler = logging.FileHandler('test_schema_validation.log')
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
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    def test_schema_validation(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Schema Validation Test',
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
            'details': {},
        }

        try:
            schema = {
                'bsonType': 'object',
                'required': ['name', 'age'],
                'properties': {
                    'name': {
                        'bsonType': 'string',
                        'description': 'must be a string and is required'
                    },
                    'age': {
                        'bsonType': 'int',
                        'minimum': 0,
                        'maximum': 120,
                        'description': 'must be an integer in [0, 120] and is required'
                    }
                }
            }

            try:
                self.docdb_db.create_collection(
                    self.collection_name,
                    validator={'$jsonSchema': schema},
                    validationAction='error'
                )
                result_document['log_lines'].append('Collection with schema validation created successfully.')
                self.logger.debug("Collection with schema validation created successfully.")
            except Exception as e:
                error_msg = f"Schema validation not supported: {str(e)}"
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.warning(error_msg)
                try:
                    self.docdb_db.create_collection(self.collection_name)
                    result_document['log_lines'].append('Collection created without schema validation.')
                    self.logger.debug("Collection created without schema validation.")
                except Exception as e2:
                    error_msg = f"Error creating regular collection: {str(e2)}"
                    result_document['description'].append(error_msg)
                    result_document['reason'] = 'FAILED'
                    self.logger.error(error_msg)

            valid_doc = {'name': 'Alice', 'age': 30}
            collection.insert_one(valid_doc)
            result_document['log_lines'].append('Valid document inserted successfully.')
            self.logger.debug("Valid document inserted successfully.")

            try:
                invalid_doc = {'name': 'Bob', 'age': 'thirty'}
                collection.insert_one(invalid_doc)
                error_msg = 'Invalid document inserted without raising an error.'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
            except WriteError as we:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Schema validation correctly rejected invalid document.')
                self.logger.debug("Schema validation correctly rejected invalid document.")
        except Exception as e:
            error_msg = f"Error during schema validation test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                server_info = collection.database.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        cls.docdb_db.drop_collection(cls.collection_name)
        cls.logger.debug("Dropped collection during teardown.")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
