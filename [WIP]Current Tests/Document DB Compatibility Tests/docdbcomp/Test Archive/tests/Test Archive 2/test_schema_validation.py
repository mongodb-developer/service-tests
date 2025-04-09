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

        cls.logger = logging.getLogger('TestSchemaValidation')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_schema_validation.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

    def setUp(self):
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

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
            except Exception as e:
                error_msg = f"Schema validation not supported: {str(e)}"
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.warning(error_msg)
                self.docdb_db.create_collection(self.collection_name)
                result_document['log_lines'].append('Collection created without schema validation.')

            valid_doc = {'name': 'Alice', 'age': 30}
            self.docdb_coll.insert_one(valid_doc)
            result_document['log_lines'].append('Valid document inserted successfully.')

            invalid_doc = {'name': 'Bob', 'age': 'thirty'}
            self.docdb_coll.insert_one(invalid_doc)
            error_msg = 'Invalid document inserted without raising an error.'
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)

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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        cls.docdb_db.drop_collection(cls.collection_name)
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
