# tests/test_error_handling.py

import unittest
from pymongo import errors
from datetime import datetime
import traceback
import logging
import json
import time
from base_test import BaseTest

class TestErrorHandling(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_error_handling'

        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        cls.docdb_coll.drop()

        # Configure logging
        cls.logger = logging.getLogger('TestErrorHandling')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_error_handling.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

    def setUp(self):
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

    def test_duplicate_key_error(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Duplicate Key Error Test',
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

        data = {'_id': 1, 'value': 'test'}

        try:
            # Insert initial document
            collection.insert_one(data)
            result_document['details']['initial_insert'] = 'Success'
            # Attempt to insert duplicate
            collection.insert_one(data)
            error_msg = 'DuplicateKeyError was not raised on duplicate insert.'
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except errors.DuplicateKeyError:
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('DuplicateKeyError correctly raised on duplicate insert.')
        except Exception as e:
            error_msg = f"Unexpected error during duplicate key test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            # Capture elapsed time and end time
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            # Retrieve server version dynamically
            try:
                server_info = collection.database.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
