# tests/test_data_types.py

import unittest
from pymongo.errors import PyMongoError
from bson import Decimal128, ObjectId, Binary, Regex, Code, Timestamp, MinKey, MaxKey, DBRef
from bson.symbol import Symbol
from bson.int64 import Int64
from datetime import datetime
import traceback
import warnings
import contextlib
from base_test import BaseTest
import config
import logging
import time
import json

class TestDataTypes(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_data_types'

        # Define collection
        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        # Drop existing collection
        cls.docdb_coll.drop()

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

    def setUp(self):
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    def insert_and_store_result(self, data, data_type_name):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Data Type Test - {data_type_name}",
            'platform': config.PLATFORM,
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': self.collection_name,
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': 'FAILED',
            'description': [],
            'insert_result': {},
        }

        try:
            collection.insert_one(data)
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['insert_result'] = {'inserted_id': str(data['_id'])}
            self.logger.debug(f"Data type '{data_type_name}' inserted successfully.")
            result_document['log_lines'].append(f"Data type '{data_type_name}' inserted successfully.")
        except Exception as e:
            error_msg = f"Error inserting data type '{data_type_name}': {str(e)}"
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

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_string(self):
        self.insert_and_store_result({'_id': ObjectId(), 'string': 'text'}, 'string')

    def test_integer(self):
        self.insert_and_store_result({'_id': ObjectId(), 'int': 42}, 'integer')

    def test_float(self):
        self.insert_and_store_result({'_id': ObjectId(), 'float': 3.14}, 'float')

    def test_decimal128(self):
        self.insert_and_store_result({'_id': ObjectId(), 'decimal': Decimal128('123.45')}, 'decimal128')

    def test_datetime(self):
        self.insert_and_store_result({'_id': ObjectId(), 'date': datetime.utcnow()}, 'datetime')

    def test_object_id(self):
        self.insert_and_store_result({'_id': ObjectId(), 'object_id': ObjectId()}, 'object_id')

    def test_binary(self):
        self.insert_and_store_result({'_id': ObjectId(), 'binary': Binary(b'binary data', 0)}, 'binary')

    def test_array(self):
        self.insert_and_store_result({'_id': ObjectId(), 'array': [1, 2, 3]}, 'array')

    def test_nested_array(self):
        self.insert_and_store_result({'_id': ObjectId(), 'nested_array': [[1, 2], [3, 4]]}, 'nested_array')

    def test_document(self):
        self.insert_and_store_result({'_id': ObjectId(), 'document': {'nested': 'value'}}, 'document')

    def test_nested_document(self):
        self.insert_and_store_result({'_id': ObjectId(), 'nested_document': {'level1': {'level2': {'level3': 'deep'}}}}, 'nested_document')

    def test_regex(self):
        self.insert_and_store_result({'_id': ObjectId(), 'regex': Regex('^pattern$', 'i')}, 'regex')

    def test_code(self):
        self.insert_and_store_result({'_id': ObjectId(), 'javascript': Code('function() { return true; }')}, 'code')

    def test_code_with_scope(self):
        code_with_scope = Code('function() { return x; }', {'x': 42})
        self.insert_and_store_result({'_id': ObjectId(), 'javascript_with_scope': code_with_scope}, 'code_with_scope')

    def test_timestamp(self):
        self.insert_and_store_result({'_id': ObjectId(), 'timestamp': Timestamp(1628791594, 1)}, 'timestamp')

    def test_minkey(self):
        self.insert_and_store_result({'_id': ObjectId(), 'min_key': MinKey()}, 'min_key')

    def test_maxkey(self):
        self.insert_and_store_result({'_id': ObjectId(), 'max_key': MaxKey()}, 'max_key')

    def test_dbref(self):
        self.insert_and_store_result({'_id': ObjectId(), 'dbref': DBRef('collection', ObjectId(), 'database')}, 'dbref')

    def test_null(self):
        self.insert_and_store_result({'_id': ObjectId(), 'null': None}, 'null')

    def test_boolean_true(self):
        self.insert_and_store_result({'_id': ObjectId(), 'bool_true': True}, 'boolean_true')

    def test_boolean_false(self):
        self.insert_and_store_result({'_id': ObjectId(), 'bool_false': False}, 'boolean_false')

    # ------------------ New Data Type Tests ------------------

    def test_symbol(self):
        self.insert_and_store_result({'_id': ObjectId(), 'symbol': Symbol('mysymbol')}, 'symbol')

    def test_int32(self):
        # Using a 32-bit maximum value: 2147483647
        self.insert_and_store_result({'_id': ObjectId(), 'int32': 2147483647}, 'int32')

    def test_int64(self):
        # Using a 64-bit maximum value via bson.int64.Int64: 9223372036854775807
        self.insert_and_store_result({'_id': ObjectId(), 'int64': Int64(9223372036854775807)}, 'int64')

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        cls.logger.debug("Dropped collection during teardown.")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
