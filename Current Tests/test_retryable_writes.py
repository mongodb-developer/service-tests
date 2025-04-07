# tests/test_retryable_writes.py

import unittest
import time
import json
import logging
import traceback
from datetime import datetime
from pymongo.errors import OperationFailure, PyMongoError
import config
from base_test import BaseTest

class TestRetryableWrites(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_retryable_writes'
        cls.collection = cls.docdb_db[cls.collection_name]

        # Ensure a clean slate by dropping the collection if it exists.
        cls.collection.drop()

        # Configure logging similar to the aggregation test.
        cls.logger = logging.getLogger('TestRetryableWrites')
        cls.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler('test_retryable_writes.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        # In-Memory Log Capture List
        cls.log_capture_list = []

        # Custom Handler to capture logs in memory.
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

        # List to accumulate test results.
        cls.test_results = []

    def setUp(self):
        self.collection = self.__class__.collection
        self.logger = self.__class__.logger
        # Clear the in-memory log capture list before each test.
        self.__class__.log_capture_list.clear()

    def execute_retryable_write_test(self, doc, description):
        """
        Executes a retryable write test by simulating a transient failure.
        On the first attempt, the insert_one command will raise an OperationFailure.
        On the second attempt, the command is re-transmitted and succeeds.
        """
        start_time = time.time()
        result_document = {
            'status': 'fail',  # Default to 'fail'; will update based on conditions.
            'test_name': f'Retryable Write Test - {description}',
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
            'reason': '',
            'attempts': 0,
        }

        # Save the original insert_one method.
        original_insert_one = self.collection.insert_one

        # Define a flaky version of insert_one that fails once.
        def flaky_insert_one(document):
            if not hasattr(flaky_insert_one, "called"):
                flaky_insert_one.called = 0
            flaky_insert_one.called += 1
            self.logger.debug("flaky_insert_one attempt %d", flaky_insert_one.called)
            # Simulate failure on the first attempt.
            if flaky_insert_one.called == 1:
                raise OperationFailure("Simulated transient error for retryable write")
            else:
                return original_insert_one(document)

        # Replace the insert_one method with our flaky version.
        self.collection.insert_one = flaky_insert_one

        try:
            # Attempt to perform the write operation.
            result = self.collection.insert_one(doc)
            result_document['attempts'] = flaky_insert_one.called
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            self.logger.info("Document inserted with _id: %s after %d attempts", result.inserted_id, flaky_insert_one.called)
        except Exception as e:
            self.logger.error("Error during retryable write test: %s\n%s", e, traceback.format_exc())
            result_document['reason'] = str(e)
            result_document['attempts'] = flaky_insert_one.called if hasattr(flaky_insert_one, "called") else 0
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()
            try:
                server_info = self.collection.database.client.server_info()
                result_document['version'] = server_info.get('version', 'unknown')
            except Exception as ve:
                self.logger.error("Error retrieving server version: %s", ve)
                result_document['version'] = 'unknown'

            result_document['log_lines'] = list(self.__class__.log_capture_list)
            # Restore the original insert_one method.
            self.collection.insert_one = original_insert_one
            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

        # Assert that a retry occurred (i.e. more than one attempt) and that the document was inserted.
        self.assertGreater(flaky_insert_one.called, 1, "Retry did not occur")
        self.assertEqual(result.inserted_id, doc['_id'], "Document _id does not match the expected value.")

    def test_retryable_write(self):
        """
        Test a retryable write operation.
        The test simulates receiving a write operation submission, executes the write,
        detects a simulated transient error, and then re-executes the write operation.
        """
        test_doc = {"_id": "retry_test", "data": "test"}
        self.execute_retryable_write_test(test_doc, "Simulated Transient Failure Test")

    @classmethod
    def tearDownClass(cls):
        # Clean up the test collection after all tests.
        cls.collection.drop()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
