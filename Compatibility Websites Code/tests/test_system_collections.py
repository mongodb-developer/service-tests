# tests/test_system_collections.py

import unittest
from datetime import datetime
import traceback
import logging
import json
import time
from base_test import BaseTest
import config

class TestSystemCollections(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_system_collections'
        cls.docdb_admin_db = cls.docdb_client['admin']

        cls.logger = logging.getLogger('TestSystemCollections')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_system_collections.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

    def test_system_collections(self):
        admin_db = self.docdb_admin_db
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'System Collections Test',
            'platform': config.PLATFORM,
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_system_collections',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': 'FAILED',
            'description': [],
            'system_collections': [],
        }

        try:
            system_collections = admin_db.list_collection_names()
            result_document['system_collections'] = system_collections
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('System collections listed successfully.')
        except Exception as e:
            error_msg = f"Error listing system collections: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                server_info = admin_db.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
