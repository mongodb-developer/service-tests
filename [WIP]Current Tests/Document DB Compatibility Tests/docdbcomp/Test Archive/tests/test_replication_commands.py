# tests/test_replication_commands.py

import unittest
from pymongo.errors import OperationFailure
from datetime import datetime
import traceback
import logging
import json
import time
from base_test import BaseTest

class TestReplicationCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_replication_commands'

        cls.docdb_admin_db = cls.docdb_client['admin']

        cls.logger = logging.getLogger('TestReplicationCommands')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_replication_commands.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

    def setUp(self):
        self.docdb_admin_db = self.__class__.docdb_admin_db
        self.logger = self.__class__.logger

    def execute_and_store_command(self, command_name):
        admin_db = self.docdb_admin_db
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Replication Command Test - {command_name}",
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
            if command_name == 'replSetGetStatus':
                result = admin_db.command('replSetGetStatus')
            else:
                raise ValueError(f"Unsupported or unknown command: {command_name}")

            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['command_result'] = result
            result_document['log_lines'].append(f"Replication command '{command_name}' executed successfully.")
        except Exception as e:
            error_msg = f"Error executing replication command '{command_name}': {str(e)}"
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

    def test_replSetGetStatus(self):
        self.execute_and_store_command('replSetGetStatus')

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
