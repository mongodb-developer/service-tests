# tests/test_diagnostic_commands.py

import unittest
from pymongo.errors import OperationFailure
from datetime import datetime
import traceback
import logging
import json
import time
from base_test import BaseTest

class TestDiagnosticCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_diagnostic_commands'

        # Define admin database
        cls.docdb_admin_db = cls.docdb_client['admin']

        # Configure logging
        cls.logger = logging.getLogger('TestDiagnosticCommands')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_diagnostic_commands.log')
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
            'test_name': f"Diagnostic Command Test - {command_name}",
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
            if command_name == 'buildInfo':
                result = admin_db.command('buildInfo')
            elif command_name == 'connectionStatus':
                result = admin_db.command('connectionStatus')
            elif command_name == 'dbStats':
                result = admin_db.command('dbStats')
            elif command_name == 'getLog':
                result = admin_db.command('getLog', 'global')
            elif command_name == 'ping':
                result = admin_db.command('ping')
            elif command_name == 'serverStatus':
                result = admin_db.command('serverStatus')
            else:
                raise ValueError(f"Unsupported or unknown command: {command_name}")

            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['command_result'] = result
            result_document['log_lines'].append(f"Diagnostic command '{command_name}' executed successfully.")
        except Exception as e:
            error_msg = f"Error executing diagnostic command '{command_name}': {str(e)}"
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
                server_info = admin_db.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_buildInfo(self):
        self.execute_and_store_command('buildInfo')

    def test_connectionStatus(self):
        self.execute_and_store_command('connectionStatus')

    def test_dbStats(self):
        self.execute_and_store_command('dbStats')

    def test_getLog(self):
        self.execute_and_store_command('getLog')

    def test_ping(self):
        self.execute_and_store_command('ping')

    def test_serverStatus(self):
        self.execute_and_store_command('serverStatus')

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
