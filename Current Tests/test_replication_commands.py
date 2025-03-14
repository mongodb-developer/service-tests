import unittest
from datetime import datetime
import traceback
import contextlib
from base_test import BaseTest
import logging
import time
import json
import io
import config

class TestReplicationCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_replication_commands'

        # Configure logging
        cls.logger = logging.getLogger('TestReplicationCommands')
        cls.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler('test_replication_commands.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        # In-memory log capture list and custom log handler
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

        # Initialize streams and warnings list
        cls.stdout_stream = io.StringIO()
        cls.stderr_stream = io.StringIO()
        cls.log_stream = io.StringIO()
        cls.warnings_list = []

        # Initialize test results accumulator
        cls.test_results = []

    def setUp(self):
        self.logger = self.__class__.logger
        self.__class__.log_capture_list.clear()
        for stream in (self.__class__.stdout_stream, self.__class__.stderr_stream, self.__class__.log_stream):
            stream.truncate(0)
            stream.seek(0)
        self.__class__.warnings_list.clear()

    def run_replication_commands_test(self, command_name, command_body):
        """Helper method to run an administrative command test."""
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Replication Commands Test - {command_name}",
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
            result_document['log_lines'] = list(self.__class__.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.__class__.test_results.append(result_document)

    def test_replication_commands(self):
        # Mapping of replication commands to their command bodies
        commands = {
            'appendOplogNote': {'appendOplogNote': 1, 'data': {'msg': 'Test note'}},
            'applyOps': {'applyOps': [{'op': 'i', 'ns': 'test.test_collection', 'o': {'_id': 1, 'field': 'value'}}]},
            'replSetAbortPrimaryCatchUp': {'replSetAbortPrimaryCatchUp': 1},
            'replSetFreeze': {'replSetFreeze': 0},
            'replSetGetConfig': {'replSetGetConfig': 1},
            'replSetGetStatus': {'replSetGetStatus': 1},
            'replSetInitiate': {'replSetInitiate': {'_id': 'rs0', 'members': [{'_id': 0, 'host': 'localhost:27017'}]}},
            'replSetMaintenance': {'replSetMaintenance': 1, 'enableMaintenanceMode': True},
            'replSetReconfig': {'replSetReconfig': {'_id': 'rs0', 'members': [{'_id': 0, 'host': 'localhost:27017'}], 'version': 2}},
            'replSetResizeOplog': {'replSetResizeOplog': 1, 'size': 1024},
            'replSetStepDown': {'replSetStepDown': 1, 'stepDownSecs': 60},
            'replSetSyncFrom': {'replSetSyncFrom': 'localhost:27017'}
        }

        for command_name, command_body in commands.items():
            self.run_replication_commands_test(command_name, command_body)

    @classmethod
    def tearDownClass(cls):
        cls.logger.debug("Tear down completed for TestReplicationCommands.")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
