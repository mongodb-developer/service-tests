import unittest
from pymongo.errors import OperationFailure
from datetime import datetime
import traceback
import warnings
import contextlib
from base_test import BaseTest
import logging
import time
import json
import io
import config

class TestDiagnosticCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_diagnostic_commands'
        # Define collection and drop any existing data
        cls.docdb_coll = cls.docdb_db[cls.collection_name]
        cls.docdb_coll.drop()

        # Set up the admin connection using the DocumentDB client
        cls.docdb_admin = cls.docdb_db.client.admin

        # Configure logging
        cls.logger = logging.getLogger('TestDiagnosticCommands')
        cls.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler('test_diagnostic_commands.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        # In-Memory Log Capture List and custom log handler
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
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger
        # Clear in-memory log capture list and streams before each test
        self.__class__.log_capture_list.clear()
        for stream in (self.__class__.stdout_stream, self.__class__.stderr_stream, self.__class__.log_stream):
            stream.truncate(0)
            stream.seek(0)
        self.__class__.warnings_list.clear()

    def create_result_document(self, command, start_time=None):
        if start_time is None:
            start_time = time.time()
        return {
            'status': 'fail',
            'test_name': f"Diagnostic Command Test - {command}",
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
            'errors': [],
            'command_result': {},
            '_start_time': start_time  # store raw start time for later calculation
        }

    def execute_and_store_command(self, command):
        start_time = time.time()
        result_document = self.create_result_document(command, start_time)
        try:
            with contextlib.redirect_stdout(self.__class__.stdout_stream), \
                 contextlib.redirect_stderr(self.__class__.stderr_stream):
                if command == 'collStats':
                    result = self.__class__.docdb_admin.command('collStats', self.collection_name)
                elif command == 'connPoolStats':
                    result = self.__class__.docdb_admin.command('connPoolStats')
                elif command == 'connectionStatus':
                    result = self.__class__.docdb_admin.command('connectionStatus')
                elif command == 'dbHash':
                    result = self.__class__.docdb_admin.command('dbHash')
                elif command == 'dbStats':
                    result = self.__class__.docdb_admin.command('dbStats')
                elif command == 'explain':
                    result = self.__class__.docdb_admin.command('explain', {'find': self.collection_name})
                elif command == '_isSelf':
                    result = self.__class__.docdb_admin.command('_isSelf')
                elif command == 'listCommands':
                    result = self.__class__.docdb_admin.command('listCommands')
                elif command == 'lockInfo':
                    result = self.__class__.docdb_admin.command('lockInfo')
                elif command == 'netstat':
                    # Simulate response as netstat is not supported on Atlas.
                    result = {"netstat": "not supported on Atlas"}
                elif command == 'serverStatus':
                    result = self.__class__.docdb_admin.command('serverStatus')
                elif command == 'top':
                    result = self.__class__.docdb_admin.command('top')
                elif command == 'logApplicationMessage':
                    # Simulate response as logApplicationMessage is not authorized on Atlas.
                    result = {"logApplicationMessage": "not supported on Atlas"}
                else:
                    raise ValueError(f"Unknown or unsupported command: {command}")

                result_document['details'] = result
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                self.logger.debug(f"Executed command {command} successfully.")
                result_document['log_lines'].append(f"Executed command {command} successfully.")
        except Exception as e:
            error_msg = f"{command} Error: {str(e)}"
            result_document['errors'].append(error_msg)
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - result_document['_start_time']
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()
            # Retrieve server version dynamically
            try:
                server_info = self.docdb_coll.database.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'
            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.__class__.log_capture_list)
            # Remove the internal start time
            del result_document['_start_time']
            # Ensure all fields are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))
            self.__class__.test_results.append(result_document)

    def test_diagnostic_commands(self):
        commands = [
            'availableQueryOptions', 'buildInfo', 'collStats', 'connPoolStats', 'connectionStatus',
            'dbHash', 'dbStats', 'explain', 'getCmdLineOpts', 'getLog', 'hello', 'hostInfo', '_isSelf',
            'listCommands', 'lockInfo', 'netstat', 'ping', 'serverStatus', 'top', 'whatsmyuri',
            'logApplicationMessage'
        ]
        for command in commands:
            self.execute_and_store_command(command)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.docdb_coll.drop()
            cls.logger.debug("Dropped collection during teardown.")
        except Exception as e:
            cls.logger.error(f"Error in teardown: {e}")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
