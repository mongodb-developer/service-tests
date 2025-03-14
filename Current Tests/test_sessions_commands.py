import unittest
from pymongo.errors import OperationFailure
from datetime import datetime
import traceback
import contextlib
from base_test import BaseTest
import logging
import time
import json
import io
import config

class TestSessionsCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_sessions_commands'
        # Use a single admin interface from our documentDB reference.
        cls.admin = cls.docdb_db.client  # Admin commands will be issued via this client.
        cls.sessions = {}

        # Configure logging
        cls.logger = logging.getLogger('TestSessionsCommands')
        cls.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler('test_sessions_commands.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        # In-memory log capture list and custom handler
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
        # Clear streams and warnings before each test
        self.__class__.log_capture_list.clear()
        for stream in (self.__class__.stdout_stream, self.__class__.stderr_stream, self.__class__.log_stream):
            stream.truncate(0)
            stream.seek(0)
        self.__class__.warnings_list.clear()

    # ---------------- Helper Methods ----------------
    def initialize_result_document(self, test_name):
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Sessions Command Test - {test_name}",
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
            'errors': [],
            'reason': 'FAILED',
            'description': [],
            'command_result': {},
            '_start_time': start_time
        }
        return result_document

    def finalize_result_document(self, result_document):
        end_time = time.time()
        result_document['elapsed'] = end_time - result_document['_start_time']
        result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()
        try:
            server_info = self.docdb_db.client.server_info()
            server_version = server_info.get('version', 'unknown')
            result_document['version'] = server_version
        except Exception as ve:
            self.logger.error(f"Error retrieving server version: {ve}")
            result_document['version'] = 'unknown'
        result_document['log_lines'] = list(self.__class__.log_capture_list)
        del result_document['_start_time']
        result_document = json.loads(json.dumps(result_document, default=str))
        self.__class__.test_results.append(result_document)

    def execute_and_store_session_command(self, command):
        result_document = self.initialize_result_document(command)
        try:
            # For session-related commands, use "documentdb" as the environment.
            env = "documentdb"
            # For commands that require an existing session, check if one exists.
            if command in ['startSession', 'abortTransaction', 'commitTransaction', 'endSessions',
                           'killSessions', 'refreshSessions']:
                if command == 'startSession':
                    if env not in self.sessions:
                        session = self.docdb_db.command('startSession')
                        self.sessions[env] = session
                        result = {'session_id': session.get('id')}
                    else:
                        result = {'session_id': self.sessions[env].get('id')}
                elif command == 'abortTransaction':
                    if env in self.sessions:
                        session = self.sessions[env]
                        session.start_transaction()
                        result = session.abort_transaction()
                    else:
                        raise ValueError("No active session for abortTransaction")
                elif command == 'commitTransaction':
                    if env in self.sessions:
                        session = self.sessions[env]
                        session.start_transaction()
                        result = session.commit_transaction()
                    else:
                        raise ValueError("No active session for commitTransaction")
                elif command == 'endSessions':
                    if env in self.sessions:
                        session = self.sessions.pop(env)
                        try:
                            result = self.docdb_db.command('endSessions', sessions=[session])
                        except OperationFailure as e:
                            error_msg = f"endSessions command failed: {e}"
                            result_document['errors'].append(error_msg)
                            result_document['command_result'] = {'error': error_msg}
                            result = None
                    else:
                        raise ValueError("No active session for endSessions")
                elif command == 'killSessions':
                    if env in self.sessions:
                        session = self.sessions[env]
                        result = self.docdb_db.command('killSessions', sessions=[session])
                    else:
                        raise ValueError("No active session for killSessions")
                elif command == 'refreshSessions':
                    if env in self.sessions:
                        session = self.sessions[env]
                        result = self.docdb_db.command('refreshSessions', sessions=[session])
                    else:
                        raise ValueError("No active session for refreshSessions")
                else:
                    raise ValueError(f"Unknown session command: {command}")
            elif command in ['killAllSessions', 'killAllSessionsByPattern']:
                if command == 'killAllSessions':
                    result = self.docdb_db.command('killAllSessions')
                else:
                    result = self.docdb_db.command('killAllSessionsByPattern', pattern={})
            else:
                raise ValueError(f"Unknown command or insufficient setup for command: {command}")

            if result is not None:
                result_document['command_result'] = result
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
        except Exception as e:
            error_trace = traceback.format_exc()
            error_msg = f"{command} Error: {e}"
            result_document['errors'].append(error_msg)
            result_document['command_result'] = {'error': error_msg}
        finally:
            result_document['warnings'] = [str(w.message) for w in self.__class__.warnings_list]
            result_document['stdout'] = self.__class__.stdout_stream.getvalue()
            result_document['stderr'] = self.__class__.stderr_stream.getvalue()
            self.finalize_result_document(result_document)

    # ---------------- Test Method ----------------
    def test_sessions_commands(self):
        commands = [
            'startSession', 'abortTransaction', 'commitTransaction', 'endSessions',
            'killAllSessions', 'killAllSessionsByPattern', 'killSessions', 'refreshSessions'
        ]
        # Execute each command (only once in our unified environment)
        for command in commands:
            self.execute_and_store_session_command(command)

    @classmethod
    def tearDownClass(cls):
        # Clean up any remaining sessions without explicit ending
        cls.sessions.clear()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
