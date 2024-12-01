# tests/test_sessions_commands.py

import unittest
from pymongo.errors import OperationFailure, InvalidOperation, ConfigurationError
from datetime import datetime
import traceback
import logging
import json
import time
from base_test import BaseTest

class TestSessionsCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_sessions_commands'
        cls.docdb_admin_db = cls.docdb_client['admin']
        cls.docdb_coll = cls.docdb_db[cls.collection_name]
        cls.docdb_coll.drop()

        # Configure logging
        cls.logger = logging.getLogger('TestSessionsCommands')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_sessions_commands.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

    def setUp(self):
        self.docdb_admin_db = self.__class__.docdb_admin_db
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger
        self.session = None  # Initialize session to None

    def execute_and_store_command(self, command_name):
        admin_db = self.docdb_admin_db
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Sessions Command Test - {command_name}",
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_sessions_commands',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'command_result': {},
        }

        try:
            if command_name == 'startSession':
                session = admin_db.client.start_session()
                self.session = session  # Store the session for other tests
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Session started successfully.')
            elif command_name == 'abortTransaction':
                session = getattr(self, 'session', None)
                if session is None:
                    raise Exception("No session available to abort transaction.")
                session.start_transaction()
                # Perform some operations
                collection.insert_one({'_id': 1, 'value': 'test'}, session=session)
                session.abort_transaction()
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Transaction aborted successfully.')
            elif command_name == 'commitTransaction':
                session = getattr(self, 'session', None)
                if session is None:
                    raise Exception("No session available to commit transaction.")
                session.start_transaction()
                # Perform some operations
                collection.insert_one({'_id': 2, 'value': 'test'}, session=session)
                session.commit_transaction()
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Transaction committed successfully.')
            elif command_name == 'endSessions':
                session = getattr(self, 'session', None)
                if session is None:
                    raise Exception("No session available to end.")
                session.end_session()
                self.session = None
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Session ended successfully.')
            elif command_name == 'killAllSessions':
                result = admin_db.command('killAllSessions')
                result_document['command_result'] = result
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('killAllSessions command executed successfully.')
            elif command_name == 'killSessions':
                session = getattr(self, 'session', None)
                if session is None:
                    raise Exception("No session available to kill.")
                session_id = session.session_id
                result = admin_db.command('killSessions', [session_id])
                result_document['command_result'] = result
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('killSessions command executed successfully.')
            elif command_name == 'refreshSessions':
                session = getattr(self, 'session', None)
                if session is None:
                    raise Exception("No session available to refresh.")
                result = admin_db.command('refreshSessions', [session.session_id])
                result_document['command_result'] = result
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('refreshSessions command executed successfully.')
            else:
                raise ValueError(f"Unsupported or unknown command: {command_name}")
        except (OperationFailure, InvalidOperation, ConfigurationError) as e:
            error_msg = f"Error executing sessions command '{command_name}': {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Exception during sessions command '{command_name}': {str(e)}"
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

    #def test_startSession(self):
        #self.execute_and_store_command('startSession')

    def test_abortTransaction(self):
        self.execute_and_store_command('abortTransaction')

    def test_commitTransaction(self):
        self.execute_and_store_command('commitTransaction')

    def test_endSessions(self):
        self.execute_and_store_command('endSessions')

    def test_killAllSessions(self):
        self.execute_and_store_command('killAllSessions')

    def test_killSessions(self):
        self.execute_and_store_command('killSessions')

    def test_refreshSessions(self):
        self.execute_and_store_command('refreshSessions')

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
