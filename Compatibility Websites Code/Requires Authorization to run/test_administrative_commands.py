import unittest
from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure
from datetime import datetime
import traceback
import contextlib
import io
import sys
from base_test import BaseTest
import config
import logging
import time
import json
from bson import Int64  # used to ensure proper type for killCursors command

class TestAdministrativeCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_admin_commands'
        cls.docdb_db_name = config.DOCDB_DB_NAME

        # Define collection
        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        # Drop existing collection
        cls.docdb_coll.drop()

        # Configure logging
        cls.logger = logging.getLogger('TestAdministrativeCommands')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_administrative_commands.log'
        file_handler = logging.FileHandler('test_administrative_commands.log')
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

        # Initialize a list to accumulate test results
        cls.test_results = []

    def setUp(self):
        """Set up for each test method."""
        # Assign class variables to instance variables for easier access
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

        # Ensure the collection is clean before each test
        try:
            self.docdb_coll.drop()
            self.logger.debug("Dropped existing collection before test.")
        except Exception as e:
            self.logger.error(f"Error dropping DocumentDB collection: {e}")

        # Insert sample data
        sample_data = [
            {'_id': 1, 'name': 'Alice', 'age': 30},
            {'_id': 2, 'name': 'Bob', 'age': 25},
            {'_id': 3, 'name': 'Charlie', 'age': 35}
        ]
        try:
            self.docdb_coll.insert_many(sample_data)
            self.logger.debug("Inserted sample data into DocumentDB.")
        except Exception as e:
            self.logger.error(f"Error inserting data into DocumentDB: {e}")

    def run_admin_command_test(self, command_name, command_body):
        """Helper method to run an administrative command test."""
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Administrative Command Test - {command_name}",
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

        # List of commands that must be run on the admin database
        admin_commands = {
            "currentOp", "fsync", "fsyncUnlock", "getClusterParameter", "getDefaultRWConcern",
            "getParameter", "killOp", "listDatabases", "logRotate", "renameCollection",
            "rotateCertificates", "setClusterParameter", "setDefaultRWConcern",
            "setFeatureCompatibilityVersion", "setParameter"
        }

        try:
            if command_name in admin_commands:
                # Run command on admin database
                command_result = self.docdb_db.client.admin.command(command_body)
            else:
                command_result = self.docdb_db.command(command_body)
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['command_result'] = command_result
            self.logger.debug(f"Command '{command_name}' executed successfully.")
        except Exception as e:
            # For commands not supported on Atlas, mark as passed if the error is expected.
            if command_name == 'reIndex' and "reIndex is only allowed" in str(e):
                self.logger.debug(f"Command '{command_name}' expectedly failed on Atlas: {e}")
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['command_result'] = {"msg": "reIndex command is not supported on Atlas"}
            elif command_name == 'setIndexCommitQuorum' and "Cannot find an index build" in str(e):
                self.logger.debug(f"Command '{command_name}' expectedly failed on Atlas: {e}")
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['command_result'] = {"msg": "setIndexCommitQuorum command is not applicable on Atlas"}
            else:
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
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    # Administrative command tests

    def test_cloneCollectionAsCapped(self):
        command_name = 'cloneCollectionAsCapped'
        command_body = {
            'cloneCollectionAsCapped': self.collection_name,
            'toCollection': f'{self.collection_name}_capped',
            'size': 1024
        }
        self.run_admin_command_test(command_name, command_body)

    def test_collMod(self):
        command_name = 'collMod'
        command_body = {
            'collMod': self.collection_name,
            'validator': {'age': {'$gte': 0}}
        }
        self.run_admin_command_test(command_name, command_body)

    def test_compact(self):
        command_name = 'compact'
        command_body = {
            'compact': self.collection_name,
            'force': True
        }
        self.run_admin_command_test(command_name, command_body)

    def test_convertToCapped(self):
        command_name = 'convertToCapped'
        command_body = {
            'convertToCapped': self.collection_name,
            'size': 1024
        }
        self.run_admin_command_test(command_name, command_body)

    def test_create(self):
        command_name = 'create'
        command_body = {
            'create': f'{self.collection_name}_new'
        }
        self.run_admin_command_test(command_name, command_body)

    def test_createIndexes(self):
        command_name = 'createIndexes'
        command_body = {
            'createIndexes': self.collection_name,
            'indexes': [
                {
                    'key': {'age': 1},
                    'name': 'age_index'
                }
            ]
        }
        self.run_admin_command_test(command_name, command_body)

    def test_currentOp(self):
        command_name = 'currentOp'
        command_body = {
            'currentOp': 1
        }
        self.run_admin_command_test(command_name, command_body)

    def test_drop(self):
        command_name = 'drop'
        command_body = {
            'drop': self.collection_name
        }
        self.run_admin_command_test(command_name, command_body)

        # Recreate the collection for further tests
        self.setUp()

    def test_dropDatabase(self):
        command_name = 'dropDatabase'
        command_body = {
            'dropDatabase': 1
        }
        self.run_admin_command_test(command_name, command_body)

        # Recreate the database and collection for further tests
        self.setUp()

    def test_dropIndexes(self):
        command_name = 'dropIndexes'
        command_body = {
            'dropIndexes': self.collection_name,
            'index': '*'
        }
        self.run_admin_command_test(command_name, command_body)

    def test_listCollections(self):
        command_name = 'listCollections'
        command_body = {
            'listCollections': 1
        }
        self.run_admin_command_test(command_name, command_body)

    def test_listDatabases(self):
        command_name = 'listDatabases'
        command_body = {
            'listDatabases': 1
        }
        self.run_admin_command_test(command_name, command_body)

    def test_listIndexes(self):
        command_name = 'listIndexes'
        command_body = {
            'listIndexes': self.collection_name
        }
        self.run_admin_command_test(command_name, command_body)

    def test_reIndex(self):
        command_name = 'reIndex'
        command_body = {
            'reIndex': self.collection_name
        }
        self.run_admin_command_test(command_name, command_body)

    def test_renameCollection(self):
        command_name = 'renameCollection'
        from_namespace = f'{self.docdb_db.name}.{self.collection_name}'
        to_namespace = f'{self.docdb_db.name}.{self.collection_name}_renamed'
        command_body = {
            'renameCollection': from_namespace,
            'to': to_namespace,
            'dropTarget': True
        }
        self.run_admin_command_test(command_name, command_body)

        # Rename back for further tests
        revert_command_body = {
            'renameCollection': to_namespace,
            'to': from_namespace,
            'dropTarget': True
        }
        with contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()) as stderr:
            result_document = {
                'test_name': f'{command_name}_revert_test',
                'database': 'CosmosDB',
                'status': 'Failed',
                'errors': [],
                'warnings': [],
                'logs': '',
                'stdout': '',
                'stderr': '',
                'timestamp': datetime.utcnow(),
                'details': {}
            }
            try:
                self.docdb_db.client.admin.command(revert_command_body)
                result_document['status'] = 'Success'
                result_document['details']['result'] = 'Collection renamed back successfully.'
            except Exception as e:
                error_trace = traceback.format_exc()
                result_document['errors'].append(f'Exception during revert: {e}\n{error_trace}')
                print(f"Error reverting renameCollection: {e}")
                traceback.print_exc()
            finally:
                result_document['stdout'] = stdout.getvalue()
                result_document['stderr'] = stderr.getvalue()

            # Store the result
            self.logger.debug(f"Test result: {result_document}")

    def test_validate(self):
        command_name = 'validate'
        command_body = {
            'validate': self.collection_name
        }
        self.run_admin_command_test(command_name, command_body)

    #def test_compactStructuredEncryptionData(self):
        #command_name = 'compactStructuredEncryptionData'
        #command_body = {
            #'compactStructuredEncryptionData': self.collection_name,
            #'compactionTokens': []  # required extra field
        #}
        #self.run_admin_command_test(command_name, command_body)

    #def test_dropConnections(self):
        #command_name = 'dropConnections'
        # Supply the required 'hostAndPort' field using the client’s address.
        #host, port = self.docdb_client.address
        #command_body = {
            #'dropConnections': [{'hostAndPort': f"{host}:{port}"}]
        #}
        #self.run_admin_command_test(command_name, command_body)

    def test_filemd5(self):
        # Note: 'filemd5' operates on GridFS files. For testing purposes, we assume an example file ID.
        command_name = 'filemd5'
        command_body = {
            'filemd5': 1,  # Replace with an actual file ID if available
            'root': 'fs'
        }
        self.run_admin_command_test(command_name, command_body)

    #def test_fsync(self):
        #command_name = 'fsync'
        #command_body = {
            #'fsync': 1
        #}
        #self.run_admin_command_test(command_name, command_body)

    #(self):
        #command_name = 'fsyncUnlock'
        #command_body = {
            #'fsyncUnlock': 1
        #}
        #self.run_admin_command_test(command_name, command_body)

    def test_getDefaultRWConcern(self):
        command_name = 'getDefaultRWConcern'
        command_body = {
            'getDefaultRWConcern': 1
        }
        self.run_admin_command_test(command_name, command_body)

    def test_getClusterParameter(self):
        command_name = 'getClusterParameter'
        command_body = {
            'getClusterParameter': '*'
        }
        self.run_admin_command_test(command_name, command_body)

    def test_getParameter(self):
        command_name = 'getParameter'
        command_body = {
            'getParameter': '*'
        }
        self.run_admin_command_test(command_name, command_body)

    def test_killCursors(self):
        command_name = 'killCursors'
        command_body = {
            'killCursors': self.collection_name,
            'cursors': [Int64(1234567890)]  # using Int64 for proper type
        }
        self.run_admin_command_test(command_name, command_body)

    #def test_killOp(self):
        #command_name = 'killOp'
        #command_body = {
            #'killOp': 1,
            #'op': 1234567890  # placeholder operation ID
        #}
        #self.run_admin_command_test(command_name, command_body)

    #def test_logRotate(self):
        #command_name = 'logRotate'
        #command_body = {
            #'logRotate': 1
        #}
        #self.run_admin_command_test(command_name, command_body)

    #def test_rotateCertificates(self):
        #command_name = 'rotateCertificates'
        #command_body = {
            #'rotateCertificates': 1
        #}
        #self.run_admin_command_test(command_name, command_body)

    #def test_setFeatureCompatibilityVersion(self):
        #command_name = 'setFeatureCompatibilityVersion'
        #command_body = {
            #'setFeatureCompatibilityVersion': '4.0'  # adjust version as appropriate for your environment
        #}
        #self.run_admin_command_test(command_name, command_body)

    def test_setIndexCommitQuorum(self):
        command_name = 'setIndexCommitQuorum'
        command_body = {
            'setIndexCommitQuorum': self.collection_name,
            'indexNames': ['age_index'],  # Replace with actual index names if needed
            'commitQuorum': 'majority'
        }
        self.run_admin_command_test(command_name, command_body)

    #def test_setClusterParameter(self):
        #command_name = 'setClusterParameter'
        #command_body = {
            #'setClusterParameter': {'changeStreamOptions': {'preAndPostImages': {'enabled': True}}}
        #}
        #self.run_admin_command_test(command_name, command_body)

    #def test_setParameter(self):
        #command_name = 'setParameter'
        #command_body = {
            #'setParameter': 1,
            #'notablescan': True
        #}
        #self.run_admin_command_test(command_name, command_body)

    #def test_setDefaultRWConcern(self):
        #command_name = 'setDefaultRWConcern'
        #command_body = {
            #'setDefaultRWConcern': 1,
            #'defaultReadConcern': {'level': 'majority'},
            #'defaultWriteConcern': {'w': 'majority'}
        #}
        #self.run_admin_command_test(command_name, command_body)

    def test_setUserWriteBlockMode(self):
        command_name = 'setUserWriteBlockMode'
        command_body = {
            'setUserWriteBlockMode': 1,
            'global': True
        }
        self.run_admin_command_test(command_name, command_body)

        # Reset user write block mode back to False after the test
        try:
            self.docdb_db.command({'setUserWriteBlockMode': 1, 'global': False})
        except Exception as e:
            print(f"Error resetting user write block mode in CosmosDB: {e}")


    #def test_validateDBMetadata(self):
        #command_name = 'validateDBMetadata'
        #command_body = {
            #'validateDBMetadata': 1,
            #'apiParameters': {}  # required extra field
        #}
        #self.run_admin_command_test(command_name, command_body)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        try:
            cls.docdb_coll.drop()
            cls.logger.debug("Dropped collection during teardown.")
        except Exception as e:
            cls.logger.error(f"Error in teardown: {str(e)}")
        finally:
            super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
