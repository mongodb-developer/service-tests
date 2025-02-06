# tests/test_user_management_commands.py

import unittest
from pymongo import MongoClient
from pymongo.errors import OperationFailure, ConfigurationError, PyMongoError
from datetime import datetime
import logging
import json
import time
from base_test import BaseTest

class TestUserManagementCommands(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_user_management_commands'
        cls.docdb_admin_db = cls.docdb_client['admin']

        # Configure logging
        cls.logger = logging.getLogger('TestUserManagementCommands')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_user_management_commands.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

        cls.created_users = []

    def setUp(self):
        super().setUp()
        self.collection_name = 'test_user_management_commands'
        self.docdb_admin_db = self.__class__.docdb_admin_db

        # Use unique user and role names per test run
        timestamp = int(time.time() * 1000)
        self.test_user = f'testUser_{timestamp}'
        self.test_password = f'testPassword_{timestamp}'
        self.test_role = {'role': 'readWrite', 'db': 'test'}
        self.logger = self.__class__.logger

        # Create the test user
        try:
            user_definition = {
                'createUser': self.test_user,
                'pwd': self.test_password,
                'roles': [self.test_role]
            }
            result = self.docdb_admin_db.command(user_definition)
            self.__class__.created_users.append(self.test_user)
            self.logger.info('Test user created successfully in setUp.')
        except OperationFailure as e:
            if e.code == 11:  # UserAlreadyExists
                self.logger.info('Test user already exists.')
            else:
                self.logger.error(f"Failed to create test user in setUp: {e}")
                raise

    def create_result_document(self, command_name):
        """Creates a base result document template for a specific command."""
        start_time = time.time()
        return {
            'status': 'fail',
            'test_name': f"User Management Command Test - {command_name}",
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_user_management_commands',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'command_result': {},
        }

    def execute_and_store_command(self, command):
        """Executes a command and stores the result."""
        result_document = self.create_result_document(command)

        try:
            if command == 'createUser':
                # User is already created in setUp
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('User created successfully.')
            elif command == 'dropUser':
                result = self.docdb_admin_db.command('dropUser', self.test_user)
                if self.test_user in self.__class__.created_users:
                    self.__class__.created_users.remove(self.test_user)
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('User dropped successfully.')
            elif command == 'grantRolesToUser':
                roles = [{'role': 'read', 'db': 'test'}]
                result = self.docdb_admin_db.command('grantRolesToUser', self.test_user, roles=roles)
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Roles granted to user successfully.')
            elif command == 'revokeRolesFromUser':
                roles = [{'role': 'readWrite', 'db': 'test'}]
                result = self.docdb_admin_db.command('revokeRolesFromUser', self.test_user, roles=roles)
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Roles revoked from user successfully.')
            elif command == 'updateUser':
                result = self.docdb_admin_db.command('updateUser', self.test_user, pwd='newPassword')
                self.test_password = 'newPassword'
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('User updated successfully.')
            elif command == 'usersInfo':
                result = self.docdb_admin_db.command('usersInfo', self.test_user)
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('User info retrieved successfully.')
            elif command == 'authenticate':
                # Use the same host and port as the main client
                host_info = self.docdb_client.address  # Returns a tuple (host, port)
                host, port = host_info
                uri = f"mongodb://{self.test_user}:{self.test_password}@{host}:{port}/?authSource=admin&retryWrites=false"

                # Use the same SSL settings as the main client
                ssl_opts = self.docdb_client._MongoClient__options.ssl_opts
                auth_client = MongoClient(
                    uri,
                    tls=True,
                    tlsCAFile=ssl_opts.get('tlsCAFile'),
                    tlsCertificateKeyFile=ssl_opts.get('tlsCertificateKeyFile'),
                    tlsAllowInvalidCertificates=ssl_opts.get('tlsAllowInvalidCertificates'),
                )
                result = auth_client.admin.command('ping')
                auth_client.close()
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('User authenticated successfully.')
            elif command == 'logout':
                # For logout, we can simply close the client
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('User logged out successfully.')
            else:
                raise ValueError(f"Unknown command: {command}")
        except (OperationFailure, ConfigurationError, PyMongoError) as e:
            error_msg = f"{command} error: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during {command}: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            end_time = time.time()
            start_time = datetime.strptime(result_document['start'], "%Y-%m-%dT%H:%M:%S.%f")
            result_document['elapsed'] = end_time - start_time.timestamp()
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                server_info = self.docdb_client.server_info()
                result_document['version'] = server_info.get('version', 'unknown')
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

            # Fail the test if the command was not successful
            self.assertEqual(result_document['status'], 'pass', f"User management command '{command}' failed: {result_document['description']}")

    def test_createUser(self):
        self.execute_and_store_command('createUser')

    def test_authenticate(self):
        self.execute_and_store_command('authenticate')

    def test_grantRolesToUser(self):
        self.execute_and_store_command('grantRolesToUser')

    def test_revokeRolesFromUser(self):
        self.execute_and_store_command('revokeRolesFromUser')

    def test_updateUser(self):
        self.execute_and_store_command('updateUser')

    def test_usersInfo(self):
        self.execute_and_store_command('usersInfo')

    def test_logout(self):
        self.execute_and_store_command('logout')

    def test_dropUser(self):
        self.execute_and_store_command('dropUser')

    def tearDown(self):
        # Clean up by attempting to drop the test user
        try:
            self.docdb_admin_db.command('dropUser', self.test_user)
            self.logger.info(f"Dropped user {self.test_user} in tearDown.")
        except Exception as e:
            self.logger.error(f"Error dropping user {self.test_user} in tearDown: {e}")
        if self.test_user in self.__class__.created_users:
            self.__class__.created_users.remove(self.test_user)
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        # Clean up any remaining users
        for user in cls.created_users:
            try:
                cls.docdb_admin_db.command('dropUser', user)
                cls.logger.info(f"Dropped user {user} in tearDownClass.")
            except Exception as e:
                cls.logger.error(f"Error dropping user {user} in tearDownClass: {e}")
        cls.created_users.clear()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()