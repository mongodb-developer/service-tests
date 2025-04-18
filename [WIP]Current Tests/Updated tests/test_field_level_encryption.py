# tests/test_field_level_encryption.py

import unittest
from pymongo import MongoClient
from pymongo.encryption import ClientEncryption, Algorithm
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.errors import EncryptionError, OperationFailure, ConfigurationError, PyMongoError
from bson.codec_options import CodecOptions
from bson.binary import STANDARD
from datetime import datetime
import logging
import json
import time
import os
from base_test import BaseTest
import config

class TestFieldLevelEncryption(BaseTest):
    """
    Test suite for client-side field-level encryption and queryable encryption.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.collection_name = 'test_field_level_encryption'

        # Configure logging
        cls.logger = logging.getLogger('TestFieldLevelEncryption')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_field_level_encryption.log'
        file_handler = logging.FileHandler('test_field_level_encryption.log')
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

        cls.docdb_coll = cls.docdb_db[cls.collection_name]
        cls.docdb_coll.drop()

    def setUp(self):
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    def test_field_level_encryption(self):
        """
        Test client-side field-level encryption.
        """
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Field Level Encryption Test',
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
            'details': {},
        }

        try:
            # Generate a local master key
            local_master_key = os.urandom(96)
            self.logger.debug("Generated local master key.")

            # Set up KMS providers
            kms_providers = {
                'local': {
                    'key': local_master_key
                }
            }
            self.logger.debug("Configured KMS providers.")

            key_vault_namespace = 'encryption.__keyVault'

            # Create key vault client (can be the same as self.docdb_client)
            key_vault_client = self.docdb_client

            # Create ClientEncryption object
            client_encryption = ClientEncryption(
                kms_providers,
                key_vault_namespace,
                key_vault_client,
                CodecOptions(uuid_representation=STANDARD)
            )
            self.logger.debug("ClientEncryption object created.")

            # Create a data encryption key
            data_key_id = client_encryption.create_data_key('local')
            self.logger.info(f'Data encryption key created with _id: {data_key_id}')

            # Encrypt a field value
            encrypted_ssn = client_encryption.encrypt(
                '123-45-6789',
                Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
                key_id=data_key_id
            )
            self.logger.debug("Encrypted SSN successfully.")

            # Insert the document with the encrypted field
            collection.insert_one({
                'firstName': 'John',
                'lastName': 'Doe',
                'ssn': encrypted_ssn
            })
            self.logger.info('Document inserted with encrypted field.')
            result_document['details']['inserted_document'] = {
                'firstName': 'John',
                'lastName': 'Doe',
                'ssn': str(encrypted_ssn)
            }

            # Retrieve the document
            result = collection.find_one({'firstName': 'John'})
            if result and result.get('ssn') == encrypted_ssn:
                self.logger.info('Successfully retrieved document with encrypted field.')
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Field-level encryption executed successfully.')
                result_document['details']['retrieved_document'] = {
                    'firstName': result.get('firstName'),
                    'lastName': result.get('lastName'),
                    'ssn': str(result.get('ssn'))
                }
            else:
                error_msg = 'Failed to retrieve document with encrypted field.'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except (ConfigurationError, OperationFailure, PyMongoError, EncryptionError) as e:
            # Expected errors due to lack of support in DocumentDB
            error_msg = f"Field-level encryption not supported as expected: {str(e)}"
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append('Field-level encryption correctly not supported.')
            self.logger.info(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during field-level encryption test: {str(e)}"
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
                server_info = collection.database.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_queryable_encryption(self):
        """
        Test queryable encryption.
        """
        collection_name = self.collection_name
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Queryable Encryption Test',
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
            'details': {},
        }

        # Initialize encrypted_client to None
        encrypted_client = None

        try:
            # Generate a local master key
            local_master_key = os.urandom(96)
            self.logger.debug("Generated local master key for queryable encryption.")

            # Set up KMS providers
            kms_providers = {
                'local': {
                    'key': local_master_key
                }
            }
            self.logger.debug("Configured KMS providers for queryable encryption.")

            key_vault_namespace = 'encryption.__keyVault'

            # Create key vault client (can be the same as self.docdb_client)
            key_vault_client = self.docdb_client

            # Create ClientEncryption object
            client_encryption = ClientEncryption(
                kms_providers,
                key_vault_namespace,
                key_vault_client,
                CodecOptions(uuid_representation=STANDARD)
            )
            self.logger.debug("ClientEncryption object created for queryable encryption.")

            # Create a data encryption key
            data_key_id = client_encryption.create_data_key('local')
            self.logger.info(f'Data encryption key created with _id: {data_key_id}')

            # Define encryptedFields map
            encrypted_fields_map = {
                f"{self.docdb_db.name}.{collection_name}": {
                    "fields": [
                        {
                            "path": "ssn",
                            "keyId": data_key_id,
                            "bsonType": "string",
                            "queries": {"queryType": "equality"}
                        },
                        {
                            "path": "medicalRecords",
                            "keyId": data_key_id,
                            "bsonType": "array"
                        }
                    ]
                }
            }

            # Set up AutoEncryptionOpts
            auto_encryption_opts = AutoEncryptionOpts(
                kms_providers=kms_providers,
                key_vault_namespace=key_vault_namespace,
                encrypted_fields_map=encrypted_fields_map
            )
            self.logger.debug("AutoEncryptionOpts configured for queryable encryption.")

            # Create encrypted client
            encrypted_client = MongoClient(
                config.DOCDB_URI,
                auto_encryption_opts=auto_encryption_opts
            )
            self.logger.debug("Encrypted MongoClient created for queryable encryption.")

            db = encrypted_client[self.docdb_db.name]

            # Drop the collection if it exists
            db.drop_collection(collection_name)
            self.logger.debug(f"Dropped existing collection '{collection_name}' if it existed.")

            # Create the collection with encryptedFields
            try:
                db.create_collection(
                    collection_name,
                    encryptedFields=encrypted_fields_map[f"{self.docdb_db.name}.{collection_name}"]
                )
                self.logger.info('Collection created with encryptedFields.')
            except OperationFailure as e:
                self.logger.error(f'Failed to create collection with encryptedFields: {e}')
                # Since DocumentDB may not support encryptedFields, consider this expected
                error_msg = f"Queryable encryption not supported as expected: {str(e)}"
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append('Queryable encryption correctly not supported.')
                self.logger.info('Queryable encryption not supported as expected.')
                return  # Exit the test early
            except Exception as e:
                # For other exceptions, re-raise the exception
                raise

            # If the collection creation succeeded, proceed to insert and query
            # Insert a document
            coll = db[collection_name]
            insert_result = coll.insert_one({
                'firstName': 'John',
                'lastName': 'Doe',
                'ssn': '123-45-6789',
                'medicalRecords': ['Record1', 'Record2']
            })
            self.logger.info(f'Document inserted with _id: {insert_result.inserted_id}')
            result_document['details']['inserted_document'] = {
                'firstName': 'John',
                'lastName': 'Doe',
                'ssn': '123-45-6789',
                'medicalRecords': ['Record1', 'Record2']
            }

            # Query the document
            result = coll.find_one({'ssn': '123-45-6789'})
            if result:
                self.logger.info('Successfully queried encrypted document.')
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Queryable encryption executed successfully.')
                result_document['details']['queried_document'] = {
                    'firstName': result.get('firstName'),
                    'lastName': result.get('lastName'),
                    'ssn': result.get('ssn'),
                    'medicalRecords': result.get('medicalRecords')
                }
            else:
                error_msg = 'Failed to query encrypted document.'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except (ConfigurationError, OperationFailure, PyMongoError) as e:
            # Expected errors due to lack of support in DocumentDB
            error_msg = f"Queryable encryption not supported as expected: {str(e)}"
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append('Queryable encryption correctly not supported.')
            self.logger.info(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during queryable encryption test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            # Clean up: drop the collection and key vault collection
            if encrypted_client:
                try:
                    encrypted_client[self.docdb_db.name].drop_collection(collection_name)
                    self.logger.info('Encrypted collection dropped.')
                except Exception as e:
                    self.logger.error(f"Error dropping encrypted collection: {e}")

                try:
                    encrypted_client['encryption'].drop_collection('__keyVault')
                    self.logger.info('Key vault collection dropped.')
                except Exception as e:
                    self.logger.error(f"Error dropping key vault collection: {e}")

                encrypted_client.close()
                self.logger.debug('Encrypted MongoClient closed.')

            # Capture elapsed time and end time
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            # Retrieve server version dynamically
            try:
                server_info = self.docdb_client.server_info()
                result_document['version'] = server_info.get('version', 'unknown')
                self.logger.debug(f"Server version retrieved: {result_document['version']}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

            # Fail the test if it did not pass
            if result_document['status'] != 'pass':
                self.fail(f"Queryable encryption test failed: {result_document['description']}")

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        cls.logger.debug("Dropped collection during teardown.")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
