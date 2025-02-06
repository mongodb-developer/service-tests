# tests/test_field_level_encryption.py

import unittest
from pymongo import MongoClient, WriteConcern
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.errors import EncryptionError, OperationFailure
from bson.codec_options import CodecOptions
from bson.binary import STANDARD
from datetime import datetime
import traceback
import logging
import json
import time
from base_test import BaseTest
import config

class TestFieldLevelEncryption(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.collection_name = 'test_field_level_encryption'

        # Configure logging
        cls.logger = logging.getLogger('TestFieldLevelEncryption')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_field_level_encryption.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

        cls.docdb_coll = cls.docdb_db[cls.collection_name]
        cls.docdb_coll.drop()

    def test_field_level_encryption(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Field Level Encryption Test',
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

        try:
            # Attempt to use field-level encryption
            try:
                # Setup encryption options (this will likely fail)
                auto_encryption_opts = AutoEncryptionOpts(
                    kms_providers=config.KMS_PROVIDERS,
                    key_vault_namespace=config.KEY_VAULT_NAMESPACE,
                    schema_map={}
                )
                encrypted_client = MongoClient(
                    config.DOCUMENTDB_URI,
                    auto_encryption_opts=auto_encryption_opts
                )
                encrypted_coll = encrypted_client[config.DOCUMENTDB_DB_NAME][self.collection_name]
                encrypted_coll.insert_one({'_id': 1, 'encrypted_field': 'Sensitive Data'})
                result_document['description'].append('Field-level encryption succeeded (unexpected).')
                result_document['reason'] = 'FAILED'
                self.logger.error('Field-level encryption succeeded on DocumentDB, which is unexpected.')
            except Exception as e:
                error_msg = f"Field-level encryption not supported as expected: {str(e)}"
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()