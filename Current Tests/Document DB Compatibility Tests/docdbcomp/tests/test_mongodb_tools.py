# tests/test_mongodb_tools.py

import unittest
from datetime import datetime
import traceback
import subprocess
import os
import logging
import json
import time
from base_test import BaseTest
import config

class TestMongoDBTools(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_mongodb_tools'

        cls.docdb_coll = cls.docdb_db[cls.collection_name]
        cls.docdb_coll.drop()

        cls.logger = logging.getLogger('TestMongoDBTools')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_mongodb_tools.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

        cls.dump_directory = 'dump_test_mongodb_tools'
        if os.path.exists(cls.dump_directory):
            subprocess.call(['rm', '-rf', cls.dump_directory])

    def setUp(self):
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger
        self.dump_directory = self.__class__.dump_directory

    def test_mongodb_tools(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'MongoDB Tools Test',
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
            data = [{'_id': i, 'value': f'data{i}'} for i in range(10)]
            collection.insert_many(data)
            result_document['log_lines'].append('Data inserted successfully.')

            try:
                dump_cmd = [
                    config.MONGODUMP_PATH,
                    '--uri', config.DOCUMENTDB_URI,
                    '--collection', self.collection_name,
                    '--db', config.DOCUMENTDB_DB_NAME,
                    '--out', self.dump_directory,
                    '--ssl',
                    '--sslCAFile', config.DOCUMENTDB_SSL_CA_FILE
                ]
                dump_result = subprocess.run(dump_cmd, capture_output=True, text=True)
                if dump_result.returncode != 0:
                    error_msg = f"mongodump failed: {dump_result.stderr}"
                    result_document['description'].append(error_msg)
                    result_document['reason'] = 'FAILED'
                    self.logger.error(error_msg)
                else:
                    result_document['log_lines'].append('mongodump executed successfully.')
            except Exception as e:
                error_msg = f"Error running mongodump: {str(e)}"
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)

            try:
                restore_cmd = [
                    config.MONGORESTORE_PATH,
                    '--uri', config.DOCUMENTDB_URI,
                    '--nsInclude', f'{config.DOCUMENTDB_DB_NAME}.{self.collection_name}',
                    self.dump_directory,
                    '--ssl',
                    '--sslCAFile', config.DOCUMENTDB_SSL_CA_FILE
                ]
                restore_result = subprocess.run(restore_cmd, capture_output=True, text=True)
                if restore_result.returncode != 0:
                    error_msg = f"mongorestore failed: {restore_result.stderr}"
                    result_document['description'].append(error_msg)
                    result_document['reason'] = 'FAILED'
                    self.logger.error(error_msg)
                else:
                    result_document['log_lines'].append('mongorestore executed successfully.')
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
            except Exception as e:
                error_msg = f"Error running mongorestore: {str(e)}"
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during MongoDB Tools test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            if os.path.exists(self.dump_directory):
                subprocess.call(['rm', '-rf', self.dump_directory])

            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                server_info = collection.database.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
