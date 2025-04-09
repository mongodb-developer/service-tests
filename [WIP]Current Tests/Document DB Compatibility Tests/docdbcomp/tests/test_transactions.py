# tests/test_transactions.py

import unittest
from datetime import datetime, timezone
from pymongo import WriteConcern
from pymongo.errors import PyMongoError, OperationFailure, ConfigurationError
import logging
import json
import time
from base_test import BaseTest

class TestACIDTransactions(BaseTest):
    """
    Test suite for multi-document ACID transactions.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_acid_transactions'
        cls.transactions_collection_name = 'transactions'

        # DocumentDB collections
        cls.docdb_accounts = cls.docdb_db[cls.collection_name]
        cls.docdb_transactions = cls.docdb_db[cls.transactions_collection_name]

        # Configure logging
        cls.logger = logging.getLogger('TestACIDTransactions')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_transactions.log'
        file_handler = logging.FileHandler('test_transactions.log')
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

        # Clean up previous data
        try:
            cls.docdb_accounts.drop()
            cls.docdb_transactions.drop()
            cls.logger.info('Collections dropped successfully.')
        except Exception as e:
            cls.logger.error(f"Error dropping collections: {e}")

        # Initial accounts data
        cls.initial_accounts = [
            {'name': 'Alice', 'balance': 1000},
            {'name': 'Bob', 'balance': 1000}
        ]

        # Insert initial data with error handling
        try:
            insert_result = cls.docdb_accounts.insert_many(cls.initial_accounts)
            cls.logger.info(f"Inserted documents IDs: {insert_result.inserted_ids}")
            cls.logger.info('Initial accounts data inserted successfully.')

            # Verify insertion
            count = cls.docdb_accounts.count_documents({})
            cls.logger.info(f"Number of documents in '{cls.collection_name}': {count}")
            if count != len(cls.initial_accounts):
                cls.logger.error(f"Expected {len(cls.initial_accounts)} documents, found {count}")
                raise Exception("Initial data insertion verification failed.")
        except Exception as e:
            cls.logger.error(f"Error inserting initial accounts data: {e}")
            raise e  # Re-raise to prevent tests from running without initial data

    def execute_transaction(self, collection, transactions_collection, session, from_account, to_account, amount):
        """Perform a transaction with a debit and credit operation."""
        collection.update_one(
            {'name': from_account, 'balance': {'$gte': amount}},
            {'$inc': {'balance': -amount}},
            session=session
        )
        collection.update_one(
            {'name': to_account},
            {'$inc': {'balance': amount}},
            session=session
        )
        transactions_collection.insert_one(
            {'from': from_account, 'to': to_account, 'amount': amount, 'timestamp': datetime.utcnow()},
            session=session
        )

    def test_transaction_rollback(self):
        """Test transaction rollback on DocumentDB."""
        collection = self.docdb_accounts
        transactions_collection = self.docdb_transactions

        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Transaction Rollback Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.fromtimestamp(start_time, timezone.utc).isoformat(),
            'end': None,
            'suite': 'test_transactions',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        # Initial balance check before transaction
        initial_alice_doc = collection.find_one({'name': 'Alice'})
        initial_bob_doc = collection.find_one({'name': 'Bob'})

        if initial_alice_doc is None or initial_bob_doc is None:
            error_msg = "Document for 'Alice' or 'Bob' not found in collection."
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
            self.fail(error_msg)

        initial_alice = initial_alice_doc['balance']
        initial_bob = initial_bob_doc['balance']

        try:
            with self.docdb_client.start_session() as session:
                with session.start_transaction():
                    # Attempt a valid transaction
                    self.execute_transaction(collection, transactions_collection, session, 'Alice', 'Bob', 100)
                    # Force a failure by updating a non-existent document
                    if not collection.update_one({'name': 'Nonexistent Account'}, {'$inc': {'balance': 100}}, session=session).matched_count:
                        raise Exception("Forced error to test rollback.")  # Explicitly raise an error
        except Exception as e:
            # Expected error should trigger rollback
            result_document['description'].append(f"Expected rollback error: {e}")
            result_document['log_lines'].append(f"Exception occurred: {e}")
            self.logger.error(f"Exception occurred: {e}")
            # Proceed to check balances to verify rollback
        else:
            # If no exception occurs, the test should fail because we expect an exception
            error_msg = "Transaction did not fail as expected."
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
            self.fail(error_msg)
        finally:
            # Verify rollback: Balances should remain the same as initial values
            final_alice_doc = collection.find_one({'name': 'Alice'})
            final_bob_doc = collection.find_one({'name': 'Bob'})

            if final_alice_doc is None or final_bob_doc is None:
                error_msg = "Document for 'Alice' or 'Bob' not found in collection after transaction."
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
                self.fail(error_msg)

            final_alice = final_alice_doc['balance']
            final_bob = final_bob_doc['balance']
            # Record balances in result document
            result_document['details']['initial_balances'] = {'Alice': initial_alice, 'Bob': initial_bob}
            result_document['details']['final_balances'] = {'Alice': final_alice, 'Bob': final_bob}

            if final_alice == initial_alice and final_bob == initial_bob:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Transaction rolled back successfully.')
                self.logger.debug("Transaction rolled back successfully.")
            else:
                error_msg = "Rollback did not restore initial balances."
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
                self.fail(error_msg)

            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.fromtimestamp(end_time, timezone.utc).isoformat()

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

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        # Drop collections after tests
        try:
            cls.docdb_accounts.drop()
            cls.logger.info('docdb_accounts collection dropped successfully.')
        except Exception as e:
            cls.logger.error(f"Error dropping docdb_accounts collection: {e}")

        try:
            cls.docdb_transactions.drop()
            cls.logger.info('docdb_transactions collection dropped successfully.')
        except Exception as e:
            cls.logger.error(f"Error dropping docdb_transactions collection: {e}")

        super().tearDownClass()

    if __name__ == '__main__':
        unittest.main()
