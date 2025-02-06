# tests/test_transactions_postgresql.py

import unittest
import logging
import json
import time
import traceback
from datetime import datetime, timezone, timedelta
import sys
import os
import decimal

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest
import config  # Ensure this module has PostgreSQL configurations


class TestPostgreSQLACIDTransactions(BaseTest):
    """
    Test suite for PostgreSQL multi-document ACID transactions.
    Translates MongoDB/DocumentDB transaction tests to PostgreSQL using psycopg2.
    Includes tests for transaction rollback, insert operations within transactions, etc.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.accounts_table = 'test_postgresql_acid_transactions_accounts'
        cls.transactions_table = 'test_postgresql_acid_transactions_transactions'

        cls.logger_name = 'TestPostgreSQLACIDTransactions'

        # Configure logging
        cls.logger = logging.getLogger(cls.logger_name)
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

        # Create tables for testing
        try:
            with cls.test_conn.cursor() as cur:
                # Drop tables if they exist to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.transactions_table)))
                cls.logger.debug(f"Dropped table '{cls.transactions_table}' if it existed.")

                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.accounts_table)))
                cls.logger.debug(f"Dropped table '{cls.accounts_table}' if it existed.")

                # Create accounts table
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        id SERIAL PRIMARY KEY,
                        name TEXT UNIQUE NOT NULL,
                        balance DOUBLE PRECISION NOT NULL
                    );
                """).format(sql.Identifier(cls.accounts_table)))
                cls.logger.debug(f"Created table '{cls.accounts_table}' with necessary fields.")

                # Create transactions table
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        id SERIAL PRIMARY KEY,
                        from_account TEXT NOT NULL,
                        to_account TEXT NOT NULL,
                        amount DOUBLE PRECISION NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        FOREIGN KEY (from_account) REFERENCES {}(name),
                        FOREIGN KEY (to_account) REFERENCES {}(name)
                    );
                """).format(
                    sql.Identifier(cls.transactions_table),
                    sql.Identifier(cls.accounts_table),
                    sql.Identifier(cls.accounts_table)
                ))
                cls.logger.debug(f"Created table '{cls.transactions_table}' with necessary fields.")

                # Create indexes if necessary
                cur.execute(sql.SQL("""
                    CREATE INDEX idx_accounts_name ON {} (name);
                """).format(sql.Identifier(cls.accounts_table)))
                cls.logger.debug(f"Created index 'idx_accounts_name' on '{cls.accounts_table}'.")

                cur.execute(sql.SQL("""
                    CREATE INDEX idx_transactions_from_account ON {} (from_account);
                """).format(sql.Identifier(cls.transactions_table)))
                cls.logger.debug(f"Created index 'idx_transactions_from_account' on '{cls.transactions_table}'.")

                cur.execute(sql.SQL("""
                    CREATE INDEX idx_transactions_to_account ON {} (to_account);
                """).format(sql.Identifier(cls.transactions_table)))
                cls.logger.debug(f"Created index 'idx_transactions_to_account' on '{cls.transactions_table}'.")

            cls.test_conn.commit()
            cls.logger.debug(f"Setup for '{cls.accounts_table}' and '{cls.transactions_table}' completed successfully.")
            print(f"Tables '{cls.accounts_table}' and '{cls.transactions_table}' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error setting up tables: {e}")
            print(f"Error setting up tables: {e}")
            raise

    def setUp(self):
        """Reset tables before each test"""
        try:
            with self.test_conn.cursor() as cur:
                # Truncate tables to remove existing data
                cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(sql.Identifier(self.accounts_table)))
                cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(sql.Identifier(self.transactions_table)))
            self.test_conn.commit()
            self.logger.debug("Truncated tables before test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error truncating tables in setUp: {e}")
            raise

        # Insert initial data
        initial_accounts = [
            {'name': 'Alice', 'balance': 1000.0},
            {'name': 'Bob', 'balance': 1000.0}
        ]
        try:
            with self.test_conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {} (name, balance)
                    VALUES (%s, %s)
                    ON CONFLICT (name) DO NOTHING;
                """).format(sql.Identifier(self.accounts_table))
                for account in initial_accounts:
                    cur.execute(insert_query, (account['name'], account['balance']))
            self.test_conn.commit()
            self.logger.debug("Inserted initial accounts data successfully.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error inserting initial accounts data: {e}")
            raise

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    @staticmethod
    def convert_objects(obj):
        """
        Recursively convert Decimal and datetime objects to serializable types.
        """
        if isinstance(obj, list):
            return [TestPostgreSQLACIDTransactions.convert_objects(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: TestPostgreSQLACIDTransactions.convert_objects(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if precision is important
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj

    def initialize_result_document(self, test_name):
        start_time = time.time()
        return {
            'status': 'fail',
            'test_name': test_name,
            'platform': 'postgresql',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
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

    def finalize_result_document(self, result_document, start_time):
        end_time = time.time()
        result_document['elapsed'] = end_time - start_time
        result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

        try:
            with self.test_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version_info = cur.fetchone()
            server_version = version_info[0] if version_info else 'unknown'
            result_document['version'] = server_version
            self.logger.debug(f"Server version retrieved: {server_version}")
        except Exception as ve:
            self.logger.error(f"Error retrieving server version: {ve}")
            result_document['version'] = 'unknown'

        # Assign captured log lines to the result document
        result_document['log_lines'] = list(self.log_capture_list)

        # Convert all Decimal and datetime objects to JSON serializable types
        result_document = self.convert_objects(result_document)

        # Ensure all fields in result_document are JSON serializable
        try:
            json.dumps(result_document)
        except TypeError as te:
            self.logger.error(f"JSON serialization error: {te}")
            result_document['description'].append(f"JSON serialization error: {te}")
            result_document['reason'] += f" JSON serialization error: {te}"
            result_document['status'] = 'fail'

        # Print the result_document for debugging
        print(json.dumps(result_document, indent=4))

        # Accumulate result for later storage
        self.test_results.append(result_document)

    def execute_transaction(self, cur, from_account, to_account, amount):
        """
        Perform a debit and credit operation within a transaction using the provided cursor.
        """
        # Debit from_account
        cur.execute(sql.SQL("""
            UPDATE {}
            SET balance = balance - %s
            WHERE name = %s AND balance >= %s;
        """).format(sql.Identifier(self.accounts_table)), (amount, from_account, amount))
        if cur.rowcount == 0:
            raise Exception(f"Insufficient funds in account '{from_account}'.")

        # Credit to_account
        cur.execute(sql.SQL("""
            UPDATE {}
            SET balance = balance + %s
            WHERE name = %s;
        """).format(sql.Identifier(self.accounts_table)), (amount, to_account))
        if cur.rowcount == 0:
            raise Exception(f"Account '{to_account}' does not exist.")

        # Insert transaction record
        cur.execute(sql.SQL("""
            INSERT INTO {} (from_account, to_account, amount, timestamp)
            VALUES (%s, %s, %s, %s);
        """).format(sql.Identifier(self.transactions_table)), (
            from_account,
            to_account,
            amount,
            datetime.utcnow()
        ))

    def test_transaction_rollback(self):
        """Test transaction rollback on PostgreSQL."""
        collection = self.accounts_table
        transactions_collection = self.transactions_table

        start_time = time.time()
        result_document = self.initialize_result_document('Transaction Rollback Test')

        # Initial balance check before transaction
        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql.SQL("SELECT name, balance FROM {} WHERE name IN (%s, %s);").format(
                    sql.Identifier(collection)), ('Alice', 'Bob'))
                initial_docs = cur.fetchall()
            initial_balances = {doc['name']: doc['balance'] for doc in initial_docs}
            result_document['details']['initial_balances'] = initial_balances
            self.logger.debug(f"Initial balances: {initial_balances}")
        except Exception as e:
            error_msg = f"Error retrieving initial balances: {e}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
            self.fail(error_msg)

        try:
            # Begin transaction
            with self.test_conn:
                with self.test_conn.cursor() as cur:
                    # Perform a valid transaction
                    self.execute_transaction(cur, 'Alice', 'Bob', 100)
                    # Attempt a transaction that should fail
                    self.execute_transaction(cur, 'Alice', 'NonExistent', 50)  # This should raise an exception
        except Exception as e:
            # Expected error should trigger rollback
            result_document['description'].append(f"Expected rollback error: {e}")
            result_document['log_lines'].append(f"Exception occurred: {e}")
            self.logger.error(f"Exception occurred: {e}")

        finally:
            # Verify rollback: Balances should remain the same as initial values
            try:
                with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql.SQL("SELECT name, balance FROM {} WHERE name IN (%s, %s);").format(
                        sql.Identifier(collection)), ('Alice', 'Bob'))
                    final_docs = cur.fetchall()
                final_balances = {doc['name']: doc['balance'] for doc in final_docs}
                result_document['details']['final_balances'] = final_balances
                self.logger.debug(f"Final balances: {final_balances}")
            except Exception as ve:
                error_msg = f"Error retrieving final balances: {ve}"
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
                self.fail(error_msg)

            if final_balances == initial_balances:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Transaction rolled back successfully.')
                self.logger.debug("Transaction rolled back successfully.")
            else:
                error_msg = "Rollback did not restore initial balances."
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
                self.fail(error_msg)

            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.fromtimestamp(end_time, timezone.utc).isoformat()

            try:
                with self.test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version_info = cur.fetchone()
                server_version = version_info[0] if version_info else 'unknown'
                result_document['version'] = server_version
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Convert all Decimal and datetime objects to JSON serializable types
            result_document = self.convert_objects(result_document)

            # Ensure all fields in result_document are JSON serializable
            try:
                json.dumps(result_document)
            except TypeError as te:
                self.logger.error(f"JSON serialization error: {te}")
                result_document['description'].append(f"JSON serialization error: {te}")
                result_document['reason'] += f" JSON serialization error: {te}"
                result_document['status'] = 'fail'

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.transactions_table)))
                cls.logger.debug(f"Dropped table '{cls.transactions_table}' successfully.")
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.accounts_table)))
                cls.logger.debug(f"Dropped table '{cls.accounts_table}' successfully.")
            cls.test_conn.commit()
            cls.logger.debug("Teardown completed successfully.")
            print(f"Tables '{cls.accounts_table}' and '{cls.transactions_table}' dropped successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error dropping tables during teardown: {e}")
            print(f"Error dropping tables during teardown: {e}")
        finally:
            super().tearDownClass()


if __name__ == '__main__':
    unittest.main()
