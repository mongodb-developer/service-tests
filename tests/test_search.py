# tests/test_postgresql_search.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime
import sys
import os
import decimal  # Importing the decimal module

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest
import config  # Ensure this module has PostgreSQL configurations

class TestPostgreSQLSearchCapabilities(BaseTest):
    """
    Test suite for PostgreSQL search capabilities.
    Translates MongoDB/DocumentDB search tests to PostgreSQL using psycopg2 and pgvector.
    Includes tests for text search, vector search (IVFFlat), and hybrid search.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_postgresql_search'
        cls.logger_name = 'TestPostgreSQLSearchCapabilities'

        # Configure logging
        cls.logger = logging.getLogger(cls.logger_name)
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_postgresql_search.log'
        file_handler = logging.FileHandler('test_postgresql_search.log')
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

        # Create table and enable pgvector extension
        try:
            with cls.test_conn.cursor() as cur:
                # Enable pgvector extension
                cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS vector;"))
                cls.logger.debug("pgvector extension ensured.")

                # Drop table if it exists to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table with appropriate columns, including a vector column
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        _id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        bio TEXT,
                        vectorContent VECTOR(3),  -- Using pgvector's VECTOR type
                        year INTEGER,
                        language TEXT
                    );
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Created table '{cls.table_name}' with necessary fields.")

                # Create GIN index for full-text search on 'bio'
                cur.execute(sql.SQL("""
                    CREATE INDEX IF NOT EXISTS idx_bio_fulltext
                    ON {}
                    USING GIN (to_tsvector('english', bio));
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug("Created GIN index for full-text search on 'bio'.")

                # Create IVFFlat index for vector search on 'vectorContent'
                # Note: Requires pgvector's ivfflat index type
                cur.execute(sql.SQL("""
                    CREATE INDEX IF NOT EXISTS idx_vector_ivfflat
                    ON {}
                    USING ivfflat (vectorContent vector_l2_ops)
                    WITH (lists = 100);
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug("Created IVFFlat index for vector search on 'vectorContent'.")
            cls.test_conn.commit()
            cls.logger.debug(f"Setup for '{cls.table_name}' completed successfully.")
            print(f"Table '{cls.table_name}' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error setting up table: {e}")
            print(f"Error setting up table: {e}")
            raise

    def setUp(self):
        """Reset table before each test"""
        try:
            with self.test_conn.cursor() as cur:
                # Truncate table to remove existing data
                cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(sql.Identifier(self.table_name)))
            self.test_conn.commit()
            self.logger.debug("Truncated table before test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error truncating table in setUp: {e}")
            raise

        # Insert test data
        test_data = self.get_test_data()
        success, error = self.insert_test_data(test_data)
        if not success:
            self.fail(f"Failed to insert test data: {error}")

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    @staticmethod
    def convert_decimals(obj):
        """Recursively convert Decimal objects to float."""
        if isinstance(obj, list):
            return [TestPostgreSQLSearchCapabilities.convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: TestPostgreSQLSearchCapabilities.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if precision is important
        else:
            return obj

    def get_test_data(self):
        """Generate sample data for search tests"""
        return [
            {
                '_id': 'test_1',
                'name': 'Eugenia Lopez',
                'bio': 'Eugenia is the CEO of AdventureWorks.',
                'vectorContent': [0.51, 0.12, 0.23],
                'year': 2001,
                'language': 'en'
            },
            {
                '_id': 'test_2',
                'name': 'Cameron Baker',
                'bio': 'Cameron Baker CFO of AdventureWorks.',
                'vectorContent': [0.55, 0.89, 0.44],
                'year': 2002,
                'language': 'es'
            },
            {
                '_id': 'test_3',
                'name': 'Jessie Irwin',
                'bio': "Jessie Irwin is the former CEO of AdventureWorks and now the director of the Our Planet initiative.",
                'vectorContent': [0.13, 0.92, 0.85],
                'year': 2001,
                'language': 'fr'
            },
            {
                '_id': 'test_4',
                'name': 'Rory Nguyen',
                'bio': "Rory Nguyen is the founder of AdventureWorks and the president of the Our Planet initiative.",
                'vectorContent': [0.91, 0.76, 0.83],
                'year': 2002,
                'language': 'es',
            },
        ]

    def insert_test_data(self, test_data):
        """Inserts test data into the table."""
        try:
            with self.test_conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {} (_id, name, bio, vectorContent, year, language)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (_id) DO NOTHING;
                """).format(sql.Identifier(self.table_name))
                for doc in test_data:
                    # Convert the vector to PostgreSQL's vector format string
                    vector_str = f'[{",".join(map(str, doc["vectorContent"]))}]'
                    cur.execute(insert_query, (
                        doc['_id'],
                        doc['name'],
                        doc['bio'],
                        vector_str,
                        doc['year'],
                        doc['language']
                    ))
            self.test_conn.commit()
            self.logger.debug("Inserted test data successfully.")
            return True, ""
        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Failed to insert test data: {e}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return False, error_msg

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
            'suite': self.table_name,
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

        # Convert all Decimal objects to float or string
        result_document = self.convert_decimals(result_document)

        # Ensure all fields in result_document are JSON serializable
        try:
            json.dumps(result_document)
        except TypeError as te:
            self.logger.error(f"JSON serialization error: {te}")
            result_document['reason'] += f" JSON serialization error: {te}"
            result_document['status'] = 'fail'

        # Print the result_document for debugging
        print(json.dumps(result_document, indent=4))

        # Accumulate result for later storage
        self.test_results.append(result_document)

    def perform_vector_search_test(self, test_name, query_vector, expected_names, index_type='ivfflat'):
        """
        General method to perform vector search tests.
        """
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document(test_name)

        try:
            # Ensure the appropriate vector index exists
            if index_type == 'ivfflat':
                index_name = 'idx_vector_ivfflat'
                # Already created in setUpClass
                self.logger.debug(f"Using existing IVFFlat index '{index_name}'.")
            elif index_type == 'hnsw':
                # If pgvector supports HNSW, create the index
                index_name = 'idx_vector_hnsw'
                with self.test_conn.cursor() as cur:
                    # Example: Creating an HNSW index (Assuming pgvector supports it)
                    # Note: As of pgvector's latest version, HNSW may not be supported.
                    # Uncomment and modify the following lines if HNSW is supported.
                    
                    # cur.execute(sql.SQL("""
                    #     CREATE INDEX IF NOT EXISTS {}
                    #     ON {}
                    #     USING hnsw (vectorContent vector_cosine_ops)
                    #     WITH (m = 16, ef_construction = 200);
                    # """).format(sql.Identifier(index_name), sql.Identifier(self.table_name)))
                    
                    # For demonstration, we'll skip HNSW index creation.
                    raise NotImplementedError("HNSW index type is not supported by pgvector.")
                self.test_conn.commit()
                self.logger.debug(f"Created HNSW index '{index_name}'.")
            else:
                raise ValueError(f"Unsupported index type: {index_type}")

            # Perform vector similarity search
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Using pgvector's similarity function (<-> operator for L2 distance)
                # Convert the query_vector to PostgreSQL's vector format string
                vector_str = f'[{",".join(map(str, query_vector))}]'
                query = sql.SQL("""
                    SELECT name, bio, vectorContent <-> %s AS distance
                    FROM {}
                    ORDER BY distance ASC
                    LIMIT 2;
                """).format(sql.Identifier(self.table_name))
                cur.execute(query, (vector_str,))
                results = cur.fetchall()
            result_document['details'][f'vector_search_results_{index_type}'] = results

            result_names = set(doc['name'] for doc in results)

            if expected_names.issubset(result_names):
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append(f'Vector search ({index_type.upper()}) executed successfully.')
                self.logger.debug(f"Vector search ({index_type.upper()}) executed successfully.")
            else:
                missing = expected_names - result_names
                error_msg = f'Missing expected results: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)

        except NotImplementedError as nie:
            # Handle unsupported index types gracefully
            self.logger.warning(f"{nie}")
            result_document['reason'] = 'FAILED'
            result_document['description'].append(str(nie))
            result_document['status'] = 'fail'
            result_document['log_lines'].append(str(nie))
        except Exception as e:
            error_msg = f"Error during vector search ({index_type.upper()}) test: {e}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_text_search(self):
        """Test full-text search on the 'bio' field"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Text Search Test')

        try:
            # Perform full-text search for 'CEO'
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = sql.SQL("""
                    SELECT name, bio, ts_rank(to_tsvector('english', bio), plainto_tsquery('english', %s)) AS rank
                    FROM {}
                    WHERE to_tsvector('english', bio) @@ plainto_tsquery('english', %s)
                    ORDER BY rank DESC;
                """).format(sql.Identifier(self.table_name))
                cur.execute(query, ('CEO', 'CEO'))
                results = cur.fetchall()
            result_document['details']['text_search_results'] = results

            expected_names = {'Eugenia Lopez', 'Jessie Irwin'}
            result_names = set(doc['name'] for doc in results)

            if expected_names == result_names:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Text search executed successfully.')
                self.logger.debug("Text search executed successfully.")
            else:
                missing = expected_names - result_names
                error_msg = f'Missing expected results: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during text search test: {e}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_vector_search_ivfflat(self):
        """Test vector search using IVFFlat index"""
        query_vector = [0.52, 0.28, 0.12]
        expected_names = {'Eugenia Lopez', 'Rory Nguyen'}
        self.perform_vector_search_test(
            test_name='Vector Search Test - IVFFlat',
            query_vector=query_vector,
            expected_names=expected_names,
            index_type='ivfflat'
        )

    def test_vector_search_hnsw(self):
        """Test vector search using HNSW index"""
        query_vector = [0.52, 0.28, 0.12]
        expected_names = {'Eugenia Lopez', 'Rory Nguyen'}
        self.perform_vector_search_test(
            test_name='Vector Search Test - HNSW',
            query_vector=query_vector,
            expected_names=expected_names,
            index_type='hnsw'
        )

    def test_hybrid_search(self):
        """Test hybrid search combining text and vector search"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Hybrid Search Test')

        try:
            # Define the query vector
            query_vector = [0.52, 0.28, 0.12]
            vector_str = f'[{",".join(map(str, query_vector))}]'

            # Perform hybrid search: full-text search for 'CEO' and vector similarity
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Combining full-text search and vector similarity in WHERE clause
                query = sql.SQL("""
                    SELECT name, bio, vectorContent <-> %s AS distance, ts_rank(to_tsvector('english', bio), plainto_tsquery('english', %s)) AS rank
                    FROM {}
                    WHERE to_tsvector('english', bio) @@ plainto_tsquery('english', %s)
                    ORDER BY rank DESC, distance ASC
                    LIMIT 1;
                """).format(sql.Identifier(self.table_name))
                cur.execute(query, (vector_str, 'CEO', 'CEO'))
                results = cur.fetchall()
            result_document['details']['hybrid_search_results'] = results

            expected_names = {'Eugenia Lopez'}
            result_names = set(doc['name'] for doc in results)

            if expected_names.issubset(result_names):
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Hybrid search executed successfully.')
                self.logger.debug("Hybrid search executed successfully.")
            else:
                missing = expected_names - result_names
                error_msg = f'Missing expected results: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during hybrid search test: {e}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
        finally:
            self.finalize_result_document(result_document, start_time)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
            cls.test_conn.commit()
            cls.logger.debug("Dropped table during teardown.")
            print(f"Table '{cls.table_name}' dropped successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error dropping table during teardown: {e}")
            print(f"Error dropping table during teardown: {e}")
        finally:
            super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
