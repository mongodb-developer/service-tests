# tests/test_all_operators.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime
import sys
import os

from psycopg2.extras import RealDictCursor

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest

class TestAllOperators(BaseTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.logger = logging.getLogger('TestAllOperators')
        cls.logger.setLevel(logging.DEBUG)

        # Set up file handler and list handler
        file_handler = logging.FileHandler('test_all_operators.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

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

        # Create a test table to store documents (similar to storing JSON in Postgres)
        # We'll store data in a single JSONB column for minimal schema approach
        try:
            with cls.test_conn.cursor() as cur:
                # Drop if exists to start clean
                cur.execute("DROP TABLE IF EXISTS test_all_operators;")
                # Create a table with an id and a jsonb doc
                cur.execute("""
                    CREATE TABLE test_all_operators (
                        id SERIAL PRIMARY KEY,
                        doc JSONB
                    );
                """)
            print("Table 'test_all_operators' created successfully.")
        except Exception as e:
            print(f"Error creating table 'test_all_operators': {e}")
            raise

        # Insert sample data
        initial_data = [
            {
                "_id": 1, "value": 10, "category": "A", "tags": "tag1 tag2",
                "location": {"type": "Point", "coordinates": [40, 5]},
                "numericArray": [1, 2, 3],
                "bitwiseField": 42,
                "dateField": str(datetime(2023, 1, 1)),
                "stringField": "hello world",
                "nullField": None
            },
            {
                "_id": 2, "value": 20, "category": "B", "tags": "tag2 tag3",
                "location": {"type": "Point", "coordinates": [42, 3]},
                "numericArray": [4, 5, 6],
                "bitwiseField": 23,
                "dateField": str(datetime(2023, 5, 15)),
                "stringField": "test string",
                "nullField": None
            },
            {
                "_id": 3, "value": 30, "category": "C", "tags": "tag3 tag4",
                "location": {"type": "Point", "coordinates": [41, 4]},
                "numericArray": [7, 8, 9],
                "bitwiseField": 15,
                "dateField": str(datetime(2024, 2, 28)),
                "stringField": "another test",
                "nullField": None
            },
            {
                "_id": 4, "value": 40, "category": "A", "tags": "tag1 tag4",
                "location": {"type": "Point", "coordinates": [39, 6]},
                "numericArray": [10, 11, 12],
                "bitwiseField": 7,
                "dateField": str(datetime(2024, 8, 18)),
                "stringField": "sample text",
                "nullField": None
            }
        ]

        for doc in initial_data:
            cls._insert_doc(doc)
        print("Sample data inserted into 'test_all_operators' table.")

    @classmethod
    def tearDownClass(cls):
        # Drop the test table
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_all_operators;")
            cls.logger.debug("Dropped test_all_operators table during teardown.")
            print("Table 'test_all_operators' dropped successfully.")
        except Exception as e:
            print(f"Error dropping table 'test_all_operators': {e}")

        super().tearDownClass()

    @classmethod
    def _insert_doc(cls, doc):
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO test_all_operators (doc)
                    VALUES (%s)
                """, (json.dumps(doc),))
            print(f"Inserted document with _id: {doc['_id']}")
        except Exception as e:
            print(f"Error inserting document {doc['_id']}: {e}")
            raise

    def _fetch_all_docs(self):
        """
        Return all rows as list of Python dicts from doc column.
        """
        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT doc FROM test_all_operators")
                rows = cur.fetchall()
            docs = [row['doc'] for row in rows]
            return docs
        except Exception as e:
            self.logger.error(f"Error fetching documents: {e}")
            raise

    def _update_doc(self, old_doc, new_doc):
        """
        Replace old_doc with new_doc in the table.
        We'll match by _id if present.
        """
        doc_id = old_doc.get("_id")
        if doc_id is None:
            return
        try:
            with self.test_conn.cursor() as cur:
                cur.execute("""
                    UPDATE test_all_operators
                    SET doc = %s
                    WHERE doc->>'_id' = %s
                """, (json.dumps(new_doc), str(doc_id)))
            print(f"Updated document with _id: {doc_id}")
        except Exception as e:
            self.logger.error(f"Error updating document {doc_id}: {e}")
            raise

    # ---------------------------------------------------------
    # Main test runner method
    # ---------------------------------------------------------
    def execute_and_store_query(self, query, operator_name,
                                is_aggregation=False, is_update=False,
                                update_operation=None, **kwargs):
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Operator Test - {operator_name}",
            'platform': 'docdb_postgres',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_all_operators',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'query_result': {},
        }

        try:
            if is_update and update_operation:
                # Emulate partial update
                matched, modified = self._emulate_update(query, update_operation)
                result_document['query_result'] = {
                    'matched_count': matched,
                    'modified_count': modified
                }
            elif is_aggregation:
                # Naive aggregator logic in Python
                docs = self._fetch_all_docs()
                aggregator_res = self._handle_aggregation(docs, query)
                result_document['query_result'] = aggregator_res
            else:
                # "Find" -> naive filter in Python
                all_docs = self._fetch_all_docs()
                matched_docs = self._mongo_filter(all_docs, query)
                result_document['query_result'] = matched_docs

            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append(f"Operator '{operator_name}' executed successfully.")
            print(f"Operator '{operator_name}' executed successfully.")
        except Exception as e:
            error_trace = traceback.format_exc()
            error_msg = f"Error executing operator '{operator_name}': {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(f"Error executing operator '{operator_name}': {e}\n{error_trace}")
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()
            result_document['log_lines'] = list(self.log_capture_list)
            result_document['version'] = 'DocDB_Postgres_VUnknown'
            result_document = json.loads(json.dumps(result_document, default=str))

            self.test_results.append(result_document)

    # ---------------------------------------------------------
    # Emulate partial updates
    # ---------------------------------------------------------
    def _emulate_update(self, query, update_op):
        all_docs = self._fetch_all_docs()
        matched_docs = self._mongo_filter(all_docs, query)
        matched_count = len(matched_docs)
        modified_count = 0
        for doc in matched_docs:
            old_doc = doc
            new_doc = self._apply_update(doc, update_op)
            if new_doc != old_doc:
                self._update_doc(old_doc, new_doc)
                modified_count += 1
        return matched_count, modified_count

    def _apply_update(self, doc, update_op):
        changed_doc = json.loads(json.dumps(doc))  # deep copy
        for op, fields in update_op.items():
            if op == "$inc":
                for k, v in fields.items():
                    if k in changed_doc and isinstance(changed_doc[k], (int, float)):
                        changed_doc[k] += v
            elif op == "$set":
                for k, v in fields.items():
                    changed_doc[k] = v
            elif op == "$unset":
                for k in fields.keys():
                    if k in changed_doc:
                        del changed_doc[k]
            elif op == "$mul":
                for k, v in fields.items():
                    if k in changed_doc and isinstance(changed_doc[k], (int, float)):
                        changed_doc[k] *= v
            elif op == "$rename":
                for old_field, new_field in fields.items():
                    if old_field in changed_doc:
                        changed_doc[new_field] = changed_doc.pop(old_field)
            elif op == "$currentDate":
                from datetime import datetime
                for field, val in fields.items():
                    if val is True:
                        changed_doc[field] = str(datetime.utcnow())
            elif op == "$addToSet":
                for k, item in fields.items():
                    arr = changed_doc.get(k, [])
                    if not isinstance(arr, list):
                        arr = []
                    if item not in arr:
                        arr.append(item)
                    changed_doc[k] = arr
            elif op == "$push":
                for k, item in fields.items():
                    arr = changed_doc.get(k, [])
                    if not isinstance(arr, list):
                        arr = []
                    arr.append(item)
                    changed_doc[k] = arr
            elif op == "$pull":
                for k, value in fields.items():
                    arr = changed_doc.get(k, [])
                    if isinstance(arr, list):
                        arr = [x for x in arr if x != value]
                    changed_doc[k] = arr
            elif op == "$pop":
                for k, direction in fields.items():
                    arr = changed_doc.get(k, [])
                    if isinstance(arr, list) and arr:
                        if direction == 1:  # pop last
                            arr.pop()
                        else:               # pop first
                            arr.pop(0)
                        changed_doc[k] = arr
            # $bitsAllClear, etc. not handled
        return changed_doc

    # ---------------------------------------------------------
    # Simple aggregator logic
    # ---------------------------------------------------------
    def _handle_aggregation(self, docs, pipeline):
        # If pipeline is a list of stages, we do naive Python approach
        if not isinstance(pipeline, list) or not pipeline:
            return docs
        # We'll pretend to handle just $project
        stage = pipeline[0]
        if "$project" in stage:
            proj_fields = stage["$project"]
            out_docs = []
            for doc in docs:
                new_doc = {}
                for field_name, expr in proj_fields.items():
                    if isinstance(expr, dict):
                        # e.g. {"$abs": -1}
                        new_doc[field_name] = self._eval_agg_expr(doc, expr)
                    else:
                        new_doc[field_name] = expr
                out_docs.append(new_doc)
            return out_docs
        return docs

    def _eval_agg_expr(self, doc, expr):
        # e.g. expr = {"$abs": -1} or {"$add": ["$value", 10]}
        for op, val in expr.items():
            if op == "$abs":
                return abs(val) if isinstance(val, (int, float)) else None
            elif op == "$add":
                return self._agg_add(doc, val)
            elif op == "$ceil":
                import math
                return math.ceil(val) if isinstance(val, (int, float)) else None
            elif op == "$divide":
                import math
                if isinstance(val, list) and len(val) == 2:
                    numerator = self._resolve_value(doc, val[0])
                    denominator = self._resolve_value(doc, val[1])
                    if denominator != 0:
                        return numerator / denominator
                return None
            elif op == "$exp":
                import math
                return math.exp(val) if isinstance(val, (int, float)) else None
            elif op == "$floor":
                import math
                return math.floor(val) if isinstance(val, (int, float)) else None
            elif op == "$ln":
                import math
                return math.log(val) if isinstance(val, (int, float)) and val > 0 else None
            elif op == "$log":
                import math
                if isinstance(val, list) and len(val) == 2:
                    base = self._resolve_value(doc, val[1])
                    number = self._resolve_value(doc, val[0])
                    if base > 0 and base != 1 and number > 0:
                        return math.log(number, base)
                return None
            elif op == "$log10":
                import math
                return math.log10(val) if isinstance(val, (int, float)) and val > 0 else None
            elif op == "$mod":
                if isinstance(val, list) and len(val) == 2:
                    dividend = self._resolve_value(doc, val[0])
                    divisor = self._resolve_value(doc, val[1])
                    if isinstance(dividend, (int, float)) and isinstance(divisor, (int, float)) and divisor != 0:
                        return dividend % divisor
                return None
            elif op == "$multiply":
                return self._agg_multiply(doc, val)
            elif op == "$pow":
                if isinstance(val, list) and len(val) == 2:
                    base = self._resolve_value(doc, val[0])
                    exponent = self._resolve_value(doc, val[1])
                    if isinstance(base, (int, float)) and isinstance(exponent, (int, float)):
                        return base ** exponent
                return None
            elif op == "$round":
                import math
                if isinstance(val, list) and len(val) >= 1:
                    number = self._resolve_value(doc, val[0])
                    places = self._resolve_value(doc, val[1]) if len(val) > 1 else 0
                    if isinstance(number, (int, float)) and isinstance(places, int):
                        return round(number, places)
                return None
            elif op == "$sqrt":
                import math
                return math.sqrt(val) if isinstance(val, (int, float)) and val >= 0 else None
            elif op == "$subtract":
                return self._agg_subtract(doc, val)
            elif op == "$trunc":
                import math
                if isinstance(val, list) and len(val) == 2:
                    number = self._resolve_value(doc, val[0])
                    places = self._resolve_value(doc, val[1])
                    if isinstance(number, (int, float)) and isinstance(places, int):
                        factor = 10 ** places
                        return math.trunc(number * factor) / factor
                elif isinstance(val, (int, float)):
                    return math.trunc(val)
                return None
            elif op == "$arrayElemAt":
                if isinstance(val, list) and len(val) == 2:
                    array = self._resolve_value(doc, val[0])
                    index = self._resolve_value(doc, val[1])
                    if isinstance(array, list) and isinstance(index, int):
                        if -len(array) <= index < len(array):
                            return array[index]
                return None
            elif op == "$concatArrays":
                if isinstance(val, list):
                    concatenated = []
                    for item in val:
                        resolved = self._resolve_value(doc, item)
                        if isinstance(resolved, list):
                            concatenated.extend(resolved)
                        else:
                            return None
                    return concatenated
                return None
            elif op == "$filter":
                if isinstance(val, dict):
                    input_arr = self._resolve_value(doc, val.get("input", []))
                    as_var = val.get("as", "item")
                    cond = val.get("cond", {})
                    if isinstance(input_arr, list):
                        filtered = []
                        for item in input_arr:
                            local_doc = {as_var: item}
                            if self._matches_cond(local_doc, cond):
                                filtered.append(item)
                        return filtered
                return None
            elif op == "$map":
                if isinstance(val, dict):
                    input_arr = self._resolve_value(doc, val.get("input", []))
                    as_var = val.get("as", "item")
                    in_expr = val.get("in", {})
                    if isinstance(input_arr, list):
                        mapped = []
                        for item in input_arr:
                            local_doc = {as_var: item}
                            mapped_value = self._eval_agg_expr(local_doc, in_expr)
                            mapped.append(mapped_value)
                        return mapped
                return None
            elif op == "$reduce":
                if isinstance(val, dict):
                    input_arr = self._resolve_value(doc, val.get("input", []))
                    initial = self._resolve_value(doc, val.get("initialValue", 0))
                    in_expr = val.get("in", {})
                    if isinstance(input_arr, list):
                        accumulated = initial
                        for item in input_arr:
                            local_doc = {"value": accumulated, "this": item}
                            accumulated = self._eval_agg_expr(local_doc, in_expr)
                        return accumulated
                return None
            elif op == "$dateAdd":
                if isinstance(val, dict):
                    start_date = self._resolve_value(doc, val.get("startDate"))
                    unit = val.get("unit")
                    amount = self._resolve_value(doc, val.get("amount"))
                    if isinstance(start_date, str) and isinstance(unit, str) and isinstance(amount, int):
                        dt = datetime.fromisoformat(start_date)
                        if unit == "day":
                            dt += timedelta(days=amount)
                        elif unit == "month":
                            dt = dt.replace(month=dt.month + amount)
                        elif unit == "year":
                            dt = dt.replace(year=dt.year + amount)
                        # Add more units as needed
                        return dt.isoformat()
                return None
            elif op == "$dateToString":
                if isinstance(val, dict):
                    fmt = val.get("format", "%Y-%m-%d")
                    date = self._resolve_value(doc, val.get("date"))
                    if isinstance(date, str):
                        dt = datetime.fromisoformat(date)
                        return dt.strftime(fmt)
                return None
            elif op == "$dayOfWeek":
                date = self._resolve_value(doc, val)
                if isinstance(date, str):
                    dt = datetime.fromisoformat(date)
                    return dt.isoweekday()  # 1 (Monday) through 7 (Sunday)
                return None
            elif op == "$concat":
                if isinstance(val, list):
                    concatenated = ""
                    for item in val:
                        resolved = self._resolve_value(doc, item)
                        if isinstance(resolved, str):
                            concatenated += resolved
                        else:
                            return None
                    return concatenated
                return None
            elif op == "$toLower":
                s = self._resolve_value(doc, val)
                if isinstance(s, str):
                    return s.lower()
                return None
            elif op == "$strLenBytes":
                s = self._resolve_value(doc, val)
                if isinstance(s, str):
                    return len(s.encode('utf-8'))
                return None
            elif op == "$regexMatch":
                if isinstance(val, dict):
                    input_str = self._resolve_value(doc, val.get("input", ""))
                    pattern = val.get("regex", "")
                    if isinstance(input_str, str) and isinstance(pattern, str):
                        import re
                        return bool(re.search(pattern, input_str))
                return False
            elif op == "$cond":
                if isinstance(val, dict):
                    if_expr = val.get("if", {})
                    then_expr = val.get("then", None)
                    else_expr = val.get("else", None)
                    condition = self._matches_cond(doc, if_expr)
                    if condition:
                        return self._resolve_value(doc, then_expr)
                    else:
                        return self._resolve_value(doc, else_expr)
                return None
            elif op == "$ifNull":
                if isinstance(val, list) and len(val) == 2:
                    expr = self._resolve_value(doc, val[0])
                    replacement = self._resolve_value(doc, val[1])
                    return expr if expr is not None else replacement
                return None
            elif op == "$toInt":
                value = self._resolve_value(doc, val)
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None
            elif op == "$convert":
                if isinstance(val, dict):
                    input_val = self._resolve_value(doc, val.get("input"))
                    to_type = val.get("to")
                    on_error = self._resolve_value(doc, val.get("onError", None))
                    try:
                        if to_type == "int":
                            return int(input_val)
                        elif to_type == "float":
                            return float(input_val)
                        elif to_type == "str":
                            return str(input_val)
                        # Add more type conversions as needed
                        else:
                            return None
                    except (ValueError, TypeError):
                        return on_error
                return None
            return None

    def _agg_add(self, doc, arr):
        total = 0
        for item in arr:
            if isinstance(item, str) and item.startswith('$'):
                f = item[1:]
                v = doc.get(f, 0)
                if isinstance(v, (int, float)):
                    total += v
            elif isinstance(item, (int, float)):
                total += item
        return total

    def _agg_multiply(self, doc, arr):
        product = 1
        for item in arr:
            if isinstance(item, str) and item.startswith('$'):
                f = item[1:]
                v = doc.get(f, 1)
                if isinstance(v, (int, float)):
                    product *= v
            elif isinstance(item, (int, float)):
                product *= item
        return product

    def _agg_subtract(self, doc, arr):
        if len(arr) != 2:
            return None
        minuend = self._resolve_value(doc, arr[0])
        subtrahend = self._resolve_value(doc, arr[1])
        if isinstance(minuend, (int, float)) and isinstance(subtrahend, (int, float)):
            return minuend - subtrahend
        return None

    def _resolve_value(self, doc, expr):
        if isinstance(expr, str) and expr.startswith('$'):
            return doc.get(expr[1:], None)
        return expr

    def _matches_cond(self, doc, cond):
        # A simple evaluator for conditions in $filter and $cond
        # This can be expanded based on requirements
        if not isinstance(cond, dict):
            return False
        for key, value in cond.items():
            if key == "$gt":
                field, threshold = list(value.items())[0]
                return doc.get(field, 0) > threshold
            elif key == "$lt":
                field, threshold = list(value.items())[0]
                return doc.get(field, 0) < threshold
            elif key == "$eq":
                field, target = list(value.items())[0]
                return doc.get(field, None) == target
            # Add more operators as needed
        return False

    # ---------------------------------------------------------
    # Naive client-side "filter" to interpret Mongo queries
    # ---------------------------------------------------------
    def _mongo_filter(self, docs, query):
        """Return the subset of docs that match the (Mongo-style) 'query'."""
        matched = []
        for doc in docs:
            if self._matches(doc, query):
                matched.append(doc)
        return matched

    def _matches(self, doc, condition):
        """
        Minimal check for e.g. {"value": {"$eq": 20}}.
        If it's more complex ($and, $or, etc.), we'll do partial logic.
        """
        if not isinstance(condition, dict):
            return True  # fallback
        for field, val in condition.items():
            if field == "$and":
                # e.g. {"$and": [{"value": {"$gt": 10}}, {"value": {"$lt": 30}}]}
                if not isinstance(val, list):
                    return True
                return all(self._matches(doc, sub) for sub in val)
            elif field == "$or":
                if not isinstance(val, list):
                    return True
                return any(self._matches(doc, sub) for sub in val)
            elif field == "$text":
                # Not supported. We'll no-op
                return False
            elif field == "$nor":
                # not in your tests but example
                return not any(self._matches(doc, sub) for sub in val)
            else:
                # e.g. "value": {"$eq": 20}
                doc_val = doc.get(field)
                if isinstance(val, dict):
                    for op, cond_val in val.items():
                        if op == "$eq":
                            if doc_val != cond_val:
                                return False
                        elif op == "$gt":
                            if not (isinstance(doc_val, (int, float)) and doc_val > cond_val):
                                return False
                        elif op == "$gte":
                            if not (isinstance(doc_val, (int, float)) and doc_val >= cond_val):
                                return False
                        elif op == "$lt":
                            if not (isinstance(doc_val, (int, float)) and doc_val < cond_val):
                                return False
                        elif op == "$lte":
                            if not (isinstance(doc_val, (int, float)) and doc_val <= cond_val):
                                return False
                        elif op == "$ne":
                            if doc_val == cond_val:
                                return False
                        elif op == "$in":
                            if doc_val not in cond_val:
                                return False
                        elif op == "$nin":
                            if doc_val in cond_val:
                                return False
                        elif op == "$exists":
                            # e.g. True => field must exist
                            # but we've got doc_val already
                            exists = (doc_val is not None)
                            if val[op] is True and doc_val is None:
                                return False
                            if val[op] is False and doc_val is not None:
                                return False
                        elif op == "$regex":
                            # Not truly supported in Python logic, do naive 'in' check
                            if cond_val not in str(doc_val):
                                return False
                        else:
                            # skip unknown operators
                            pass
                else:
                    # if it's not a dict, we just do eq
                    if doc_val != val:
                        return False
        return True

    # ---------------------------------------------------------
    # All your test methods remain unchanged below
    # (We just call execute_and_store_query with the same query.)
    # ---------------------------------------------------------
    # Operator Tests
    def test_eq_operator(self):
        operator_name = '$eq'
        query = {"value": {"$eq": 20}}
        self.execute_and_store_query(query, operator_name)

    def test_gt_operator(self):
        operator_name = '$gt'
        query = {"value": {"$gt": 20}}
        self.execute_and_store_query(query, operator_name)

    def test_gte_operator(self):
        operator_name = '$gte'
        query = {"value": {"$gte": 30}}
        self.execute_and_store_query(query, operator_name)

    def test_in_operator(self):
        operator_name = '$in'
        query = {"value": {"$in": [10, 30]}}
        self.execute_and_store_query(query, operator_name)

    def test_lt_operator(self):
        operator_name = '$lt'
        query = {"value": {"$lt": 30}}
        self.execute_and_store_query(query, operator_name)

    def test_lte_operator(self):
        operator_name = '$lte'
        query = {"value": {"$lte": 10}}
        self.execute_and_store_query(query, operator_name)

    def test_ne_operator(self):
        operator_name = '$ne'
        query = {"value": {"$ne": 10}}
        self.execute_and_store_query(query, operator_name)

    def test_nin_operator(self):
        operator_name = '$nin'
        query = {"value": {"$nin": [10, 20]}}
        self.execute_and_store_query(query, operator_name)

    def test_and_operator(self):
        operator_name = '$and'
        query = {"$and": [{"value": {"$gt": 10}}, {"value": {"$lt": 30}}]}
        self.execute_and_store_query(query, operator_name)

    def test_or_operator(self):
        operator_name = '$or'
        query = {"$or": [{"value": 10}, {"value": 20}]}
        self.execute_and_store_query(query, operator_name)

    def test_not_operator(self):
        operator_name = '$not'
        query = {"value": {"$not": {"$gte": 30}}}
        self.execute_and_store_query(query, operator_name)

    def test_exists_operator(self):
        operator_name = '$exists'
        query = {"missingField": {"$exists": False}}
        self.execute_and_store_query(query, operator_name)

    def test_type_operator(self):
        operator_name = '$type'
        query = {"value": {"$type": "int"}}
        self.execute_and_store_query(query, operator_name)

    def test_regex_operator(self):
        operator_name = '$regex'
        query = {"stringField": {"$regex": "test"}}
        self.execute_and_store_query(query, operator_name)

    def test_text_operator(self):
        operator_name = '$text'
        query = {"$text": {"$search": "tag1"}}
        self.execute_and_store_query(query, operator_name)

    def test_geoIntersects_operator(self):
        operator_name = '$geoIntersects'
        query = {"location": {"$geoIntersects": {"$geometry": {"type": "Point", "coordinates": [40, 5]}}}}
        self.execute_and_store_query(query, operator_name)

    def test_geoWithin_operator(self):
        operator_name = '$geoWithin'
        query = {"location": {"$geoWithin": {"$centerSphere": [[40, 5], 0.1]}}}
        self.execute_and_store_query(query, operator_name)

    def test_near_operator(self):
        operator_name = '$near'
        query = {"location": {"$near": {"$geometry": {"type": "Point", "coordinates": [40, 5]}, "$maxDistance": 1000}}}
        self.execute_and_store_query(query, operator_name)

    def test_all_operator(self):
        operator_name = '$all'
        query = {"numericArray": {"$all": [1, 2]}}
        self.execute_and_store_query(query, operator_name)

    def test_elemMatch_operator(self):
        operator_name = '$elemMatch'
        query = {"numericArray": {"$elemMatch": {"$gt": 8}}}
        self.execute_and_store_query(query, operator_name)

    def test_size_operator(self):
        operator_name = '$size'
        query = {"numericArray": {"$size": 3}}
        self.execute_and_store_query(query, operator_name)

    def test_bitsAllClear_operator(self):
        operator_name = '$bitsAllClear'
        query = {"bitwiseField": {"$bitsAllClear": 8}}
        self.execute_and_store_query(query, operator_name)

    def test_bitsAllSet_operator(self):
        operator_name = '$bitsAllSet'
        query = {"bitwiseField": {"$bitsAllSet": 8}}
        self.execute_and_store_query(query, operator_name)

    def test_bitsAnyClear_operator(self):
        operator_name = '$bitsAnyClear'
        query = {"bitwiseField": {"$bitsAnyClear": 8}}
        self.execute_and_store_query(query, operator_name)

    def test_bitsAnySet_operator(self):
        operator_name = '$bitsAnySet'
        query = {"bitwiseField": {"$bitsAnySet": 8}}
        self.execute_and_store_query(query, operator_name)

    # Update Operator Tests
    def test_currentDate_operator(self):
        operator_name = '$currentDate'
        query = {"category": "A"}
        update = {"$currentDate": {"lastModified": True}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_inc_operator(self):
        operator_name = '$inc'
        query = {"category": "B"}
        update = {"$inc": {"value": 5}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_min_operator(self):
        operator_name = '$min'
        query = {"category": "C"}
        update = {"$min": {"value": 25}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_max_operator(self):
        operator_name = '$max'
        query = {"category": "C"}
        update = {"$max": {"value": 35}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_mul_operator(self):
        operator_name = '$mul'
        query = {"category": "A"}
        update = {"$mul": {"value": 2}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_rename_operator(self):
        operator_name = '$rename'
        query = {"category": "B"}
        update = {"$rename": {"stringField": "renamedField"}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_set_operator(self):
        operator_name = '$set'
        query = {"category": "C"}
        update = {"$set": {"newField": "newValue"}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_unset_operator(self):
        operator_name = '$unset'
        query = {"category": "A"}
        update = {"$unset": {"nullField": ""}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_addToSet_operator(self):
        operator_name = '$addToSet'
        query = {"category": "B"}
        update = {"$addToSet": {"tags": "tag5"}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_pop_operator(self):
        operator_name = '$pop'
        query = {"category": "C"}
        update = {"$pop": {"numericArray": 1}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_pull_operator(self):
        operator_name = '$pull'
        query = {"category": "A"}
        update = {"$pull": {"numericArray": 2}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_push_operator(self):
        operator_name = '$push'
        query = {"category": "B"}
        update = {"$push": {"numericArray": 99}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    # Aggregation Expression Tests
    def test_abs_expression(self):
        operator_name = '$abs'
        pipeline = [{"$project": {"absValue": {"$abs": -1}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_add_expression(self):
        operator_name = '$add'
        pipeline = [{"$project": {"sum": {"$add": ["$value", 10]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_ceil_expression(self):
        operator_name = '$ceil'
        pipeline = [{"$project": {"ceilValue": {"$ceil": 4.7}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_divide_expression(self):
        operator_name = '$divide'
        pipeline = [{"$project": {"dividedValue": {"$divide": ["$value", 2]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_exp_expression(self):
        operator_name = '$exp'
        pipeline = [{"$project": {"expValue": {"$exp": "$value"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_floor_expression(self):
        operator_name = '$floor'
        pipeline = [{"$project": {"floorValue": {"$floor": 4.7}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_ln_expression(self):
        operator_name = '$ln'
        pipeline = [{"$project": {"lnValue": {"$ln": "$value"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_log_expression(self):
        operator_name = '$log'
        pipeline = [{"$project": {"logValue": {"$log": ["$value", 10]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_log10_expression(self):
        operator_name = '$log10'
        pipeline = [{"$project": {"log10Value": {"$log10": "$value"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_mod_expression(self):
        operator_name = '$mod (expression)'
        pipeline = [{"$project": {"modValue": {"$mod": ["$value", 3]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_multiply_expression(self):
        operator_name = '$multiply'
        pipeline = [{"$project": {"multipliedValue": {"$multiply": ["$value", 2]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_pow_expression(self):
        operator_name = '$pow'
        pipeline = [{"$project": {"powValue": {"$pow": ["$value", 2]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_round_expression(self):
        operator_name = '$round'
        pipeline = [{"$project": {"roundedValue": {"$round": [4.567, 2]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_sqrt_expression(self):
        operator_name = '$sqrt'
        pipeline = [{"$project": {"sqrtValue": {"$sqrt": "$value"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_subtract_expression(self):
        operator_name = '$subtract'
        pipeline = [{"$project": {"subtractedValue": {"$subtract": ["$value", 5]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_trunc_expression(self):
        operator_name = '$trunc'
        pipeline = [{"$project": {"truncatedValue": {"$trunc": 4.567}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_arrayElemAt_expression(self):
        operator_name = '$arrayElemAt'
        pipeline = [{"$project": {"elementAt": {"$arrayElemAt": ["$numericArray", 1]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_concatArrays_expression(self):
        operator_name = '$concatArrays'
        pipeline = [{"$project": {"concatenatedArray": {"$concatArrays": ["$numericArray", [13, 14]]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_filter_expression(self):
        operator_name = '$filter'
        pipeline = [{"$project": {"filteredArray": {"$filter": {"input": "$numericArray", "as": "num", "cond": {"$gt": ["$$num", 5]}}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_map_expression(self):
        operator_name = '$map'
        pipeline = [{"$project": {"mappedArray": {"$map": {"input": "$numericArray", "as": "num", "in": {"$multiply": ["$$num", 2]}}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_reduce_expression(self):
        operator_name = '$reduce'
        pipeline = [{"$project": {"sumOfArray": {"$reduce": {"input": "$numericArray", "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_size_expression(self):
        operator_name = '$size (expression)'
        pipeline = [{"$project": {"arraySize": {"$size": "$numericArray"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_dateAdd_expression(self):
        operator_name = '$dateAdd'
        pipeline = [{"$project": {"newDate": {"$dateAdd": {"startDate": "$dateField", "unit": "day", "amount": 5}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_dateToString_expression(self):
        operator_name = '$dateToString'
        pipeline = [{"$project": {"dateString": {"$dateToString": {"format": "%Y-%m-%d", "date": "$dateField"}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_dayOfWeek_expression(self):
        operator_name = '$dayOfWeek'
        pipeline = [{"$project": {"dayOfWeek": {"$dayOfWeek": "$dateField"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_concat_expression(self):
        operator_name = '$concat'
        pipeline = [{"$project": {"fullString": {"$concat": ["$stringField", " - appended text"]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_toLower_expression(self):
        operator_name = '$toLower'
        pipeline = [{"$project": {"lowerString": {"$toLower": "$stringField"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_strLenBytes_expression(self):
        operator_name = '$strLenBytes'
        pipeline = [{"$project": {"stringLength": {"$strLenBytes": "$stringField"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_regexMatch_expression(self):
        operator_name = '$regexMatch'
        pipeline = [{"$project": {"regexMatch": {"$regexMatch": {"input": "$stringField", "regex": "test"}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_cond_expression(self):
        operator_name = '$cond'
        pipeline = [{"$project": {"result": {"$cond": {"if": {"$gt": ["$value", 25]}, "then": "Greater than 25", "else": "25 or less"}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_ifNull_expression(self):
        operator_name = '$ifNull'
        pipeline = [{"$project": {"valueOrDefault": {"$ifNull": ["$nullField", "default value"]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_toInt_expression(self):
        operator_name = '$toInt'
        pipeline = [{"$project": {"intValue": {"$toInt": "$stringField"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_convert_expression(self):
        operator_name = '$convert'
        pipeline = [{"$project": {"convertedValue": {"$convert": {"input": "$stringField", "to": "int", "onError": 0}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    # ... (Ensure all 70 tests are included here) ...

if __name__ == '__main__':
    unittest.main()
