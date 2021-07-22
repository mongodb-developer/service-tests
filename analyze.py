"""
base assumption:
 - docker program was used to run program
"""
import argparse
import fnmatch
import json
import logging.config
import os
import re

import pymongo.errors

# hardcoded right now
logging.config.fileConfig('logging.conf')

# list of suites run by the docker container
SUITES = ['aggregation', 'change_streams', 'core', 'decimal', 'core_txns', 'json_schema']


def parse_args():
    """
    parse arguments from command line

    :return: list of arguments
    """
    parser = argparse.ArgumentParser(description='MongoDB correctness analysis program')
    parser.add_argument(
        '--mdburl',
        type=str,
        action='store',
        required=True,
        help='mongodb uri to store analysis in'
    )

    parser.add_argument(
        '--platform',
        type=str,
        action='store',
        required=True,
        help='platform for results, i.e.: atlas, documentdb, foundationdb, cosmos, etc.'
    )

    parser.add_argument(
        '--drop',
        action='store_true',
        required=False,
        default=False,
        help='drop the existing databsae or keep results'
    )

    parser.add_argument(
        '--version',
        type=str,
        action='store',
        required=False,
        default='v5.0',
        help='version test suite was run against'
    )

    parser.add_argument(
        '--run',
        type=int,
        action='store',
        required=False,
        default=1,
        help='run number, use if we want to compare different run results'
    )

    parser.add_argument(
        '--rdir',
        type=str,
        action='store',
        required=False,
        default='./results-5.0',
        help='directory where results are stored'
    )

    parser.add_argument(
        '--db',
        type=str,
        action='store',
        required=False,
        default='results',
        help='database to store results in'
    )

    parser.add_argument(
        '--coll',
        type=str,
        action='store',
        required=False,
        default='correctness',
        help='collection to store results in'
    )

    parser.add_argument(
        '--csv',
        type=str,
        action='store',
        required=False,
        default='./results.csv',
        help='csv file of processed results'
    )

    parser.add_argument(
        '--csvfilter',
        type=str,
        required=False,
        default='{}',
        help='json filter to apply to csv processing, i.e. "{\"platform\": \"atlas\"}"'
    )
    return parser.parse_args()


def get_tests_list(suite, platform, version, run_no, results_dir):
    """
    simply get a list of the tests enriched with data about the run

    :param suite:
    :param platform:
    :param version:
    :param run_no:
    :param results_dir:
    :return:
    """
    logger.debug('attempt to process json file')
    json_f = '{}/{}'.format(
        results_dir,
        fnmatch.filter(os.listdir(results_dir), '*{}.json'.format(suite))[0]
    )
    with open(json_f, 'r') as f:
        tests = json.load(f)
    for result in tests['results']:
        result['suite'] = suite
        result['platform'] = platform
        result['version'] = version
        result['run'] = run_no
        result['processed'] = False
    return tests['results']


def get_log_lines_as_dict(suite, results_dir):
    """
    all correctness test run output has [CATEGORY:TEST] at the start of the line
    simply read the log file get the output lines for the given test add it to a
    dictionary key. {TEST: [line, line...]}

    :param suite:
    :param results_dir:
    :return:
    """
    logger.debug('attempt to process log lines')

    pattern = re.compile(r'^\[js_test:(.*?)\].*')
    if suite == 'json_schema':
        pattern = re.compile(r'^\[json_schema_test:(.*?)\].*')

    log_f = '{}/{}'.format(
        results_dir,
        fnmatch.filter(os.listdir(results_dir), '*{}.log'.format(suite))[0]
    )
    logger.debug('LOGF {}'.format(log_f))
    # build dictionary where key is test name
    log_lines = {}
    with open(log_f) as log_file:
        test = ''
        for line in log_file:
            match = pattern.match(line)
            if match:
                current_test = match.group(1)
                if current_test != test:
                    logger.debug('old test:{}, current test:{}'.format(test, current_test))
                    test = current_test
                    log_lines[test] = []
                log_lines[test].append(line[:5000].strip())
            else:
                logger.debug('skipping line: {}'.format(line.strip()))
    logger.info('{}:number of test:{}'.format(suite, len(log_lines.keys())))
    return log_lines


def add_logs_lines_to_results(test_list, log_dict):
    """
    for  each of the tests add the logs to the test

    :param test_list:
    :param log_dict:
    :return:
    """
    logger.debug('merging results')
    for test in test_list:
        # covers test file ending in js or json
        key = re.match(r'^.*/(.*)\.js.*', test['test_file'])
        if key and key.group(1) in log_dict:
            test_name = key.group(1)
            test['log_lines'] = log_dict[test_name]
        else:
            logger.warning('key {} not in log'.format(key))


def stage_results(coll, platform, version, run_no, results_dir):
    """
    build results and install into mongodb for further analysis

    :param coll:
    :param platform:
    :param version:
    :param run_no:
    :param results_dir:
    :return:
    """
    logger.debug('attempting to process results for {}'.format(platform))
    suites = None
    bulk_insert = []
    suites = SUITES

    for suite in suites:
        test_results = get_tests_list(suite, platform, version, run_no, results_dir)
        log_lines = get_log_lines_as_dict(suite, results_dir)
        add_logs_lines_to_results(test_results, log_lines)
        bulk_insert.extend(test_results)

    result = coll.insert_many(bulk_insert)
    logger.info('({}) successfully inserted {} documents'.format(result.acknowledged, len(result.inserted_ids)))


def process_cosmodb_failures(coll, regex):
    """
    TODO: though tests were run, the logs and patterns needs to be analyzed

    :param coll:
    :param regex:
    :return:
    """
    logger.warning('not yet implemented for cosmosdb')
    return []


def process_foundationdb_failures(coll, regex):
    """
    TODO: though tests were run, the logs and patterns need to be analyzed
    TODO: looks like it throws lots of exceptions (will need to revisit)

    :param coll:
    :param regex:
    :return:
    """
    logger.warning('not yet implemented for foundationdb')
    return []


def process_documentdb_failures(coll, regex):
    """
    documentdb returns an error message ("errmsg" in testing which leads to unsupported)
    this will be better redefined as it looks like the error code 3xx maps to different types
    of unsupported options (stages, operators, expressions, etc.)
    https://docs.aws.amazon.com/documentdb/latest/developerguide/CommonErrors.html

    :param coll:
    :param regex:
    :return:
    """
    bulkupdate = []
    for doc in coll.aggregate([
        {'$match': {'status': 'fail', 'processed': False}},
        {'$unwind': '$log_lines'},
        {'$match': {'log_lines': {'$regex': re.compile(regex)}}},
        {'$project': {'_id': 1, 'log_lines': 1}},
        {'$group': {'_id': '$_id', 'failure_lines': {'$addToSet': '$log_lines'}}}
    ]):
        desc = doc['failure_lines']
        # desc = desc.replace(',', '').replace('\n', '')
        bulkupdate.append(
            pymongo.UpdateOne(
                {'_id': doc['_id']},
                {'$set': {'processed': True, 'reason': 'UNSUPPORTED', 'description': desc}}
            )
        )
    if len(bulkupdate) > 0:
        results = coll.bulk_write(bulkupdate)
        logger.info('modified {} documents'.format(results.modified_count))
    else:
        logger.info('no errors to process')


def analyze_results(coll):
    """
    analyze all results added new fields:
     - processed
     - reason: PASSED, FURTHER_INVESTIGATION, UNSUPPORTED
     - description: log line of reason for unsupported

    :param coll:
    :return:
    """

    # process pass
    results = coll.update_many({'status': 'pass'}, {'$set': {'processed': True, 'reason': 'PASSED'}})
    logger.info('({}) updated {} documents'.format(results.acknowledged, results.modified_count))

    process_foundationdb_failures(coll, re.compile('\"errmsg\"'))
    process_cosmodb_failures(coll, re.compile('\"errmsg\"'))
    process_documentdb_failures(coll, re.compile('\"errmsg\"'))

    # process the rest of the failures (cannot be categorized)
    bulkupdate = []
    for doc in coll.find({'status': 'fail', 'processed': False}):
        bulkupdate.append(
            pymongo.UpdateOne(
                {'_id': doc['_id']},
                {'$set': {'processed': True, 'reason': 'FURTHER_INVESTIGATION'}})
        )
    if len(bulkupdate) > 0:
        results = coll.bulk_write(bulkupdate)
        logger.info('modified {} documents'.format(results.modified_count))
    else:
        logger.info('no errors to process for this pass')


def summarize_results(results_coll, summary_coll, platform, version, run):
    '''
    Creates a summary of the run as a document in the collection "summary".
    {
    platform: <passed in>
    run: <passed in>
    timestamp: <max timestamp of a test in the run>
    suites: [<array of suites run>]
    passing_tests:
    failing_tests:
    total_tests:
    version: <passed in>
    }
    '''
    summary = results_coll.aggregate([
        {
            '$match': {
                '$and': [
                    {
                        'platform': platform
                    }, {
                        'run': run
                    }, {
                        'version': version
                    }
                ]
            }
        }, {
            '$facet': {
                'suites': [
                    {
                        '$group': {
                            '_id': '$suite',
                            'passing_tests': {
                                '$sum': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                '$status', 'pass'
                                            ]
                                        },
                                        'then': 1,
                                        'else': 0
                                    }
                                }
                            },
                            'failing_tests': {
                                '$sum': {
                                    '$cond': {
                                        'if': {
                                            '$eq': [
                                                '$status', 'fail'
                                            ]
                                        },
                                        'then': 1,
                                        'else': 0
                                    }
                                }
                            },
                            'total_tests': {
                                '$sum': 1
                            }
                        }
                    }
                ],
                'timestamp': [
                    {
                        '$group': {
                            '_id': None,
                            'timestamp': {
                                '$max': '$end'
                            }
                        }
                    }
                ]
            }
        }, {
            '$unwind': {
                'path': '$timestamp'
            }
        }, {
            '$addFields': {
                'timestamp': '$timestamp.timestamp',
                'passing_tests': {
                    '$reduce': {
                        'input': '$suites',
                        'initialValue': '0',
                        'in': {
                            '$sum': [
                                '$$value', '$$this.passing_tests'
                            ]
                        }
                    }
                },
                'failing_tests': {
                    '$reduce': {
                        'input': '$suites',
                        'initialValue': '0',
                        'in': {
                            '$sum': [
                                '$$value', '$$this.failing_tests'
                            ]
                        }
                    }
                },
                'total_tests': {
                    '$reduce': {
                        'input': '$suites',
                        'initialValue': '0',
                        'in': {
                            '$sum': [
                                '$$value', '$$this.failing_tests', '$$this.passing_tests'
                            ]
                        }
                    }
                },
                'version': version,
                'run': run,
                'platform': platform
            }
        }, {
            '$addFields': {
                'failing_percentage': {
                    '$round': [{'$divide': [
                        {'$multiply': [100, '$failing_tests']},
                        '$total_tests'
                    ]}, 2]
                },
                'passing_percentage': {
                    '$round': [{'$divide': [
                        {'$multiply': [100, '$passing_tests']},
                        '$total_tests'
                    ]}, 2]
                },
            }
        }
    ])
    for doc in summary:
        summary_coll.insert_one(doc)


def build_csv(coll, csv_f, csv_filter):
    """

    :param coll:
    :param csv_f:
    :return:
    """
    count = coll.count_documents(csv_filter)
    if count == 0:
        logger.warning('filter is not valid, using empty filter')
        csv_filter = {}
    with open(csv_f, 'w+') as out:
        out.write('test file,suite,platform,version,run no,status,reason,description\n')
        for doc in coll.find(csv_filter):
            tn = doc['test_file']
            s = doc['suite']
            p = doc['platform']
            v = doc['version']
            run = doc['run']
            r = doc['status']
            fr = doc['reason'] if 'reason' in doc else ''
            fd = doc['description'] if 'description' in doc else ''
            clean_fd = '{}'.format(fd).replace('"', '""')
            out.write('{},{},{},{},{},{},{},"{}"\n'.format(tn, s, p, v, run, r, fr, clean_fd))


def main():
    """

    :return:
    """
    logger.debug('starting analysis')
    args = parse_args()

    try:
        client = pymongo.MongoClient(args.mdburl)
        # should we drop the database
        if args.drop:
            logger.info('dropping database first')
            client.drop_database(args.db)

        coll = client.get_database(args.db).get_collection(args.coll)
        stage_results(coll, args.platform, args.version, args.run, args.rdir)
        analyze_results(coll)
        # create the summary doc in the collection 'summary'
        summary_coll = client.get_database(args.db).get_collection('summary')
        summarize_results(coll, summary_coll, args.platform, args.version, args.run)
        build_csv(coll, args.csv, json.loads(args.csvfilter))
        logger.info('finished analysis, csv file created: {}'.format(args.csv))
    except Exception as e:
        # general exception in case connection/inserts/finds/updates fail.
        logger.error('exception occurred during analysis: {}'.format(e), exc_info=True)


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    main()
