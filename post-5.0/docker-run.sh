#!/usr/bin/env bash
#TODO - Remove dead branches
if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters"
    echo "Usage : $0 [URI of MongoDB Atlas, AWS Document DB, or Azure Cosmos DB] [Version to test, either 5.0, 5.1, 5.2, or 6.0]"
    exit 1
fi
if [[ $2 != "5.0" ]] && [[ $2 != "5.1" ]] && [[ $2 != "5.2" ]] && [[ $2 != "6.0" ]]; then
    echo "Invalid version; must be 5.0, 5.1, 5.2, or 6.0. Please use the pre-5.0 directory for running older versions."
fi

URI=$1
VERSION=$2
LOCAL_RESULTS_DIR="$(pwd)/results-${VERSION}"
IMAGE="mongo/mongodb-tests:${VERSION}"
rm -rf ${LOCAL_RESULTS_DIR}
mkdir ${LOCAL_RESULTS_DIR}

echo "Starting test suite - Decimal"
docker run --name mongodb-tests-decimal-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} decimal > /dev/null
docker logs mongodb-tests-decimal-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_decimal.log
docker rm -v mongodb-tests-decimal-${VERSION}
echo "Decimal tests complete"

echo "Starting test suite - Core"
docker run --name mongodb-tests-core-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} core > /dev/null
docker logs mongodb-tests-core-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_core.log
docker rm -v mongodb-tests-core-${VERSION}
echo "Core tests complete"

echo "Starting test suite - Transactions"
docker run --name mongodb-tests-core-txns-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} core_txns > /dev/null
docker logs mongodb-tests-core-txns-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_core_txns.log
docker rm -v mongodb-tests-core-txns-${VERSION}
echo "Transactions tests complete"

echo "Starting test suite - JSON Schema"
docker run --name mongodb-tests-json-schema-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} json_schema > /dev/null
docker logs mongodb-tests-json-schema-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_json_schema.log
docker rm -v mongodb-tests-json-schema-${VERSION}
echo "JSON Schema tests complete"

echo "Starting test suite - Change Streams"
docker run --name mongodb-tests-change-streams-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} change_streams > /dev/null
docker logs mongodb-tests-change-streams-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_change_streams.log
docker rm -v mongodb-tests-change-streams-${VERSION}
echo "Change Streams tests complete"

echo "Starting test suite - Aggregation"
docker run --name mongodb-tests-aggregation-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} aggregation > /dev/null
docker logs mongodb-tests-aggregation-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_aggregation.log
docker rm -v mongodb-tests-aggregation-${VERSION}
echo "Aggregation tests complete"

echo "All tests complete"
