#!/usr/bin/env bash
if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters"
    echo "Usage : $0 [URI of MongoDB Atlas, AWS Document DB or Azure Cosmos DB] [Version to test, either 4.0, 4.2 or 4.4]"
    exit 1
fi
if [[ $2 != "4.0" ]] && [[ $2 != "4.2" ]] && [[ $2 != "4.4" ]]; then
    echo "Invalid version; must be 4.0, 4.2 or 4.4"
fi

URI=$1
VERSION=$2
LOCAL_RESULTS_DIR="$(pwd)/results-${VERSION}"
IMAGE="mongo/mongodb-tests:${VERSION}"
rm -rf ${LOCAL_RESULTS_DIR}
mkdir ${LOCAL_RESULTS_DIR}

echo "Starting test suite - Decimal - Execution time 4s to 37s."
docker run --name mongodb-tests-decimal-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} decimal > /dev/null
docker logs mongodb-tests-decimal-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_decimal.log
docker rm -v mongodb-tests-decimal-${VERSION}

echo "Starting test suite - Json Schema - Execution time 24s to 353s."
docker run --name mongodb-tests-json-schema-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} json_schema > /dev/null
docker logs mongodb-tests-json-schema-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_json_schema.log
docker rm -v mongodb-tests-json-schema-${VERSION}

echo "Starting test suite - Change Streams - Execution time 13s to 47s."
docker run --name mongodb-tests-change-streams-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} change_streams > /dev/null
docker logs mongodb-tests-change-streams-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_change_streams.log
docker rm -v mongodb-tests-change-streams-${VERSION}

echo "Starting test suite - Aggregation - Execution time 224s to 3044s."
docker run --name mongodb-tests-aggregation-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} aggregation > /dev/null
docker logs mongodb-tests-aggregation-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_aggregation.log
docker rm -v mongodb-tests-aggregation-${VERSION}

echo "Starting test suite - Core - Execution time 846s to 13043s."
docker run --name mongodb-tests-core-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} core > /dev/null
docker logs mongodb-tests-core-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_core.log
docker rm -v mongodb-tests-core-${VERSION}

echo "Starting test suite - Transactions - Execution time 402s to 1270s."
docker run --name mongodb-tests-core-txns-${VERSION} -e "URI=${URI}" -v ${LOCAL_RESULTS_DIR}:/results ${IMAGE} core_txns > /dev/null
docker logs mongodb-tests-core-txns-${VERSION} > ${LOCAL_RESULTS_DIR}/stdout_core_txns.log
docker rm -v mongodb-tests-core-txns-${VERSION}

