#!/usr/bin/env bash
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



echo "All tests complete"
