#!/usr/bin/env bash
if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
    echo "Usage : $0 [URI of MongoDB Atlas, AWS Document DB or Azure Cosmos DB]"
    exit 1
fi

URI=$1
rm -rf results
mkdir results
echo "Starting test suite - 1/5 - Decimal - Execution time 4s to 37s."
docker run --name mongodb-tests-decimal -e "URI=${URI}" -v $(pwd)/results:/results mongo/mongodb-tests:3.6 decimal > /dev/null
echo "Starting test suite - 2/5 - Json Schema - Execution time 24s to 353s."
docker run --name mongodb-tests-json-schema -e "URI=${URI}" -v $(pwd)/results:/results mongo/mongodb-tests:3.6 json_schema > /dev/null
echo "Starting test suite - 3/5 - Change Streams - Execution time 13s to 47s."
docker run --name mongodb-tests-change-streams -e "URI=${URI}" -v $(pwd)/results:/results mongo/mongodb-tests:3.6 change_streams > /dev/null
echo "Starting test suite - 4/5 - Aggregation - Execution time 224s to 3044s."
docker run --name mongodb-tests-aggregation -e "URI=${URI}" -v $(pwd)/results:/results mongo/mongodb-tests:3.6 aggregation > /dev/null
echo "Starting test suite - 5/5 - Core - Execution time 846s to 13043s."
docker run --name mongodb-tests-core -e "URI=${URI}" -v $(pwd)/results:/results mongo/mongodb-tests:3.6 core > /dev/null

