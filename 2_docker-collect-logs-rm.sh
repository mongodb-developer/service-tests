#!/usr/bin/env bash
docker logs mongodb-tests-decimal > results/stdout_decimal.log
docker logs mongodb-tests-json-schema > results/stdout_json_schema.log
docker logs mongodb-tests-change-streams > results/stdout_change_streams.log
docker logs mongodb-tests-aggregation > results/stdout_aggregation.log
docker logs mongodb-tests-core > results/stdout_core.log
docker rm -v mongodb-tests-decimal
docker rm -v mongodb-tests-json-schema
docker rm -v mongodb-tests-change-streams
docker rm -v mongodb-tests-aggregation
docker rm -v mongodb-tests-core

