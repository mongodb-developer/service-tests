#!/usr/bin/env bash
cd /mongo
if [ $1 = "decimal" ]; then
  python ./buildscripts/${command} --mongo=${m} --shellConnString=${URI} --continueOnFailure --reportFile=/results/dbaas_decimal.json --suites=dbaas_decimal
fi
if [ $1 = "json_schema" ]; then
  python ./buildscripts/${command} --mongo=${m} --shellConnString=${URI} --continueOnFailure --reportFile=/results/dbaas_json_schema.json --suites=dbaas_json_schema
fi
if [ $1 = "change_streams" ]; then
  python ./buildscripts/${command} --mongo=${m} --shellConnString=${URI} --continueOnFailure --reportFile=/results/dbaas_change_streams.json --suites=dbaas_change_streams
fi
if [ $1 = "aggregation" ]; then
  python ./buildscripts/${command} --mongo=${m} --shellConnString=${URI} --continueOnFailure --reportFile=/results/dbaas_aggregation.json --suites=dbaas_aggregation
fi
if [ $1 = "core" ]; then
  python ./buildscripts/${command} --mongo=${m} --shellConnString=${URI} --continueOnFailure --reportFile=/results/dbaas_core.json --suites=dbaas_core
fi
if [ $1 = "core_txns" ]; then
  python ./buildscripts/${command} --mongo=${m} --shellConnString=${URI} --continueOnFailure --reportFile=/results/dbaas_core_txns.json --suites=dbaas_core_txns
fi

echo Done!
