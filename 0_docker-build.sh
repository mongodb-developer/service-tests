#!/usr/bin/env bash
if [[ $1 = "3.6" ]]; then
  docker build -f Dockerfile-3.6 -t mongo/mongodb-tests:3.6 .
elif [[ $1 = "4.2" ]]; then
  docker build -f Dockerfile-4.2 -t mongo/mongodb-tests:4.2 .
else
  echo "Please specify either version 3.6 or 4.2"
fi
