#!/usr/bin/env bash
#source config/${1}.cfg
#linux shell files at https://www.mongodb.org/dl/linux/x86_64
if [[ $1 = "5.0" ]]; then
  branch=v5.0
  mongoshell_package=debian10-5.0.0
  docker build -f Dockerfile --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .
elif [[ $1 = "6.0" ]]; then
  branch=v6.0
  mongoshell_package=debian10-v6.0-latest
  docker build -f Dockerfile --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .  
else
  echo "Please specify either version 5.0 or 6.0. Please use the pre-5.0 directory for running older versions."
fi
