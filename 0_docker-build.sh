#!/usr/bin/env bash
#source config/${1}.cfg
if [[ $1 = "3.6" ]]; then
  branch=r3.6.18
  mongoshell_package=debian92-3.6.18
  docker build -f Dockerfile-$1 --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .
elif [[ $1 = "4.2" ]]; then
  branch=r4.2.6
  mongoshell_package=debian10-4.2.6
  docker build -f Dockerfile-$1 --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .
else
  echo "Please specify either version 3.6 or 4.2"
fi
