#!/usr/bin/env bash
#source config/${1}.cfg
if [[ $1 = "4.0" ]]; then
  branch=v4.0
  mongoshell_package=debian92-4.0.21
  docker build -f Dockerfile-$1 --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .
elif [[ $1 = "4.2" ]]; then
  branch=v4.2
  mongoshell_package=debian10-4.2.10
  docker build -f Dockerfile --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .
elif [[ $1 = "4.4" ]]; then
  branch=v4.4
  mongoshell_package=debian10-4.4.1
  docker build -f Dockerfile --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .
elif [[ $1 = "5.0" ]]; then
  branch=v5.0
  mongoshell_package=debian10-5.0.0
  docker build -f Dockerfile --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .
else
  echo "Please specify either version 4.0, 4.2, 4.4, or 5.0"
fi
