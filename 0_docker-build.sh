#!/usr/bin/env bash
#source config/${1}.cfg
#linux shell files at https://www.mongodb.org/dl/linux/x86_64?_ga=2.189064104.222886187.1543460064-1222638156.1538017689
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
elif [[ $1 = "5.1" ]]; then
  branch=v5.1
  mongoshell_package=debian10-5.1.1-rc0
  docker build -f Dockerfile --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .  
elif [[ $1 = "5.2" ]]; then
  branch=v5.2
  mongoshell_package=debian10-5.2.0-rc6
  docker build -f Dockerfile --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .  
else
  echo "Please specify either version 4.0, 4.2, 4.4, 5.0, 5.1, or 5.2"
fi
