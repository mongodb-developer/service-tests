#!/usr/bin/env bash
#source config/${1}.cfg
#linux shell files at https://www.mongodb.org/dl/linux/x86_64?_ga=2.189064104.222886187.1543460064-1222638156.1538017689
if [[ $1 = "5.0" ]]; then
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
elif [[ $1 = "6.0" ]]; then
  branch=v6.0
  mongoshell_package=debian11-v6.0-latest
  docker build -f Dockerfile --build-arg version=$1 --build-arg branch=$branch --build-arg mongoshell_package=$mongoshell_package -t mongo/mongodb-tests:$1 .  
else
  echo "Please specify either version 5.0, 5.1, 5.2, or 6.0. Please use the pre-5.0 directory for running older versions."
fi
