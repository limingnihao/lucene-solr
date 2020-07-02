#!/bin/bash
if [ -z "$1" ]; then
  echo "project is empty !!!"
  exit 1
fi

path=$(pwd)

project=solr/contrib/$1

build_path=$project/build/libs

dist_path=$path/solr/packaging/build/solr-9.0.0-SNAPSHOT/dist

echo $path $project

./gradlew clean -p $project

./gradlew assemble -p $project -x test

cp -R -v $path/$build_path/ $dist_path

