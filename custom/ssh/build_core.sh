#!/bin/bash
if [ -z "$1" ]; then
  echo "project is empty (solr or lucene) !!!"
  exit 1
fi

path=$(pwd)

project=$1/core

build_path=$project/build/libs

#dist_path=$path/solr/packaging/build/solr-9.0.0-SNAPSHOT/server/solr-webapp/webapp/WEB-INF/lib

dist_path=$path/solr/server/solr-webapp/webapp/WEB-INF/lib

echo $path $project

./gradlew clean -p $project

./gradlew assemble -p $project -x test

cp -R -v $path/$build_path/ $dist_path

