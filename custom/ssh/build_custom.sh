#!/bin/bash
#当前路径
if [ -z "$1" ]; then
  echo "project is empty !!!"
  exit 1   
fi

path=$(pwd)
project=plugin-$1
sample_path=$path/custom/$project/build/libs
runing_path=$path/solr/packaging/build/solr-9.0.0-SNAPSHOT/server/solr-webapp/webapp/WEB-INF/lib

echo $path $project

./gradlew clean -p custom/$project

./gradlew assemble -p custom/$project -x test

#./gradlew copy_jars -p custom/$project -x test

cp -R -v $sample_path/ $runing_path
