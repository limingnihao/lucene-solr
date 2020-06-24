#!/bin/bash
#当前路径
path=$(pwd)
echo $path

# lucene
echo start build lucene core ....
./gradlew build -p lucene/core -x test


lucene_core_jar=lucene-core-9.0.0-SNAPSHOT.jar
lucene_core_path=lucene/core/build/libs
echo $lucene_core_jar
echo $lucene_core_path
stat $path/$lucene_core_path/$lucene_core_jar
cp $path/$lucene_core_path/$lucene_core_jar $path/solr/server/solr-webapp/webapp/WEB-INF/lib
echo lucene core ok
