#!/bin/bash
#当前路径
path=$(pwd)
echo $path
# solr
echo start build solr ltr ....
./gradlew build -p solr/contrib/ltr -x test

solr_core_jar=solr-ltr-9.0.0-SNAPSHOT.jar
solr_core_path=solr/contrib/ltr/build/libs
echo $solr_core_jar
echo $solr_core_path
stat $path/$solr_core_path/$solr_core_jar
cp $path/$solr_core_path/$solr_core_jar $path/solr/server/solr-webapp/webapp/WEB-INF/lib
echo solr ltr ok