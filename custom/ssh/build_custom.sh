#!/bin/bash
#当前路径
path=$(pwd)
echo $path

# ./gradlew assemble -p custom/plugin-ltr -x test
./gradlew assemble -p custom/plugin-rerank -x test

./gradlew copy_jars -p custom/plugin-rerank -x test


#sample_jar=sample-custom-9.0.0-SNAPSHOT.jar
#sample_path=sample/custom/build/libs
#echo $sample_jar
#echo $sample_path
#stat $path/$sample_path/$sample_jar
#cp -R -v $path/$sample_path/ $path/solr/server/solr-webapp/webapp/WEB-INF/lib
#echo copy ok: $path/$sample_path/ TO $path/solr/server/solr-webapp/webapp/WEB-INF/lib
