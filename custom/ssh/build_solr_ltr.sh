#!/bin/bash
#当前路径
path=$(pwd)

./gradlew clean -p solr/contrib/ltr

./gradlew assemble -p solr/contrib/ltr -x test

build_path=solr/contrib/ltr/build/libs

dist_path=$path/solr/packaging/build/solr-9.0.0-SNAPSHOT/dist

cp -R -v $path/$build_path/ $dist_path

