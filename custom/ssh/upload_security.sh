#!/bin/bash
#当前路径
path=$(pwd)

fileName=$path/custom/ssh/security.json

solr/packaging/build/solr-9.0.0-SNAPSHOT/bin/solr zk cp file:$fileName zk:solr_cloud_dev_school/security.json -z localhost:2181
