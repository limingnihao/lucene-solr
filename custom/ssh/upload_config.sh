#!/bin/bash
#当前路径
path=$(pwd)

zkpath=$path/solr/packaging/build/solr-9.0.0-SNAPSHOT/server/scripts/cloud-scripts

$zkpath/zkcli.sh -zkhost localhost:2181/solr_cloud_dev_school -cmd upconfig -confdir $path/custom/data/config -confname school

$zkpath/zkcli.sh -zkhost localhost:2181 -cmd putfile "/solr_cloud_dev_school/solr.xml" $path/custom/data/config/solr.xml