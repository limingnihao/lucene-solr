#!/bin/bash
#当前路径
path=$(pwd)


$path/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:2181/solr_cloud_dev_school -cmd upconfig -confdir $path/custom/data/config -confname school

$path/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:2181 -cmd putfile "/solr_cloud_dev_school/solr.xml" $path/custom/data/config/solr.xml