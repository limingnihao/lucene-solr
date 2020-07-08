#!/usr/bin/env bash

path=$(pwd)

homePath=$path/custom/temp/n2

#solrPath=solr/packaging/build/solr-9.0.0-SNAPSHOT
solrPath=solr

zkhost=localhost:2181,localhost:2182,localhost:2183/solr_cloud_dev_school

port=8982
portDebug=5002

mkdir $homePath

$solrPath/bin/solr start -c -f \
        -s $homePath \
        -z $zkhost \
        -p $port \
        -m 256m \
        -a -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$portDebug \
        -Dsolr.solrxml.location=zookeeper \
        -Dsolr.log.dir=$homePath \
        -Dsolr.ltr.enabled=true
