#!/usr/bin/env bash

path=$(pwd)

home=$path/custom/temp/n1

zkhost=localhost:2181,localhost:2182,localhost:2183/solr_cloud_dev_school

port=8981

portDebug=5001

mkdir $home

solr/packaging/build/solr-9.0.0-SNAPSHOT/bin/solr start -c -f \
        -s $home \
        -z $zkhost \
        -p $port \
        -m 256m \
        -a -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$portDebug \
        -Dsolr.solrxml.location=zookeeper \
        -Dsolr.log.dir=$home \
        -Dsolr.ltr.enabled=true
