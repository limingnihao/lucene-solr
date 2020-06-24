#!/bin/bash
#当前路径
path=$(pwd)


curl --user solr:SolrRocks -XPUT 'http://localhost:8981/solr/school/schema/table-store' --data-binary "@sample/data/table_store.json" -H 'Content-type:application/json'

