#!/bin/bash
#当前路径
path=$(pwd)

ipAddress=http://localhost:8981/solr/

collections=school

feature=ltr_feature_v1

model=ltr_model_v1

featureJson=$path/custom/data/ltr/$feature'.json'

modelJson=$path/custom/data/ltr/$model'.json'

curl -XDELETE --user solr:SolrRocks $ipAddress$collections'/schema/feature-store/'$feature

curl -XDELETE --user solr:SolrRocks $ipAddress$collections'/schema/model-store/'$model

curl -XPUT --user solr:SolrRocks --data-binary @$featureJson -H 'Content-type:application/json' $ipAddress$collections/schema/feature-store

curl -XPUT --user solr:SolrRocks --data-binary @$modelJson -H 'Content-type:application/json' $ipAddress$collections/schema/model-store


#curl -X GET --user solr:SolrRocks  $ipAddress'admin/collections?&action=RELOAD&name='$collections'&wt=json'