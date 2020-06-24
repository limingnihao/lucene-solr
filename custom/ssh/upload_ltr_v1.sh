#!/bin/bash
#当前路径
path=$(pwd)

ipAddress=http://localhost:8981/solr/

collections=school

feature=ltr_feature_v1

model=ltr_model_v1

featureJson=$path/custom/data/ltr/$feature'.json'

modelJson=$path/custom/data/ltr/$model'.json'

echo $ipAddress', '$collections', '$featureJson', '$modelJson


curl -XDELETE  $ipAddress$collections'/schema/feature-store/'$feature

curl -XDELETE $ipAddress$collections'/schema/model-store/'$model

curl -XPUT --data-binary @$featureJson -H 'Content-type:application/json' $ipAddress$collections/schema/feature-store

curl -XPUT --data-binary @$modelJson -H 'Content-type:application/json' $ipAddress$collections/schema/model-store


#curl -X GET --user solr:SolrRocks  $ipAddress'admin/collections?&action=RELOAD&name='$collections'&wt=json'