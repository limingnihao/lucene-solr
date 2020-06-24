curl -XDELETE  --user solr:SolrRocks 'http://localhost:8981/solr/school/schema/feature-store/custom_feature'

curl -XPUT  --user solr:SolrRocks 'http://localhost:8981/solr/school/schema/feature-store' --data-binary "@sample/data/ltr_custom_feature.json" -H 'Content-type:application/json'

curl -XDELETE  --user solr:SolrRocks 'http://localhost:8981/solr/school/schema/model-store/custom_model'

curl -XPUT  --user solr:SolrRocks 'http://localhost:8981/solr/school/schema/model-store' --data-binary "@sample/data/ltr_custom_model.json" -H 'Content-type:application/json'


