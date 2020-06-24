curl --user solr:SolrRocks -XGET  http://localhost:8981/solr/school2_shard2_replica_n2/replication?command=fetchindex&masterUrl=http://localhost:8981/solr/school_shard2_replica_n2

curl --user solr:SolrRocks -XGET  http://localhost:8982/solr/school2_shard1_replica_n1/replication?command=fetchindex&masterUrl=http://localhost:8982/solr/school_shard1_replica_n1


