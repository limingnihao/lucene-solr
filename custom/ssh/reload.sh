#!/usr/bin/env bash
curl -X GET --user solr:SolrRocks 'http://localhost:8981/solr/admin/collections?action=RELOAD&name=school&wt=json'
