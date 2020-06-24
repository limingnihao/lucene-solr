
path=$(pwd)

home=$path/custom/temp/n2

zkhost=localhost:2181/solr_cloud_dev_school

port=8982

portDebug=5002

mkdir $home

solr/bin/solr start -c -f \
        -s $home \
        -z $zkhost \
        -p $port \
        -m 1g \
        -a -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$portDebug \
        -Dsolr.solrxml.location=zookeeper \
        -Dsolr.log.dir=$home
