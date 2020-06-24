
path=$(pwd)

home=$path/custom/temp/n1

zkhost=localhost:2181/solr_cloud_dev_school

port=8981

portDebug=5001

mkdir $home

solr/bin/solr start -c -f \
        -s $home \
        -z $zkhost \
        -p $port \
        -m 1g \
        -a -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$portDebug \
        -Dsolr.solrxml.location=zookeeper \
        -Dsolr.log.dir=$home
