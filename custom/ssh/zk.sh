#!/usr/bin/env bash
if [ -z "$1" ]; then
  echo "operator is empty !!!"
  exit 1
fi

bash /Volumes/Software/Apache/zookeeper/zookeeper-3.5.8-2181/bin/zkServer.sh $1
bash /Volumes/Software/Apache/zookeeper/zookeeper-3.5.8-2182/bin/zkServer.sh $1
bash /Volumes/Software/Apache/zookeeper/zookeeper-3.5.8-2183/bin/zkServer.sh $1