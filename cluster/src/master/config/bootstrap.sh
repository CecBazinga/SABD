#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start

# Start HDFS and YARN
hdfs namenode -format

$HADOOP_HOME/sbin/start-all.sh

hdfs dfs -put /usr/local/files /files

echo "Hadoop cluster succesfully started"

#Start HBase
$HBASE_HOME/bin/start-hbase.sh

echo "Hbase cluster started"


echo "+++++++ CLUSTER STARTED SUCCESFULLY +++++++"

# Launch bash console  
/bin/bash



