#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start

# Start HDFS and YARN
hdfs namenode -format

$HADOOP_HOME/sbin/start-all.sh

# Launch bash console  
/bin/bash



