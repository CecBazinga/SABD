#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start

# Launch bash console  
/bin/bash

cd /usr/local

echo "Esecuzione dell'applicazione..."

#spark-submit --class it.sabd.QueryExecutor --master yarn --deploy-mode client sparkapp.jar 

echo "Applicazione terminata"




