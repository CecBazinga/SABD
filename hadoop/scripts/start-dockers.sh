#!/bin/bash

docker run -t -i -p 9864:9864 -d --network=hadoop_network --name=slave1 hadoop3-worker
docker run -t -i -p 9863:9864 -d --network=hadoop_network --name=slave2 hadoop3-worker
docker run -t -i -p 9862:9864 -d --network=hadoop_network --name=slave3 hadoop3-worker
docker run -t -i -p 9870:9870 -p 8088:8088 --network=hadoop_network --name=master hadoop3-master
