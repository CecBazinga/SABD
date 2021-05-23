#!/bin/bash
docker network create --driver bridge hadoop_network
docker run -t -i -p 9864:9864 -d --network=hadoop_network --name=slave1 effeerre/hadoop
docker run -t -i -p 9863:9864 -d --network=hadoop_network --name=slave2 effeerre/hadoop
docker run -t -i -p 9862:9864 -d --network=hadoop_network --name=slave3 effeerre/hadoop
docker run -t -i -p 9870:9870 --network=hadoop_network --name=master effeerre/hadoop
