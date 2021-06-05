#!/bin/bash
docker kill slave1 slave2 slave3 master nifi client
docker rm master slave1 slave2 slave3 nifi client
docker network rm hadoop_network
