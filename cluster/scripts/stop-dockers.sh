#!/bin/bash
docker kill slave1 slave2 slave3 client
docker rm master slave1 slave2 slave3 client
docker network rm hadoop_network
