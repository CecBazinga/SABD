#!/bin/bash
docker build -t hadoop3-worker ../src/slave
docker build -t hadoop3-master ../src/master

cp -r ../../SparkClient ../src/client/SparkClient

docker build -t client-image   ../src/client

rm -r ../src/client/SparkClient

