#!/bin/bash
docker build -t hadoop3-worker ../src/slave
docker build -t hadoop3-master ../src/master
docker build -t client-image   ../src/client


