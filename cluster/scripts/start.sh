#!/bin/bash

echo "++++++ Building delle immagini docker ++++++"

./build-docker.sh

echo "++++++ Creazione del docker network ++++++"

docker network create --driver bridge hadoop_network

echo "++++++ Running Cluster ++++++"

x-terminal-emulator -e ./start-dockers.sh

echo "++++++ Running Client ++++++"

./start-client.sh
