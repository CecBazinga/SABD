#!/bin/bash
docker run -t -i -p 8080:8080 -p 4040:4040 --network=hadoop_network --name=client client-image
