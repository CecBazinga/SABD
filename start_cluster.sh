#!/bin/bash
echo "Creazione del cluster"
echo "Nota: lo script è interattivo (necessario per rispettare i vari timing per la sincronizzazione dei vari nodi)"
echo "Si apriranno più terminali che simuleranno i vari nodi del cluster\n\n"
echo "++++++ Building delle immagini docker ++++++"

./cluster/scripts/build-docker.sh

echo "++++++ Creazione del docker network ++++++"

docker network create --driver bridge hadoop_network

echo "++++++ Running del Cluster ++++++"

x-terminal-emulator -e ./cluster/scripts/start-dockers.sh

echo "Premere ENTER dopo che sul terminale appena aperto viene indicato che il cluster è stato inizializzato correttamente"

read input

echo "++++++ Running del Client ++++++"

./cluster/scripts/start-client.sh

echo "++++++ Eliminazione e pulizia dei container ++++++"

./cluster/scripts/stop-docker.sh

echo "Applicazione terminata."
