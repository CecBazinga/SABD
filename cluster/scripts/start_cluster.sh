#!/bin/bash
echo "Creazione del cluster"
echo "Nota: lo script è interattivo (necessario per rispettare i vari timing per la sincronizzazione dei vari nodi)"
echo "Si apriranno più terminali che simuleranno i vari nodi del cluster"
echo ""
echo ""
echo "Premere ENTER per continuare."

read input

echo "++++++ Building delle immagini docker ++++++"


./dockerscripts/build-docker.sh

echo "++++++ Creazione del docker network ++++++"

docker network create --driver bridge hadoop_network

echo ""
echo ""
echo "++++++ Running del Cluster ++++++"

x-terminal-emulator -e ./dockerscripts/start-dockers.sh

echo ""
echo ""
echo "Premere ENTER dopo che sul terminale appena aperto viene indicato che il cluster è stato inizializzato correttamente"

read input

echo "++++++ Running del Client ++++++"
echo ""

./dockerscripts/start-client.sh

echo "++++++ Eliminazione e pulizia dei container ++++++"

./dockerscripts/stop-dockers.sh

echo "Applicazione terminata."
