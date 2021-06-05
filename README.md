# SABD - Progetto #1
Progetto per Sistemi e Architetture per Big Data

Team composto da:
 - Alessandro Amici
 - Andrea Paci

## Istruzioni per il setup dell'ambiente e l'esecuzione dell'applicazione

### Ambiente di esecuzione

 - Distro Linux (Sviluppato e testato su Ubuntu 18 e 20) 

 - Installare Containerd, Docker CE CLI e Docker CE (Engine)
 
 - [OPZIONALE] ``` sudo docker run hello-world ``` per testare docker
 

### Setup del cluster e dell'applicazione

  	
Il cluster è stanziato usando Docker. I Dockerfile con i relativi file di configurazione sono presenti nella cartella "cluster". All'interno sono presenti le folder per le build relative ai vari nodi:
 - client: Eseguirà l'applicazione Spark in modalità "client"
 - Nifi: si occuperà della data ingestion
 - master: Nodo master dell'architettura Hadoop, Hbase e Zookeeper
 - slave: Nodo worker del cluster
 
 Nella folder SparkClient è presente il codice sorgente dell'applicativo

Per eseguire il setup del cluster e l'esecuzione dell'applicazione aprire un terminale sulla folder del progetto ed eseguire

``` cd cluster/scripts
sudo ./start_cluster.sh 
```
Questo comando farà la build delle varie Docker images e le eseguirà. Lo script sarà interattivo.

Inizializzato il cluster e Nifi, prima dell'esecuzione dell'applicazione Spark è necessario collegarsi a ```localhost:8180/nifi``` ed eseguire l'import del template ed eseguirlo. Il template è situato in ```cluster/src/nifi/flow.xml```

Uscire con CTRL-D p CTRL-C dalle varie shell nel seguente ordine
 1) Cluster (Hadoop)
 1) Nifi
 2) Client  (Spark)
 
#### NB:
Per avere un output più comprensibile durante l'esecuzione dell'applicativo, sono state fatte delle stampe aggiuntive sul dataset. Queste stampe incrementano i tempi di processamento. Per disabilitare le stampe è possibile impostare la variabile  ``` Utils.DEBUG = false```.


### Step opzionali	
 	
 - Fare il check del corretto funzionamento con ```hdfs dfsadmin -report``` sul terminale del cluster, oppure collegandoci a http://localhost:9870/dfshealth.html
 	
 - Sul master node di Hadoop, aprire /etc/hadoop/conf/capacity-scheduler.xml e modificare il campo "yarn.scheduler.capacity.maximum-am-resource" alzandolo a 0.5 (Migliori performance, ma niente di critico per il run di una singola applicazione) e reinizializzare il cluster prima dell'esecuzione del client

 - E' possibile consultare nel HDFS i file salvati in formato CSV, sia tramite WebUI che tramite CLI
 
 - E' possibile consultare con la shell di HBase ```$HBASE_HOME/bin/hbase shell``` il contenuto delle tabelle ("list" per ottenere la lista delle tabelle presenti e "scan 'table_name' " per recuperare il contenuto delle tabelle)
 	 
 - Prima di interrompere il client Spark è possibile controllare lo stato del sistema collegandosi a:
 		
	+ http://localhost:8088   		               (Hadoop e YARN) 
 	+ http://localhost:9870   		               (HDFS)  
 	+ http://localhost:8088/proxy/{app_id}   	(Spark) (L'URL esatto è presente all'inizio del log del client Spark, dove è necessario sostituire "localhost" con "master")
 	+ http://localhost:60010  		               (HBase)
	
(se 8088 non risponde, provare 8080)  
 		
## Risultati

I risultati sono presenti nella folder Results e sono divisi per Query. I risultati sono stati copiati dall'HDFS già in formato CSV senza nessuna manipolazione.

La data del dataset utilizzato è 04/06/2021 (Ultima esecuzione del codice).
I dataset di input vengono scaricati ad ogni esecuzione, non vi è quindi una data fissa.

## Framework e tecnologie utilizzate

 - Java 1.8

 - Spark 3.1.1

 - Hadoop 3.2.2

 - ZooKeeper

 - HBase 1.4.13

 - Nifi 1.13.2

 - Parquet
