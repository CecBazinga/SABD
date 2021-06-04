## Istruzioni per il setup dell'ambiente e l'esecuzione dell'applicazione

### Ambiente di esecuzione

 - Distro Linux (Sviluppato e testato su Ubuntu 18 e 20) 

 - Installare Containerd, Docker CE CLI e Docker CE (Engine) da: https://download.docker.com/linux/ubuntu/dists/focal/pool/stable/amd64/
   (Per installarli è necessario fare $ sudo dpkg -i path/to/package nell'ordine specificato sopra.)
 
 - [OPZIONALE] $ sudo docker run hello-world per testare docker
 

### Setup del cluster e dell'applicazione

  	
Il cluster è stanziato usando Docker. I Dockerfile con i relativi file di configurazione sono presenti nella cartella "cluster". All'interno sono presenti le folder per le build relative ai vari nodi:
 - client: Eseguirà l'applicazione Spark in modalità "client"
 - master: Nodo master dell'architettura Hadoop, Hbase e Zookeeper
 - slave: Nodo worker del cluster
 
 Nella folder SparkClient è presente il codice sorgente dell'applicativo

Per eseguire il setup del cluster e l'esecuzione dell'applicazione aprire un terminale ed eseguire

 $ sudo ./start_cluster.sh

Questo comando farà la build delle varie Docker images e le eseguirà. Lo script sarà interattivo.

Uscire con CTRL-D dalle varie shell nel seguente ordine
 1) Cluster (Hadoop)
 2) Client  (Spark)


### Step opzionali	
 	
 - Fare il check del corretto funzionamento con $ hdfs dfsadmin -report, oppure collegandoci a http://localhost:9870/dfshealth.html
 	
 - Sul master node di Hadoop, aprire /etc/hadoop/conf/capacity-scheduler.xml e modificare il campo "yarn.scheduler.capacity.maximum-am-resource" alzandolo a 0.5 (Migliori performance, ma niente di critico per il run di una singola applicazione) e reinizializzare il cluster prima dell'esecuzione del client

 - E' possibile consultare nel HDFS i file salvati in formato CSV, sia tramite WebUI che tramite CLI 
 
 - E' possibile consultare con la shell di HBase ($ $HBASE_HOME/bin/hbase shell) il contenuto delle tabelle ("list" per ottenere la lista delle tabelle presenti e "scan 'table_name' per recuperare il contenuto delle tabelle
 	 
 - Prima di interrompere il client Spark è possibile controllare lo stato del sistema collegandosi a:
 		
	+ http://localhost:8088   		               (Hadoop e YARN) 
 	+ http://localhost:9870   		               (HDFS)  
 	+ http://master:8080/proxy/{app_id}   	(Spark) (L'URL esatto è presente all'inizio del log del client Spark, dove è necessario sostituire "localhost" con "master")
 	+ http://localhost:60010  		               (HBase)
	
(se 8088 non risponde, provare 8080)  
 		


## Framework e tecnologie utilizzate

 - Java 1.8

 - Spark 3.1.1

 - Hadoop 3.2.2

 - ZooKeeper

 - Base 1.4.13

 - Nifi 1.13.2
