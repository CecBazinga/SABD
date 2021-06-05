package it.sabd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


//Connettore con HBase implementato come singleton
public class HBaseConnector {

    private static HBaseConnector instance = null;
    private Admin admin;
    private Connection conn;



    private HBaseConnector(){

        System.out.println("Connessione con HBASE");

        //Creazione della configutazione
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "master:60000");
    	conf.set("hbase.zookeeper.quorum", "master");
	    conf.setInt("hbase.zookeeper.property.clientPort", 2181);

	
        try {

            //Connessione con HBASE
            conn = ConnectionFactory.createConnection(conf);
            admin  = conn.getAdmin();

        } catch (Exception e) { e.printStackTrace(); }

        System.out.println("Connessione effettuata con successo");

    }

    public static HBaseConnector getInstance(){

        if(instance == null) instance = new HBaseConnector();

        return instance;

    }

    public void SaveQuery1(JavaPairRDD<String, Tuple2<String, String>> rdd){

        try {

            //Creazione di una nuova tabella
            HTableDescriptor tableDescriptor = new
                    HTableDescriptor(TableName.valueOf("Query1"));

            //Aggiunta famiglia di colonne
            tableDescriptor.addFamily(new HColumnDescriptor("Valore"));

            //Aggiunta tabella
            admin.createTable(tableDescriptor);

            System.out.println("Tabella creata con successo");

            Table htable = conn.getTable(TableName.valueOf("Query1"));

            //Aggiunta dei dati alla tabella
            List<Put> puts = new ArrayList<>();
            List<Tuple2<String, Tuple2<String, String>>> rddList =  rdd.collect();

            for(Tuple2<String, Tuple2<String, String>> x : rddList ) {

                Put p = new Put(Bytes.toBytes(x._1 + "-" + x._2._1));
                p.addColumn(Bytes.toBytes("Valore"), Bytes.toBytes("Col"), Bytes.toBytes(x._2._2));

                puts.add(p);
            }

            //Aggiunta di tutte le row
            htable.put(puts);

            System.out.println("Scrittura effettuata con successo");

            htable.close();


        } catch(Exception e) { e.printStackTrace(); }

    }


    public void SaveQuery2(JavaPairRDD<String, List<Tuple2<String, Long>>> rdd){

        try {

            //Creazione di una nuova tabella
            HTableDescriptor tableDescriptor = new
                    HTableDescriptor(TableName.valueOf("Query2"));

            //Aggiunta famiglia di colonne
            tableDescriptor.addFamily(new HColumnDescriptor("Classifica"));

            //Aggiunta tabella
            admin.createTable(tableDescriptor);

            System.out.println("Tabella creata con successo");

            Table htable = conn.getTable(TableName.valueOf("Query2"));


            //Necessaria trasformazione per rendere RDD serializzabile
            JavaPairRDD<String, String> rddSerializable = rdd.mapToPair(x -> new Tuple2<>(x._1, x._2.toString()));

            List<Put> puts = new ArrayList<>();
            List<Tuple2<String, String>> rddList = rddSerializable.collect();

            for(Tuple2<String, String> x : rddList ) {
                Put p = new Put(Bytes.toBytes(x._1));
                p.addColumn(Bytes.toBytes("Classifica"), Bytes.toBytes("Class"), Bytes.toBytes(x._2));

                puts.add(p);
            }

            //Aggiunta di tutte le row
            htable.put(puts);

            System.out.println("Scrittura effettuata con successo");

            htable.close();


        } catch(Exception e) { e.printStackTrace(); }

    }


    public void SaveQuery3(JavaPairRDD<String, Tuple2<String, String>> rdd, String tableName){

        try {

            //Creazione di una nuova tabella
            HTableDescriptor tableDescriptor = new
                    HTableDescriptor(TableName.valueOf(tableName));

            //Aggiunta famiglia di colonne
            tableDescriptor.addFamily(new HColumnDescriptor("Clusters"));

            //Aggiunta tabella
            admin.createTable(tableDescriptor);

            System.out.println("Tabella creata con successo");

            Table htable = conn.getTable(TableName.valueOf(tableName));



            List<Put> puts = new ArrayList<>();
            List<Tuple2<String, Tuple2<String, String>>> rddList = rdd.collect();



            for(Tuple2<String, Tuple2<String, String>> x : rddList ) {
                Put p = new Put(Bytes.toBytes(x._1));
                p.addColumn(Bytes.toBytes("Clusters"), Bytes.toBytes("WSSSE"), Bytes.toBytes(x._2._1));
                p.addColumn(Bytes.toBytes("Clusters"), Bytes.toBytes("Regioni"), Bytes.toBytes(x._2._2));

                puts.add(p);

            }

            //Aggiunta di tutte le row
            htable.put(puts);

            System.out.println("Scrittura effettuata con successo");

            htable.close();


        } catch(Exception e) { e.printStackTrace(); }

    }


}
