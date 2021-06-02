package it.sabd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.spark.sql.functions.*;

public class QueryExecutor {





    //TODO: vedere dalla traccia quando va fatto il filtraggio delle date, fare classe QUERY1 e classe QUERY2 e da li chiamare le due Query
    //TODO: sistemare come l'output viene salvato
    //TODO: change app name e nome della classe

    public static void main(String[] args) throws InterruptedException {


        /*
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Query1");
        JavaSparkContext sContext = new JavaSparkContext(conf);



        SparkSession sSession = SparkSession
                .builder()
                .appName("Query1").master("local[6]")
                .config("spark.dynamicAllocation.enabled", "true")
                .config("spark.executor.cores", 4)
                .config("spark.dynamicAllocation.minExecutors","3")
                .config("spark.dynamicAllocation.maxExecutors","5")
                .getOrCreate();
         */



        SparkSession sSession = SparkSession
                .builder()
                .appName("Query1").master("yarn").config("spark.sql.shuffle.partitions", "3")
                .getOrCreate();




        //TODO: rimuovere colonne da nifi


        HBaseConnector.getInstance().run();

        //Computo della prima query

        Query1.computeQuery1(sSession);

        //Computo della seconda query

        Query2.computeQuery2(sSession);












        //TODO: aggiungere compilazione maven, quando fai regressione ricordati che devi calcolare il giorno dopo a quello della grandezza del mese, levare static ai metodi, vedere se va fatto sparkconf, metti HBASE,
        //TODO: check che alcuni valori della regressione hanno valore negativo (UPDATE, prendendo quei valori e graficandoli ci sta che vanno in negativo, mettere il limite che se è < 0, allora = 0








        TimeUnit.MINUTES.sleep(10);

        sSession.close();
    }
}
