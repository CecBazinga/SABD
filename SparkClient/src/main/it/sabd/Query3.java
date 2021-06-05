package it.sabd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.*;
import static it.sabd.Utils.*;
import static org.apache.spark.sql.functions.*;



public class Query3 {

    public static void computeQuery3(Dataset<Row> dfSVSLQuery3, Dataset<Row> dfTPQuery3, SparkSession sSession){


        System.out.println("\n\n################################## QUERY 3 ##################################\n");


        //Aggiornamento del path di salvataggio di destinazione
        String destinationPath = Utils.outputQueries + "Query3/";



        //Preparazione e filtraggio del dataset

        //Load dei dataset
        Dataset<Row> dfSVSL = dfSVSLQuery3;
        Dataset<Row> dfTP = dfTPQuery3;

        //Formattazione data
        dfSVSL = dfSVSL.withColumn( "data_somministrazione",to_date(date_format(col("data_somministrazione"), "yyyy-LL-dd")));

        //filtraggio del dataset
        dfSVSL = dfSVSL.filter(col("data_somministrazione").lt(lit("2021-06-01")));




        long timeQuery3Spark = computeQuery3Spark(dfSVSL, dfTP, destinationPath,sSession);


        System.out.println(" + Tempo Query 3 SPARK: " + timeQuery3Spark/Utils.nanosecondsInSeconds + " secondi");


        System.out.println("\n\n#############################################################################\n");

    }



    private static long computeQuery3Spark(Dataset<Row> dfSVSL,Dataset<Row> dfTP, String destinationPath,SparkSession sSession){


        System.out.println("\n\n********************************** QUERY 3 SPARK ************************************** \n");


        //Conversione da dataframe
        JavaPairRDD<String, Tuple2<Date, Long>> svslRdd = dfSVSL.toJavaRDD().mapToPair(x -> new Tuple2<>
                (x.getString(1), new Tuple2<>(x.getDate(0), (long) x.getInt(2))));

        svslRdd.cache();


        JavaPairRDD<String,Long> totalPopulationRdd = dfTP.toJavaRDD().mapToPair(x -> new Tuple2<>
                (regionNameReducer(x.getString(0)),(long) x.getInt(1)));

        totalPopulationRdd.cache();

        Date date1Jun = new GregorianCalendar(2021, Calendar.JUNE, 1).getTime();

        //Tracciamento del tempo
        long startTime = System.nanoTime();



        //Raggruppamento per ogni area dell'insieme di tutti i giorni
        JavaPairRDD<String, Iterable<Tuple2<Date, Long>>> groupByAreaRdd = svslRdd.groupByKey();



        //Applicazione della regressione per calcolare i vaccini al 1 giugno
        JavaPairRDD<String, Tuple2<Date, Long>> predictedRdd = groupByAreaRdd.mapToPair(
                x -> new Tuple2<>(x._1, new Tuple2<>(date1Jun, Utils.totalRegression(x._2))));

        //Unione dei due RDD
        JavaPairRDD<String, Tuple2<Date, Long>> unionRdd = svslRdd.union(predictedRdd);

        unionRdd.cache();


        //RDD con regione e totale vaccinazioni
        JavaPairRDD<String,Long> totalVaccRdd = unionRdd.mapToPair(x-> new Tuple2<>(x._1, x._2._2)).reduceByKey((x, y) -> x + y);

        //Join con la popolazione totale per regione
        JavaPairRDD<String, Tuple2<Long, Long>> joinedRdd = totalVaccRdd.join(totalPopulationRdd);

        //Calcolo del rapporto "totale somministrazione vaccini" / "popolazione regione" * 100
        JavaPairRDD<String,Double> finalRdd = joinedRdd.mapToPair(x-> new Tuple2<>(x._1, (double) ((float) x._2._1 / x._2._2 * 100)));

        finalRdd.cache();


        //Necessario per computare la lineage (se non viene fatto, vengono alterati i tempi di Kmeans e BSKmeans
        finalRdd.count();



        int minCluster = 2;
        int maxCluster = 5;

        //Applicazione di KMEANS (Ritorna Tupla {tempi per ogni K, risultato clustering} )
        Tuple2<double[], JavaPairRDD<String, Tuple2<String, String>>> kMeansResults = applyKMeans(finalRdd, minCluster, maxCluster);

        //Applicazione di BSECTING KMEANS (Ritorna Tupla {tempi per ogni K, risultato clustering} )
        Tuple2<double[], JavaPairRDD<String, Tuple2<String, String>>> bisectingKMeansResults = applyBisectingKMeans(finalRdd, minCluster, maxCluster);




        if(Utils.DEBUG) {


                kMeansResults._2.foreach(x -> {
                    System.out.println("Printing predicted KMEANS: " + x);
                });


                bisectingKMeansResults._2.foreach(x -> {
                    System.out.println("Printing predicted BSECTING KMEANS: " + x);
                });

        }


        long endTime = System.nanoTime();


        System.out.println("\n\n------------------------- Tempi di K-MEANS -------------------------\n");

        for(int i = 0; i < maxCluster - minCluster + 1; i++){

            System.out.println("Tempo per KMEANS con K=" + Integer.toString(i + minCluster) + ": " + kMeansResults._1[i] + " secondi");
        }

        System.out.println("\n--------------------------------------------------------------------\n");




        System.out.println("\n\n------------------------- Tempi di BSECTING K-MEANS -------------------------\n");

        for(int i = 0; i < maxCluster - minCluster + 1; i++){

            System.out.println("Tempo per KMEANS con K=" + Integer.toString(i + minCluster) + ": " + bisectingKMeansResults._1[i] + " secondi");
        }

        System.out.println("\n--------------------------------------------------------------------\n");


        //Inserimento dei risultati di Clustering in HDFS e HBase
        try {

            convertToRDD(kMeansResults._2, sSession).saveAsTextFile(destinationPath + "Query3KMeans");
            convertToRDD(bisectingKMeansResults._2, sSession).saveAsTextFile(destinationPath + "Query3BSectiongKMeans");

            //Scrittura su HBase
            HBaseConnector.getInstance().SaveQuery3(kMeansResults._2, "Query3Kmeans");
            HBaseConnector.getInstance().SaveQuery3(bisectingKMeansResults._2, "Query3BSKmeans");

        } catch (Exception e) { e.printStackTrace(); }


        System.out.println("\n\n*************************************************************************************** \n");





        return (endTime-startTime);

    }



    private static RDD<String> convertToRDD(JavaPairRDD<String, Tuple2<String, String>> javaRdd, SparkSession sSession){


        javaRdd = javaRdd.coalesce(1);

        javaRdd.cache();

        //Preparazione del RDD per la scrittura su HDFS in formato CSV
        List<String> header = Collections.singletonList("numero_cluster,WSSSE,[cluster]Regione");

        JavaSparkContext sc = new JavaSparkContext(sSession.sparkContext());
        RDD<String> headerRDD = sc.parallelize(header).rdd();

        RDD<String> saveRDD = javaRdd.map(x-> x._1 + "," + x._2._1 + "," + x._2._2).rdd();
        saveRDD = headerRDD.union(saveRDD);

        //Raggruppamento in un singolo RDD ordinato
        saveRDD = saveRDD.repartition(1, null);

        return saveRDD;

    }
}
