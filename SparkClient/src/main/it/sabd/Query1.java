package it.sabd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Query1 {


    public static void computeQuery1(SparkSession sSession, Dataset<Row> dfSVSLQuery1, Dataset<Row> dfPSTQuery1){


        System.out.println("\n\n################################## QUERY 1 ##################################\n");


        //Aggiornamento del path di salvataggio di destinazione
        String destinationPath = Utils.outputQueries + "Query1/";



        //Preparazione e filtraggio del dataset

        //Load dei dataset
        Dataset<Row> dfSVSL = dfSVSLQuery1;
        Dataset<Row> dfPST  = dfPSTQuery1;

        //Per la query 1 non serve sapere il giorno
        dfSVSL = dfSVSL.withColumn( "data_somministrazione",to_date(date_format(col("data_somministrazione"), "yyyy-LL")));

        //Sorting e filtraggio del dataset
        dfSVSL = dfSVSL.filter(col("data_somministrazione").gt(lit("2020-12-31")));
        dfSVSL = dfSVSL.filter(col("data_somministrazione").lt(lit("2021-06-01")));
        dfSVSL = dfSVSL.sort(col("data_somministrazione"));

        long timeQuery1Spark = computeQuery1Spark(dfSVSL, dfPST, destinationPath, sSession);

        long timeQuery1SQL   = computeQuery1SQL(dfSVSL, dfPST, destinationPath, sSession);


        System.out.println(" + Tempo Query 1 SPARK: " + timeQuery1Spark/Utils.nanosecondsInSeconds + " secondi");
        System.out.println(" + Tempo Query 1 SQL:   " + timeQuery1SQL/Utils.nanosecondsInSeconds +   " secondi");


        System.out.println("\n\n#############################################################################\n");



    }






    private static long computeQuery1Spark(Dataset<Row> dfSVSL, Dataset<Row> dfPST, String destinationPath, SparkSession sSession){


        System.out.println("\n\n********************************** QUERY 1 SPARK ************************************** \n");


        //Conversione dataframe a rdd con solo le colonne desiderate
        JavaPairRDD<Tuple2<Date, String>, Integer> rddpairSVSL = dfSVSL.toJavaRDD().mapToPair(x -> new Tuple2<Tuple2<Date, String>, Integer>
                (new Tuple2<Date, String>(x.getDate(0), x.getString(1)), x.getInt(2)));

        rddpairSVSL.cache();


        //Conversione dataframe a rdd con solo le colonne desiderate
        JavaPairRDD<String, Integer> dfPSTPairs = dfPST.toJavaRDD().mapToPair(row -> new Tuple2<String, Integer>(row.getString(0), 1));

        dfPSTPairs.cache();


        //Tracciamento del tempo
        long startTime = System.nanoTime();

        JavaPairRDD<String, Integer> dfPSTCount = dfPSTPairs.reduceByKey((x, y) -> x + y);

        //Prima di mandare dai dati tra le partizioni, reduceByKey() fa merge dei dati localmente
        JavaPairRDD<Tuple2<Date, String>, Integer> regionalSomministrationsPerMonth = rddpairSVSL.reduceByKey((x, y) -> x + y);

        //Trasforma regionalSomministrationsPerMonth isolando l'attributo "area" come key per il join con dfPST
        JavaPairRDD<String, Tuple2<Date, Integer>> regionalSomministrationsPerMonthJoinable = regionalSomministrationsPerMonth.
                mapToPair(x -> new Tuple2<String, Tuple2<Date, Integer>>(x._1._2, new Tuple2<Date, Integer>(x._1._1, x._2)));



        //Join sull'attributo area
        JavaPairRDD<String, Tuple2<Tuple2<Date, Integer>, Integer>> rddJoin = regionalSomministrationsPerMonthJoinable.join(dfPSTCount);


        //Rdd with date , region and mean values of daily vaccines per center
        JavaPairRDD<Date, Tuple2<String, Double>> finalPairRdd = rddJoin.mapToPair(x -> (new Tuple2<Date, Tuple2<String, Double>>
                (x._2._1._1, new Tuple2<String, Double>(x._1, Utils.computeDailyDoses(x._2._1._1, ((double) x._2._1._2 / x._2._2))))));


        finalPairRdd.cache();


        long endTime = System.nanoTime();

        //Traduzione del dataset in un formato più user-friendly

        //Traduzione di sigle regionali in forma estesa invece di tenere colonna aggiuntiva nel RDD
        JavaPairRDD<Date, Tuple2<String, Double>> extendedNamesRdd = finalPairRdd.mapToPair(x -> (new Tuple2<Date, Tuple2<String, Double>>
                (x._1, new Tuple2<String, Double>(Utils.regionNameConverter(x._2._1), x._2._2)))).sortByKey();

        JavaPairRDD<String, Tuple2<String, String>> finalRdd = extendedNamesRdd.mapToPair(x -> (new Tuple2<String, Tuple2<String, String>>
                (Utils.dateConverter(x._1), new Tuple2<String, String>(x._2._1, String.format("%.2f", x._2._2).replace(",", ".")))));

        finalRdd = finalRdd.coalesce(1);

        finalRdd.cache();

        //Preparazione del RDD per la scrittura su HDFS in formato CSV
        List<String> header = Collections.singletonList("mese,area,valore_medio");

        JavaSparkContext sc = new JavaSparkContext(sSession.sparkContext());
        RDD<String> headerRDD = sc.parallelize(header).rdd();
        RDD<String> saveRDD = finalRdd.map(x-> x._1 + "," + x._2._1 + "," + x._2._2).rdd();
        saveRDD = headerRDD.union(saveRDD);

        //Raggruppamento in un singolo RDD ordinato
        saveRDD = saveRDD.repartition(1, null);




        if(Utils.DEBUG){


            finalRdd.foreach(x ->{
                System.out.println("Printing Query1: " + x);
            });
        }


        //Scrittura su HDFS e HBase

        try {

            saveRDD.saveAsTextFile(destinationPath + "Query1Spark");

            HBaseConnector.getInstance().SaveQuery1(finalRdd);

        } catch (Exception e) { e.printStackTrace(); }

        System.out.println("\n\n*************************************************************************************** \n");


        return (endTime - startTime);
    }







    private static long computeQuery1SQL(Dataset<Row> dfSVSL, Dataset<Row> dfPST, String destinationPath, SparkSession sSession){

        System.out.println("\n\n********************************** QUERY 1 SPARK-SQL ********************************** \n");


        //Tracciamento del tempo
        long startTime = System.nanoTime();

        //Aggiunta della view sui dataset
        dfSVSL.createOrReplaceTempView("SVSL");
        dfPST.createOrReplaceTempView("PST");

        if(Utils.DEBUG) {
            System.out.println("\n\n+++++++++++++ SOMMINISTRAZIONI VACCINI SUMMARY LATEST PER MONTH ++++++++++++++\n");

            dfSVSL.show(100);

            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

            System.out.println("\n\n++++++++++++++++++++++ PUNTI SOMMINISTRAZIONE TIPOLOGIA ++++++++++++++++++++++\n");

            dfPST.show(100);


            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }


        //Retrive del numero di centri per ogni regione
        Dataset<Row> dfPSTAverage = sSession.sql(
                "SELECT area, count(area) as centri_tot " +
                        "FROM PST " +
                        "GROUP BY area");

        //Creazione view
        dfPSTAverage.createOrReplaceTempView("PST_average");


        if(Utils.DEBUG) {
            System.out.println("\n\n+++++++++++++++++++++++++ # PUNTI SOMM. PER REGIONE ++++++++++++++++++++++++++\n");

            dfPSTAverage.show(100);

            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }

        //Calcolo del valore medio di vaccini effettuati in un mese da un centro vaccinale di una regione
        Dataset<Row> dfSVSLCenterAverage = sSession.sql(
                "SELECT DISTINCT data_somministrazione, SVSL.area, sum(totale/centri_tot)/(day(last_day(data_somministrazione))) as tot " +
                        "FROM SVSL JOIN PST_average " +
                        "ON SVSL.area = PST_average.area " +
                        "GROUP BY data_somministrazione, SVSL.area, centri_tot " +
                        "ORDER BY data_somministrazione, area");

        dfSVSLCenterAverage.createOrReplaceTempView("SVSL_centri_average");
        dfSVSLCenterAverage.cache();

        //Riscrittura ordinata del result
        dfSVSLCenterAverage = sSession.sql(
                "SELECT DATE_FORMAT(data_somministrazione, 'yyyy-MMM') as mese_anno, area, CAST(tot as DECIMAL(10,2)) as media_centro " +
                        "FROM SVSL_centri_average");




        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ AVG SOMM. PER CENTRO PER REGIONE +++++++++++++++++++++++\n");

            dfSVSLCenterAverage.show(100);

            System.out.println("\n\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }



        long endTime = System.nanoTime();


        System.out.println("\n\n*************************************************************************************** \n");


        //Scrittura su HDFS
        try {
            dfSVSLCenterAverage.coalesce(1)
                    .write().format("csv")
                    .option("sep", ",")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(destinationPath + "Query1SQL");
        } catch (Exception e) { e.printStackTrace(); }

        return (endTime - startTime);
    }


}




