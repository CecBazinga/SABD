package it.sabd;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class Query2 {



    public static void computeQuery2(SparkSession sSession,  Dataset<Row> dfSVLQuery2){


        System.out.println("\n\n################################## QUERY 2 ##################################\n");


        //Aggiornamento del path di salvataggio di destinazione
        String destinationPath = Utils.outputQueries + "Query2/";



        //Preparazione e filtraggio del dataset

        //Load dei dataset

        Dataset<Row> dfSVL = dfSVLQuery2;



        //Creazione Dataframe per la seconda query con sorting sulla data
        dfSVL = dfSVL.withColumn( "data_somministrazione",to_date(date_format(col("data_somministrazione"), "yyyy-LL-dd")));

        //Sorting e filtraggio del dataset
        dfSVL = dfSVL.filter(col("data_somministrazione").geq(lit("2021-02-01")));
        dfSVL = dfSVL.sort(col("data_somministrazione")).filter(col("data_somministrazione").lt(lit("2021-06-01")));

        long timeQuery2Spark = computeQuery2Spark(dfSVL, destinationPath, sSession);

        long timeQuery2SQL = computeQuery2SQL(dfSVL, destinationPath, sSession);


        System.out.println(" + Tempo Query 2 SPARK: " + timeQuery2Spark/Utils.nanosecondsInSeconds + " secondi");
        System.out.println(" + Tempo Query 2 SQL:   " + timeQuery2SQL/Utils.nanosecondsInSeconds +   " secondi");


        System.out.println("\n\n#############################################################################\n");



    }

    //TODO: commentare query 2
    private static long computeQuery2Spark(Dataset<Row> dfSVL, String destinationPath, SparkSession sSession){


        System.out.println("\n\n********************************** QUERY 2 SPARK ************************************** \n");


        long startTime = System.nanoTime();

        dfSVL = dfSVL.withColumn( "data_somministrazione",to_date(date_format(col("data_somministrazione"),
                "yyyy-LL-dd"))).filter(col("data_somministrazione").gt(lit("2021-01-31")));



        JavaPairRDD<Tuple3<Date,String, String>, Long> avoidCloneDaysRdd = dfSVL.toJavaRDD().mapToPair(x -> new Tuple2<Tuple3< Date,String,String>,Long>
                (new Tuple3<>(x.getDate(0),x.getString(2),x.getString(3)), (long) x.getInt(5))).reduceByKey((x, y) -> x+y);



        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<Date,Long>>> rddpairSVL = avoidCloneDaysRdd.mapToPair(x ->
                new Tuple2<>(new Tuple2<>(x._1._2(),x._1._3()), new Tuple2<>(x._1._1(), x._2))).groupByKey();



        rddpairSVL.cache();


        JavaPairRDD<Tuple2<String, String>, List<Tuple2<Date, Long>>> rddByMonth = rddpairSVL.flatMapToPair(new Utils.daysGroupedByMonth());

        JavaPairRDD<Tuple2<String, String>, List<Tuple2<Date,Long>>> filteredRddByMonth = rddByMonth.filter(x -> x._2.size() > 1);

        JavaPairRDD<String, Iterable<Tuple2<String, Long>>> predictedRdd = filteredRddByMonth.mapToPair(x ->
                new Tuple2<>(Utils.firstDayNextMonth(x._2.get(0)._1) + " " + x._1._2,new Tuple2<>(Utils.regionNameConverter(x._1._1), Utils.applyRegression(x._2))))
                .groupByKey()
                .sortByKey();


        JavaPairRDD<String , List<Tuple2<String,Long>>> orderedRdd = predictedRdd.mapToPair(x ->
                new Tuple2<String, List<Tuple2<String,Long>>>(x._1, Utils.iterableToListTop5(x._2)));


        //Raggruppamento
        orderedRdd = orderedRdd.coalesce(1);


        long endTime = System.nanoTime();


        //Preparazione del RDD per la scrittura su HDFS in formato CSV

        List<String> header = Collections.singletonList("anno-fascia,area_1,previsione_1,area_2,previsione_2,area_3,previsione_3,area_4,previsione_4,area_5,previsione_5");

        JavaSparkContext sc = new JavaSparkContext(sSession.sparkContext());
        RDD<String> headerRDD = sc.parallelize(header).rdd();
        JavaRDD<String> saveJavaRDD = orderedRdd.map(x -> x._1 + "," + x._2.toString().replace("[(","").replace(")]","").replace("(","").replace(")", "").replace(" ", ""));

        if(Utils.DEBUG) {
            saveJavaRDD.foreach(x -> {
                System.out.println("Printing: " + x);
            });
        }



        System.out.println("\n\n*************************************************************************************** \n");


        RDD<String> saveRDD = headerRDD.union(saveJavaRDD.rdd());

        //Raggruppamento in un singolo RDD ordinato
        saveRDD = saveRDD.repartition(1, null);

        try {

            saveRDD.saveAsTextFile(destinationPath + "Query2Spark");

            HBaseConnector.getInstance().SaveQuery2(orderedRdd);

        } catch (Exception e) { e.printStackTrace(); }

        return (endTime-startTime);


    }





    private static long computeQuery2SQL(Dataset<Row> dfSVL, String destinationPath, SparkSession sSession){


        System.out.println("\n\n********************************** QUERY 2 SPARK-SQL ********************************** \n");



        //Registrazione dell'UDF
        String regressionUDF = "regress";
        String top5UDF = "top5";

        registerRegressionUDF(sSession, regressionUDF);
        registerTop5UDF(sSession, top5UDF);




        //Tracciamento del tempo
        long startTime = System.nanoTime();

        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ SOMMINISTRAZIONI VACCINI LATEST ++++++++++++++++++++++\n");
            dfSVL.show(100);
            System.out.println("\n\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }


        //Aggiunta della colonna "anno-mese" per categorizzare le entry in un determinato mese
        Dataset<Row> dfSVLMonth = dfSVL.withColumn( "anno_mese",to_date(date_format(col("data_somministrazione"), "yyyy-LL")));

        //Aggiunta della view sul Dataset appena creato
        dfSVLMonth.createOrReplaceTempView("SVL_month");

        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ SOMMINISTRAZIONI VACCINI LATEST con ANNO-MESE ++++++++++++++++++++++\n");
            dfSVLMonth.show(100);
            System.out.println("\n\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }

        //Raggruppamento delle somministrazioni dei vaccini tra tutte le tipologie dei vaccini con l'aggiunta del campo giorno
        Dataset<Row> dfSVLGrouped = sSession.sql(
                "SELECT area, anno_mese, fascia_anagrafica, DAY(data_somministrazione) as giorno, sum(sesso_femminile) as total " +
                        "FROM SVL_month " +
                        "GROUP BY anno_mese, giorno, area, fascia_anagrafica " +
                        "ORDER BY area, anno_mese, fascia_anagrafica, giorno ");
        //Order By aggiunto per motivazioni di debugging, la sua rimozione comporta un aumento delle performance

        //Aggiunta della view sul Dataset appena creato
        dfSVLGrouped.createOrReplaceTempView("SVL_sum");
        dfSVLGrouped.cache();

        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++ SOMM. VACCINI LATEST con ANNO-MESE, GIORNO e TOT. VACCINI ++++++++++++++++\n");
            dfSVLGrouped.show(100);
            System.out.println("\n\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }



        //Raggruppamento dei giorni e del numero di vaccinazioni in array
        Dataset<Row> dfSVLListed = sSession.sql(
                "SELECT " +
                        "area, " +
                        "anno_mese, " +
                        "fascia_anagrafica, " +
                        "collect_list(giorno) AS list_giorno, " +
                        "collect_list(total) AS list_total " +
                        "FROM SVL_sum " +
                        "GROUP BY anno_mese, area, fascia_anagrafica " +
                        "ORDER BY area, anno_mese, fascia_anagrafica");


        dfSVLListed.cache();

        if(Utils.DEBUG) {
            System.out.println("\n\n+++++++++++++++++++ SOMM. VACCINI LATEST con GIORNO-TOT. VACC. +++++++++++++++++++\n");
            dfSVLListed.show(100);
            System.out.println("\n\nCon n. entry = " + dfSVLListed.count() + "\n");
            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }

        //Filtraggio del dataset levando quelle entry con meno di due vaccinazioni al mese
        dfSVLListed = dfSVLListed.where("cardinality(list_giorno) > 1 AND cardinality(list_total) > 1 ");


        //Aggiunta del valore di regressione
        dfSVLListed = dfSVLListed.withColumn("regressione", callUDF(regressionUDF, col("list_giorno"), col("list_total"), col("anno_mese")));


        //Aggiunta della view sul Dataset appena creato
        dfSVLListed.createOrReplaceTempView("SVL_listed");

        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ SOMM. VACCINI LATEST con REGRESSIONE ++++++++++++++++++++++\n");
            dfSVLListed.show(100);
            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }



        Dataset<Row> dfSVLLeaderboard = sSession.sql(
                "SELECT " +
                        "DATE_FORMAT(add_months(anno_mese, 1), 'yyyy-MMM') as mese, " +
                        "fascia_anagrafica, " +
                        "collect_list(area) as areas, " +
                        "collect_list(regressione) as regressions " +
                        "FROM SVL_listed " +
                        "GROUP BY anno_mese, fascia_anagrafica " +
                        "ORDER BY anno_mese ASC, fascia_anagrafica ASC");



        dfSVLLeaderboard.cache();

        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ LEADERBOARD ++++++++++++++++++++++\n");
            dfSVLLeaderboard.show(100);
            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }

        dfSVLLeaderboard = dfSVLLeaderboard.withColumn("leaderboard", callUDF(top5UDF, col("areas"), col("regressions"))).select("mese", "fascia_anagrafica", "leaderboard");



        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ LEADERBOARD ++++++++++++++++++++++\n");
            dfSVLLeaderboard.show(100, false);
            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }

        long endTime = System.nanoTime();



        System.out.println("\n\n*************************************************************************************** \n");

        try {
            dfSVLLeaderboard.coalesce(1)
                    .write().format("csv")
                    .option("sep", ",")
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .save(destinationPath + "Query2SQL");
        } catch (Exception e) { e.printStackTrace(); }

        return (endTime - startTime);

    }








    //Metodo che registra la UDF per fare la Regressione Lineare
    private static void registerRegressionUDF(SparkSession sSession, String udfName){


        sSession.udf().register(udfName, (UDF3< WrappedArray<Integer>, WrappedArray<Long>, Date, Long>)
                (giorno, valore, data) -> {

                    ArrayList<Integer> x = new ArrayList(JavaConverters.asJavaCollectionConverter(giorno).asJavaCollection());
                    ArrayList<Long> y = new ArrayList(JavaConverters.asJavaCollectionConverter(valore).asJavaCollection());


                    return Utils.regression(x,y,data);

                }, DataTypes.LongType);

    }

    //Metodo che registra la UDF per fare la Regressione Lineare
    private static void registerTop5UDF(SparkSession sSession, String udfName){


        sSession.udf().register(udfName, (UDF2< WrappedArray<String>, WrappedArray<Long>, String>)
                (areaList, regressionList) -> {

                    ArrayList<String> areas = new ArrayList(JavaConverters.asJavaCollectionConverter(areaList).asJavaCollection());
                    ArrayList<Long> regression = new ArrayList(JavaConverters.asJavaCollectionConverter(regressionList).asJavaCollection());



                    //Check per la size del dataset

                    int len = areas.size();

                    if(len != regression.size()){
                        System.out.println("ERRORE: il numero di aree ed il numero di valori regressi non corrisponde\n" +
                                "Verra selezionato il minimo valore.\n");
                    }

                    Utils.bubbleSort(regression, areas);


                    String leaderboard = "";

                    for (int i = 0; i < 5; i++){

                        leaderboard += areas.get(i) + ", " + regression.get(i).toString() + "; ";

                    }

                    return leaderboard;



                }, DataTypes.StringType);

    }
}
