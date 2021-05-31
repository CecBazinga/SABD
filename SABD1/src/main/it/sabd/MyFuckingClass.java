package it.sabd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.immutable.List;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

public class MyFuckingClass {
    //public static String fileLocation = "/media/cecbazinga/Volume/Files/";
    //public static String fileLocation = "hdfs://master:54310/files/";
    public static String fileLocation = "/home/andrea/Scrivania/SABD/Files/";

    public static String filenameSVSL = fileLocation + "SomministrazioneVacciniSummaryLatest.parquet";
    public static String filenamePST  = fileLocation + "PuntiSomministrazioneTipologia.parquet";
    public static String filenameSVL = fileLocation + "SomministrazioneVacciniLatest.parquet";


    public static String outputQueries = fileLocation + "/";

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
                .appName("Query1").master("local[*]").config("spark.sql.shuffle.partitions", "3")
                .getOrCreate();



        // Caricamento files
        Dataset<Row> dfSVSL = sSession.read().format("parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenameSVSL);

        Dataset<Row> dfPST = sSession.read().format("parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenamePST);

        Dataset<Row> dfSVL = sSession.read().format("parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenameSVL);




        //Nel caso in cui sia rilevante il giorno
        //df = df.withColumn( "data_somministrazione",to_date(col("data_somministrazione"), "yyyy-MM-dd"));

        //Per la query 1 non serve sapere il giorno
        Dataset<Row> dfSVSLQuery1 = dfSVSL.withColumn( "data_somministrazione",to_date(date_format(col("data_somministrazione"), "yyyy-LL")));
        dfSVSLQuery1 = dfSVSLQuery1.sort(col("data_somministrazione")).filter(col("data_somministrazione").gt(lit("2020-12-31")));



        //long timeQuery1 = Queries.computeQuery1(dfSVSLQuery1,dfPST,outputQueries);

        //long timeQuerySQL1 = Queries.computeQuery1SQL(dfSVSLQuery1,dfPST,outputQueries, sSession);


        Dataset<Row> dfSVLQuery2 = dfSVL.withColumn( "data_somministrazione",to_date(date_format(col("data_somministrazione"), "yyyy-LL-dd")));
        dfSVLQuery2 = dfSVLQuery2.sort(col("data_somministrazione")).filter(col("data_somministrazione").geq(lit("2021-02-01")));
        Dataset<Row> dfSVLQuery2Month = dfSVLQuery2.withColumn( "anno_mese",to_date(date_format(col("data_somministrazione"), "yyyy-LL")));

        dfSVLQuery2.createOrReplaceTempView("SVL");
        dfSVLQuery2Month.createOrReplaceTempView("SVL_month");

        dfSVLQuery2.show(500);
        dfSVLQuery2Month.show(500);

        Dataset<Row> dfSVLFiltered = sSession.sql("SELECT anno_mese, area, fascia_anagrafica FROM SVL_month GROUP BY area, anno_mese, fascia_anagrafica HAVING COUNT(*) <= 4");

        dfSVLFiltered.createOrReplaceTempView("SVL_filtered");

        dfSVLFiltered.show(100);

        Dataset<Row> dfSVLGrouped = sSession.sql("SELECT data_somministrazione, area, fascia_anagrafica, sum(sesso_femminile) as total FROM SVL GROUP BY data_somministrazione, area, fascia_anagrafica ORDER BY area, data_somministrazione, fascia_anagrafica ");

        dfSVLGrouped.createOrReplaceTempView("SVL_sum");


        //JavaRDD<Tuple3<Date,String,Double>> finalRdd = finalPairRdd.map(x-> new Tuple3<Date, String, Double>(x._1,x._2._1,x._2._2));





        /*
        finalRdd.foreach(x->{
            System.out.println("Printing: " + x._1 + ", " + x._2);
        });
        //dfSVSL.printSchema();
        //dfPST.printSchema();

        //dfSVSL.show(700);


         */



        /*
        JavaRDD<VacciniSummaryLatest> rddVSL = df.toJavaRDD().map(new Function<Row, VacciniSummaryLatest>() {
            @Override
            public VacciniSummaryLatest call(Row row) {
                VacciniSummaryLatest vsl = new VacciniSummaryLatest();
                vsl.setData_somministrazione(row.getDate(0));
                vsl.setArea(row.getString(1));
                vsl.setTotale(row.getInt(2));
                vsl.setSesso_maschile(row.getInt(3));
                vsl.setSesso_femminile(row.getInt(4));
                vsl.setCategoria_operatori_sanitari_sociosanitari(row.getInt(5));
                vsl.setCategoria_personale_non_sanitario(row.getInt(6));
                vsl.setCategoria_ospiti_rsa(row.getInt(7));
                vsl.setCategoria_personale_scolastico(row.getInt(8));
                vsl.setCategoria_60_69(row.getInt(9));
                vsl.setCategoria_70_79(row.getInt(10));
                vsl.setCategoria_over80(row.getInt(11));
                vsl.setCategoria_soggetti_fragili(row.getInt(12));
                vsl.setCategoria_forze_armate(row.getInt(13));
                vsl.setCategoria_altro(row.getInt(14));
                vsl.setPrima_dose(row.getInt(15));
                vsl.setSeconda_dose(row.getInt(16));
                vsl.setCodice_NUTS1(row.getString(17));
                vsl.setCodice_NUTS2(row.getString(18));
                vsl.setCodice_regione_ISTAT(row.getInt(19));
                vsl.setNome_area(row.getString(20));
                return vsl;
            };
        });


         */


        TimeUnit.MINUTES.sleep(10);

        sSession.close();
    }
}
