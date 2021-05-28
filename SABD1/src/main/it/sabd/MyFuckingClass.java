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
    public static String filenameSVSL = "/media/cecbazinga/Volume/Files/SomministrazioneVacciniSummaryLatest.parquet";
    public static String filenamePST  = "/media/cecbazinga/Volume/Files/PuntiSomministrazioneTipologia.parquet";
    public static String filenameSVL = "/media/cecbazinga/Volume/Files/SomministrazioneVacciniLatest.parquet";

    public static String outputQueries = "/media/cecbazinga/Volume/Files/Queries/";

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
                .appName("Query1").master("local[*]")
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
        dfSVSL = dfSVSL.withColumn( "data_somministrazione",to_date(date_format(col("data_somministrazione"), "yyyy-LL")));
        dfSVSL = dfSVSL.sort(col("data_somministrazione")).filter(col("data_somministrazione").gt(lit("2020-12-31")));




        long time1 = Queries.computeQuery1(dfSVSL,dfPST,outputQueries);




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


        //TimeUnit.MINUTES.sleep(10);

        sSession.close();
    }
}
