package it.sabd;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

public class MyFuckingClass {
    //public static String fileLocation = "/media/cecbazinga/Volume/Files/";
    //public static String fileLocation = "hdfs://master:54310/files/";
    //public static String fileLocation = "/home/andrea/Scrivania/SABD/Files/";
    public static String fileLocation = "/Users/andreapaci/Desktop/SABD/Files/";

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


        Query2.query2SQL(sSession, dfSVL);










        /*Dataset<Row> areas = dfSVLGrouped.select(col("area")).dropDuplicates();
        areas.show();

        Dataset<Row> annoMesi = dfSVLGrouped.select(col("anno_mese")).dropDuplicates();
        annoMesi.show();

        Dataset<Row> fasceAnagrafiche = dfSVLGrouped.select(col("fascia_anagrafica")).dropDuplicates();
        fasceAnagrafiche.show();

        Dataset<Row> dfSVLList = dfSVLGrouped.select(col("area"), col("anno_mese"), col("fascia_anagrafica")).dropDuplicates();

        dfSVLList.show(100);

        //TODO: aggiungere compilazione maven, quando fai regressione ricordati che devi calcolare il giorno dopo a quello della grandezza del mese, levare static ai metodi, vedere se va fatto sparkconf, metti HBASE,
        //TODO: check che alcuni valori della regressione hanno valore negativo (UPDATE, prendendo quei valori e graficandoli ci sta che vanno in negativo, mettere il limite che se è < 0, allora = 0

        SimpleRegression regression = new SimpleRegression();


        for(Row area : areas.collectAsList()) {

            System.out.println(area.get(0).toString());

            for(Row annoMese: annoMesi.collectAsList()){

                for(Row fasciaAnagrafica: fasceAnagrafiche.collectAsList()){





                }

            }

        }*/


        //JavaRDD<Tuple3<Date,String,Double>> finalRdd = finalPairRdd.map(x-> new Tuple3<Date, String, Double>(x._1,x._2._1,x._2._2));


        //Dataset<Row> resultQuery2 = dfSVLGrouped.withColumn( "regression", lr.fit(dfSVLGrouped.select(col("giorno"), col("total"))));

        //inearRegressionModel lrModel = lr.fit(training);

// Print the coefficients and intercept for linear regression.




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
