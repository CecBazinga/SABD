package it.sabd;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Date;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class Query2 {


    public static void query2SQL(SparkSession sSession, Dataset<Row> dfSVL){


        System.out.println("\n\n********************************** QUERY 2 SPARK-SQL ********************************** \n");



        //Registrazione dell'UDF
        String udfName = "regress";

        registerRegressionUDF(sSession, udfName);


        //Creazione Dataframe per la seconda query con sorting sulla data
        Dataset<Row> dfSVLQuery2 = dfSVL.withColumn( "data_somministrazione",to_date(date_format(col("data_somministrazione"), "yyyy-LL-dd")));
        dfSVLQuery2 = dfSVLQuery2.sort(col("data_somministrazione")).filter(col("data_somministrazione").geq(lit("2021-02-01")));


        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ SOMMINISTRAZIONI VACCINI LATEST ++++++++++++++++++++++\n");
            dfSVLQuery2.show(100);
            System.out.println("\n\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }


        //Aggiunta della colonna "anno-mese" per categorizzare le entry in un determinato mese
        Dataset<Row> dfSVLQuery2Month = dfSVLQuery2.withColumn( "anno_mese",to_date(date_format(col("data_somministrazione"), "yyyy-LL")));

        //Aggiunta della view sul Dataset appena creato
        dfSVLQuery2Month.createOrReplaceTempView("SVL_month");

        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ SOMMINISTRAZIONI VACCINI LATEST con ANNO-MESE ++++++++++++++++++++++\n");
            dfSVLQuery2Month.show(100);
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

        //Aggiunta della view sul Dataset appena creato
        dfSVLListed.createOrReplaceTempView("SVL_listed");




        if(Utils.DEBUG) {
            System.out.println("\n\n+++++++++++++++++++ SOMM. VACCINI LATEST con GIORNO-TOT. VACC. +++++++++++++++++++\n");
            dfSVLListed.show(100);
            System.out.println("\n\nCon n. entry = " + dfSVLListed.count() + "\n");
            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }

        //Filtraggio del dataset levando quelle entry con meno di due vaccinazioni al mese
        dfSVLListed = dfSVLListed.where("cardinality(list_giorno) > 1 AND cardinality(list_total) > 1 ");


        //Aggiunta del valore di regressione
        dfSVLListed = dfSVLListed.withColumn("regressione", callUDF(udfName, col("list_giorno"), col("list_total"), col("anno_mese")));


        if(Utils.DEBUG) {
            System.out.println("\n\n++++++++++++++++++++++ SOMM. VACCINI LATEST con REGRESSIONE ++++++++++++++++++++++\n");
            dfSVLListed.show(100);
            System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        }


        System.out.println("\n\n*************************************************************************************** \n");

    }



    //Metodo che registra la UDF per fare la Regressione Lineare
    private static void registerRegressionUDF(SparkSession sSession, String udfName){


        sSession.udf().register(udfName, (UDF3< WrappedArray<Integer>, WrappedArray<Long>, Date, Long>)
                (giorno, valore, data) -> {

                    ArrayList<Integer> x = new ArrayList(JavaConverters.asJavaCollectionConverter(giorno).asJavaCollection());
                    ArrayList<Long> y = new ArrayList(JavaConverters.asJavaCollectionConverter(valore).asJavaCollection());

                    SimpleRegression regression = new SimpleRegression();

                    //Check per la size del dataset

                    int len = x.size();

                    if(len != y.size()){
                        System.out.println("+++++++++++++++++++++ERRORE: il numero di giorni ed il numero di vaccinazioni non corrisponde\n" +
                                "Verra selezionato il minimo valore.\n");
                        len = Integer.min(len, y.size());
                    }


                    //Aggiunta dei dati al dataset
                    for(int i = 0; i < len; i++)
                        regression.addData((double) x.get(i), (double) y.get(i));

                    double day = (double) Utils.getDaysPerMonth(data) + 1.0;
                    
                    //Computo della regressione TODO: calcolare numero giorni nei mesi in accordo
                    return (long) regression.predict(day);

                }, DataTypes.LongType);

    }
}
