package it.sabd;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Scanner;

public class QueryExecutor {





    //TODO: vedere dalla traccia quando va fatto il filtraggio delle date (filtrare dataset query 3 e verificare che le altre query sono filtrate correttamente)

    public static void main(String[] args) throws InterruptedException {




        SparkSession sSession = SparkSession
                .builder()
                .appName("QueryExecutor").master("local[*]").config("spark.sql.shuffle.partitions", "3")
                .getOrCreate();


        //Load dei dataset

        Dataset<Row> dfSVSL = Utils.getDFSVSL(sSession);
        Dataset<Row> dfPST  = Utils.getDFPST(sSession);
        Dataset<Row> dfSVL = Utils.getDFSVL(sSession);



        //TODO: rimuovere colonne da nifi (provare anche a rimuovere colonna inesistente)

        //Computo della prima query

        Query1.computeQuery1(sSession, dfSVSL, dfPST);

        //Computo della seconda query

        Query2.computeQuery2(sSession, dfSVL);





        System.out.println("\n\nPremere ENTER per interrompere l'esecuzione di Spark\n" +
                "(Per consultare la WebUI di Spark è necessario che Spark sia ancora in running)");

        (new Scanner(System.in)).nextLine();


        sSession.close();
    }
}
