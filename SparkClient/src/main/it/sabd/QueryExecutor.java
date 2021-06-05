package it.sabd;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class QueryExecutor {


    public static void main(String[] args) throws InterruptedException {


        SparkSession sSession = SparkSession
                .builder()
                .appName("QueryExecutor").master("yarn").config("spark.sql.shuffle.partitions", "3")
                .getOrCreate();


        //Load dei dataset
        Dataset<Row> dfSVSL = Utils.getDFSVSL(sSession);
        Dataset<Row> dfPST  = Utils.getDFPST(sSession);
        Dataset<Row> dfSVL = Utils.getDFSVL(sSession);
        Dataset<Row> dfTP = Utils.getDFTP(sSession);

        //Computo della prima query
        Query1.computeQuery1(sSession, dfSVSL, dfPST);

        //Computo della seconda query
        Query2.computeQuery2(sSession, dfSVL);

        //Computo della terza query
        Query3.computeQuery3(dfSVSL, dfTP, sSession);


        TimeUnit.SECONDS.sleep(5);

        System.out.println("\n\nPremere ENTER per interrompere l'esecuzione di Spark\n" +
                "(Per consultare la WebUI di Spark è necessario che Spark sia ancora in running)");

        (new Scanner(System.in)).nextLine();


        sSession.close();
    }
}
