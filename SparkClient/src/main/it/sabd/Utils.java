package it.sabd;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.glassfish.jersey.internal.guava.Lists;
import scala.Tuple2;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



public class Utils {

    //Usato per fare stampe di Debug (incrementa di molto i tempi)
    public static boolean DEBUG = true;

    //Location dei file
    public static String fileLocation = "hdfs://master:54310/files/";

    public static String filenameSVSL = fileLocation + "SomministrazioneVacciniSummaryLatest.parquet";
    public static String filenamePST = fileLocation + "PuntiSomministrazioneTipologia.parquet";
    public static String filenameSVL = fileLocation + "SomministrazioneVacciniLatest.parquet";
    public static String filenameTP = fileLocation + "TotalePopolazione.parquet";

    public static String outputQueries = fileLocation + "/Results/";

    public static double nanosecondsInSeconds = 1000000000.0;






    //Funzione per il retrive di dataframe per SVSL
    public static Dataset<Row> getDFSVSL(SparkSession sSession) {

        return sSession.read().format("org.apache.spark.sql.execution.datasources.parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenameSVSL)
                .drop("UNNAMED_FIELD");
    }

    //Funzione per il retrive di dataframe per PST
    public static Dataset<Row> getDFPST(SparkSession sSession) {

        return sSession.read().format("org.apache.spark.sql.execution.datasources.parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenamePST)
                .drop("UNNAMED_FIELD");
    }

    //Funzione per il retrive di dataframe per SVL
    public static Dataset<Row> getDFSVL(SparkSession sSession) {

        return sSession.read().format("org.apache.spark.sql.execution.datasources.parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenameSVL)
                .drop("UNNAMED_FIELD");
    }

    //Funzione per il retrive di dataframe per totale popolazione stimata per regione
    public static Dataset<Row> getDFTP(SparkSession sSession) {

        return sSession.read().format("org.apache.spark.sql.execution.datasources.parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenameTP);
    }

    //Ordinare array "values" ordinando rispettivamente anche "labels"
    public static void bubbleSort(ArrayList<Long> values, ArrayList<String> labels) {

        boolean sorted = false;

        long tempValue;
        String tempLabel;

        while (!sorted) {
            sorted = true;
            for (int i = 0; i < values.size() - 1; i++) {
                if (values.get(i) < values.get(i + 1)) {

                    tempValue = values.get(i);
                    values.set(i, values.get(i + 1));
                    values.set(i + 1, tempValue);

                    tempLabel = labels.get(i);
                    labels.set(i, Utils.regionNameConverter(labels.get(i + 1)));
                    labels.set(i + 1, Utils.regionNameConverter(tempLabel));


                    sorted = false;
                }
            }
        }
    }


    //Applicazione regressione ad una lista di tuple
    public static long applyRegression(List<Tuple2<Date, Long>> list){

        ArrayList<Integer> x = new ArrayList();
        ArrayList<Long> y = new ArrayList();

        Calendar calendar = Calendar.getInstance();


        for (Tuple2<Date,Long> element : list){

            calendar.setTime(element._1);
            x.add(calendar.get(Calendar.DAY_OF_MONTH));
            y.add(element._2);

        }

        return Utils.regression(x,y,list.get(0)._1);

    }

    //Regressione lineare
    public static long regression(ArrayList<Integer> x, ArrayList<Long> y, Date data) {

        SimpleRegression regression = new SimpleRegression();

        //Check per la size del dataset

        int len = x.size();

        if (len != y.size()) {
            System.out.println("ERRORE: il numero di giorni ed il numero di vaccinazioni non corrisponde\n" +
                    "Verra selezionato il minimo valore.\n");
            len = Integer.min(len, y.size());
        }



        //Aggiunta dei dati al dataset
        for (int i = 0; i < len; i++)
            regression.addData((double) x.get(i), (double) y.get(i));

        double day = (double) Utils.getDaysPerMonth(data) + 1.0;

        //Computo della regressione
        return (long) regression.predict(day);

    }

    //Regressione per la Query 3
    public static long totalRegression(Iterable<Tuple2<Date,Long>> iterable) {

        SimpleRegression regression = new SimpleRegression();


        ArrayList<Tuple2<Date,Long>> orderedList = new ArrayList();

        for(Tuple2<Date,Long> element : iterable){

            orderedList.add(element);
        }

        orderedList.sort(Comparator.comparing(Tuple2::_1));

        //Applico la regressione passando al posto delle date il valore intero dell'indice corrispondente della lista e
        // come long il valore dei vaccini
        int i;
        for (i = 0; i < orderedList.size(); i++) {
            regression.addData(i, (double) orderedList.get(i)._2);
        }

        //Computo della regressione
        return (long) regression.predict(i);

    }


    public static Tuple2<double[], JavaPairRDD<String, Tuple2<String, String>>> applyKMeans(JavaPairRDD<String, Double> rdd, int minClusterNumber, int maxClusterNumber){

        JavaRDD<Double> doubleRdd = rdd.map(x-> x._2);
        JavaRDD<Vector> parsedData = doubleRdd.map(Vectors::dense);
        JavaPairRDD<String, Tuple2<String, String>> kMeansResults = null;

        //Cluster dei dati usando KMEANS
        int numIterations = 20;
        int numClusters;

        RDD<Vector> parseDataRDD = parsedData.rdd();

        //Array per le tempistiche dei singoli clustering al variare di K
        final int arraySize = maxClusterNumber - minClusterNumber + 1;
        double timeKmeans[] = new double[arraySize];



        for(numClusters = minClusterNumber; numClusters <= maxClusterNumber; numClusters++) {

            //Tracciamento del tempo
            timeKmeans[numClusters - minClusterNumber] = System.nanoTime();

            //Training
            KMeansModel clusters = KMeans.train(parseDataRDD, numClusters, numIterations);

            int finalNumClusters = numClusters;

            //Calcolo del WSSSE
            String cost = String.format("%.2f", clusters.computeCost(parseDataRDD)).replace(",", ".");


            //Predizione
            JavaPairRDD<String, Tuple2<String, String>> predictionRDD = rdd.mapToPair(x -> new Tuple2<>("[" + Integer.toString(clusters.predict(Vectors.dense(x._2))) + "]" + x._1 , new Tuple2<>(cost , Integer.toString(finalNumClusters))));


            //Fine tracciamento del tempo
            timeKmeans[numClusters - minClusterNumber] = (System.nanoTime() - timeKmeans[numClusters - minClusterNumber]) / Utils.nanosecondsInSeconds;



            predictionRDD = predictionRDD.sortByKey();
            predictionRDD = predictionRDD.mapToPair(x -> new Tuple2<>(x._2._2, new Tuple2<>(x._2._1 , x._1)));


            JavaPairRDD<String, Tuple2<String, String>> orderedRdd = predictionRDD.reduceByKey((x, y) -> new Tuple2<>(x._1, x._2 + "-" + y._2));

            if(kMeansResults == null) kMeansResults = orderedRdd;
            else kMeansResults = kMeansResults.union(orderedRdd);

            kMeansResults.cache();

        }


        kMeansResults = kMeansResults.sortByKey();


        return new Tuple2<>(timeKmeans, kMeansResults);

    }



    public static Tuple2<double[], JavaPairRDD<String, Tuple2<String, String>>> applyBisectingKMeans(JavaPairRDD<String, Double> rdd, int minClusterNumber, int maxClusterNumber){

        JavaRDD<Double> doubleRdd = rdd.map(x-> x._2);
        JavaRDD<Vector> parsedData = doubleRdd.map(Vectors::dense);
        JavaPairRDD<String, Tuple2<String, String>> bKMeansResults = null;


        //Cluster dei dati usando BSECTING KMEANS
        int numIterations = 20;
        int numClusters;

        RDD<Vector> parseDataRDD = parsedData.rdd();

        //Array per le tempistiche dei singoli clustering al variare di K
        final int arraySize = maxClusterNumber - minClusterNumber + 1;
        double timeKmeans[] = new double[arraySize];



        for(numClusters = minClusterNumber; numClusters <= maxClusterNumber; numClusters++) {

            //Tracciamento del tempo
            timeKmeans[numClusters - minClusterNumber] = System.nanoTime();


            //Training
            BisectingKMeans bKMeans = new BisectingKMeans().setK(numClusters)
                    .setMaxIterations(numIterations)
                    .setSeed(1L);
            BisectingKMeansModel bKMClusters = bKMeans.run(parseDataRDD);

            int finalNumClusters = numClusters;

            //Calcolo del WSSSE
            String cost = String.format("%.2f", bKMClusters.computeCost(parseDataRDD)).replace(",", ".");

            //Predizione
            JavaPairRDD<String, Tuple2<String, String>> predictionRDD = rdd.mapToPair(x -> new Tuple2<>("[" + Integer.toString(bKMClusters.predict(Vectors.dense(x._2))) + "]" + x._1 , new Tuple2<>(cost , Integer.toString(finalNumClusters))));


            //Fine tracciamento del tempo
            timeKmeans[numClusters - minClusterNumber] = (System.nanoTime() - timeKmeans[numClusters - minClusterNumber]) / Utils.nanosecondsInSeconds;


            predictionRDD = predictionRDD.sortByKey();
            predictionRDD = predictionRDD.mapToPair(x -> new Tuple2<>(x._2._2, new Tuple2<>(x._2._1 , x._1)));


            JavaPairRDD<String, Tuple2<String, String>> orderedRdd = predictionRDD.reduceByKey((x,y) -> new Tuple2<>(x._1, x._2 + "-" + y._2));

            if(bKMeansResults == null) bKMeansResults = orderedRdd;
            else bKMeansResults = bKMeansResults.union(orderedRdd);

            bKMeansResults.cache();

        }


        bKMeansResults = bKMeansResults.sortByKey();


        return new Tuple2<>(timeKmeans, bKMeansResults);

    }


    //Ottiene la Top % da un Iterable
    public static List<Tuple2<String, Long>> iterableToListTop5(Iterable<Tuple2<String, Long>> iterable) {

        List<Tuple2<String, Long>> sortedList = new ArrayList<>();
        for (Tuple2<String, Long> tuple : iterable) {
            sortedList.add(tuple);
        }
        sortedList.sort(Comparator.comparing(Tuple2::_2, Comparator.reverseOrder()));

        return sortedList.subList(0, 5);

    }


    //Fornita in input la lista dei giorni restituisce le liste dei giorni raggruppati per mese
    public static class daysGroupedByMonth implements PairFlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Date, Long>>>,
            Tuple2<String, String>, List<Tuple2<Date, Long>>> {

        @Override
        public Iterator<Tuple2<Tuple2<String, String>, List<Tuple2<Date, Long>>>>
        call(Tuple2<Tuple2<String, String>, Iterable<Tuple2<Date, Long>>> row) throws Exception {

            String area = row._1._1;
            String age = row._1._2;

            List<Tuple2<Tuple2<String, String>, List<Tuple2<Date, Long>>>> results = new ArrayList<>();
            Tuple2<Tuple2<String, String>, List<Tuple2<Date, Long>>> newRow;

            List<Tuple2<Date, Long>> dates = Lists.newArrayList(row._2);
            dates.sort(Comparator.comparing(Tuple2::_1));

            List<Tuple2<Date, Long>> datesPerMonth = new ArrayList<>();


            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Rome"));
            cal.setTime(dates.get(0)._1);
            int previousMonth = cal.get(Calendar.MONTH);
            int currentMonth = 0;


            for (Tuple2<Date, Long> date : dates) {

                cal.setTime(date._1);
                currentMonth = cal.get(Calendar.MONTH);

                if (currentMonth == previousMonth) {
                    datesPerMonth.add(date);
                    previousMonth = currentMonth;
                } else {

                    newRow = new Tuple2<>(new Tuple2<>(area, age), new ArrayList<>(datesPerMonth));
                    results.add(newRow);
                    datesPerMonth.clear();
                    datesPerMonth.add(date);
                    previousMonth = currentMonth;

                }
            }

            newRow = new Tuple2<>(new Tuple2<>(area, age), new ArrayList<>(datesPerMonth));
            results.add(newRow);


            return results.iterator();

        }
    }



    public static String dateConverter(Date date) {

        SimpleDateFormat month_date = new SimpleDateFormat("MMMM-yyyy", Locale.ITALIAN);
        String month_name = month_date.format(date);
        return month_name;
    }

    public static double computeDailyDoses(Date date, double monthlyDosesPerCenter) {

        return monthlyDosesPerCenter / getDaysPerMonth(date);
    }

    public static int getDaysPerMonth(Date date) {

        List<Integer> months31 = Arrays.asList(3, 5, 8, 10);

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Rome"));
        cal.setTime(date);
        int month = cal.get(Calendar.MONTH);

        if (month == 1) { return 28; }
        else if (months31.contains(month)) { return 30; }
        return 31;

    }

    //Metodo per trovare il primo giorno successivo del mese
    public static String firstDayNextMonth(Date data) {

        Calendar today = Calendar.getInstance();
        today.setTime(data);

        Calendar next = Calendar.getInstance();
        next.set(Calendar.YEAR, today.get(Calendar.YEAR));
        next.set(Calendar.MONTH, today.get(Calendar.MONTH) + 1);
        next.set(Calendar.DAY_OF_MONTH, 1);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ITALIAN);

        return sdf.format(next.getTime());

    }

    public static String regionNameConverter(String name) {

        String extendedName = name;
        switch (name) {
            case "ABR":
                extendedName = "Abruzzo";
                break;
            case "BAS":
                extendedName = "Basilicata";
                break;
            case "CAL":
                extendedName = "Calabria";
                break;
            case "CAM":
                extendedName = "Campania";
                break;
            case "EMR":
                extendedName = "EmiliaRomagna";
                break;
            case "FVG":
                extendedName = "FriuliVeneziaGiulia";
                break;
            case "LAZ":
                extendedName = "Lazio";
                break;
            case "LIG":
                extendedName = "Liguria";
                break;
            case "LOM":
                extendedName = "Lombardia";
                break;
            case "MAR":
                extendedName = "Marche";
                break;
            case "MOL":
                extendedName = "Molise";
                break;
            case "PAB":
                extendedName = "ProvinciaAutonomaBolzano";
                break;
            case "PAT":
                extendedName = "ProvinciaAutonomaTrento";
                break;
            case "PIE":
                extendedName = "Piemonte";
                break;
            case "PUG":
                extendedName = "Puglia";
                break;
            case "SAR":
                extendedName = "Sardegna";
                break;
            case "SIC":
                extendedName = "Sicilia";
                break;
            case "TOS":
                extendedName = "Toscana";
                break;
            case "UMB":
                extendedName = "Umbria";
                break;
            case "VDA":
                extendedName = "ValleD'Aosta";
                break;
            case "VEN":
                extendedName = "Veneto";
                break;
        }

        return extendedName;
    }


    public static String regionNameReducer(String name) {

        String reducedName = name;
        if (name.contains("Valle d'Aosta")){
            name = "Valle d'Aosta";
        }
        switch (name) {
            case "Abruzzo":
                reducedName = "ABR";
                break;
            case "Basilicata":
                reducedName = "BAS";
                break;
            case "Calabria":
                reducedName = "CAL";
                break;
            case "Campania":
                reducedName = "CAM";
                break;
            case "Emilia-Romagna":
                reducedName = "EMR";
                break;
            case "Friuli-Venezia Giulia":
                reducedName = "FVG";
                break;
            case "Lazio":
                reducedName = "LAZ";
                break;
            case "Liguria":
                reducedName = "LIG";
                break;
            case "Lombardia":
                reducedName = "LOM";
                break;
            case "Marche":
                reducedName = "MAR";
                break;
            case "Molise":
                reducedName = "MOL";
                break;
            case "Provincia Autonoma Bolzano / Bozen":
                reducedName = "PAB";
                break;
            case "Provincia Autonoma Trento":
                reducedName = "PAT";
                break;
            case "Piemonte":
                reducedName = "PIE";
                break;
            case "Puglia":
                reducedName = "PUG";
                break;
            case "Sardegna":
                reducedName = "SAR";
                break;
            case "Sicilia":
                reducedName = "SIC";
                break;
            case "Toscana":
                reducedName = "TOS";
                break;
            case "Umbria":
                reducedName = "UMB";
                break;
            case "Valle d'Aosta":
                reducedName = "VDA";
                break;
            case "Veneto":
                reducedName = "VEN";
                break;
        }

        return reducedName;
    }




}


