package it.sabd;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.glassfish.jersey.internal.guava.Lists;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;




public class Utils {

    //Usato per fare stampe di Debug (incrementa di molto i tempi
    public static boolean DEBUG = true;

    //Location dei file

    //public static String fileLocation = "/media/cecbazinga/Volume/Files/";
    public static String fileLocation = "hdfs://master:54310/files/";
    //public static String fileLocation = "/home/andrea/Scrivania/SABD/Files/";
    //public static String fileLocation = "/Users/andreapaci/Desktop/SABD/Files/";

    public static String filenameSVSL = fileLocation + "SomministrazioneVacciniSummaryLatest.parquet";
    public static String filenamePST = fileLocation + "PuntiSomministrazioneTipologia.parquet";
    public static String filenameSVL = fileLocation + "SomministrazioneVacciniLatest.parquet";


    public static String outputQueries = fileLocation + "/Results/";


    public static double nanosecondsInSeconds = 1000000000.0;


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

        //return (double) Thread.currentThread().getId();

        //LocalDate dateLocal = date.toLocalDate();
        //int month = dateLocal.getMonthValue();

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Rome"));
        cal.setTime(date);
        int month = cal.get(Calendar.MONTH);


        if (month == 1) {
            return 28;
        } else if (months31.contains(month)) {

            return 30;
        } else {
            return 31;
        }

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


    public static long regression(ArrayList<Integer> x, ArrayList<Long> y, Date data) {

        SimpleRegression regression = new SimpleRegression();

        //Check per la size del dataset

        int len = x.size();

        if (len != y.size()) {
            System.out.println("+++++++++++++++++++++ERRORE: il numero di giorni ed il numero di vaccinazioni non corrisponde\n" +
                    "Verra selezionato il minimo valore.\n");
            len = Integer.min(len, y.size());
        }



        //Aggiunta dei dati al dataset
        for (int i = 0; i < len; i++)
            regression.addData((double) x.get(i), (double) y.get(i));

        double day = (double) Utils.getDaysPerMonth(data) + 1.0;

        //Computo della regressione TODO: calcolare numero giorni nei mesi in accordo
        return (long) regression.predict(day);

    }

    public static long applyRegression(List<Tuple2<Date,Long>> list){

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


    public static long ageStringToLong(String string) {

        String newString = string.substring(0, 2) + string.substring(3);

        return Long.valueOf(newString);

    }


    public static List<Tuple2<String, Long>> iterableToListTop5(Iterable<Tuple2<String, Long>> iterable) {

        List<Tuple2<String, Long>> sortedList = new ArrayList<>();
        for (Tuple2<String, Long> tuple : iterable) {
            sortedList.add(tuple);
        }
        sortedList.sort(Comparator.comparing(Tuple2::_2, Comparator.reverseOrder()));

        return sortedList.subList(0, 5);

    }


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


            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Paris"));
            cal.setTime(dates.get(0)._1);
            int previousMonth = cal.get(Calendar.MONTH);
            int currentMonth = 0;

            //List<Iterable<Tuple2<Date,Long>>> daysByMonth = new ArrayList<>();
            //Date date =


            for (Tuple2<Date, Long> date : dates) {

                cal.setTime(date._1);
                currentMonth = cal.get(Calendar.MONTH);

                if (currentMonth == previousMonth) {
                    datesPerMonth.add(date);
                    previousMonth = currentMonth;
                } else if (currentMonth != previousMonth) {

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


    //E' necessario fare il parsing più di una volta dei file per evitare modifiche per referenza quando si fanno le query

    //Funzione per il retrive di dataframe per SVSL
    public static Dataset<Row> getDFSVSL(SparkSession sSession) {

        return sSession.read().format("parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenameSVSL);
    }

    //Funzione per il retrive di dataframe per PST
    public static Dataset<Row> getDFPST(SparkSession sSession) {

        return sSession.read().format("parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenamePST);
    }

    //Funzione per il retrive di dataframe per SVL
    public static Dataset<Row> getDFSVL(SparkSession sSession) {

        return sSession.read().format("parquet")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filenameSVL);
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

}


