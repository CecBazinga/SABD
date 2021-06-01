package it.sabd;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.glassfish.jersey.internal.guava.Lists;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;




public class Utils {

    //Usato per fare stampe di Debug
    public static boolean DEBUG = true;


    public static String regionNameConverter(String name){

        String extendedName = null ;
        switch(name){
            case "ABR" :
                extendedName = "Abruzzo";
                break;
            case "BAS" :
                extendedName = "Basilicata";
                break;
            case "CAL" :
                extendedName = "Calabria";
                break;
            case "CAM" :
                extendedName = "Campania";
                break;
            case "EMR" :
                extendedName = "EmiliaRomagna";
                break;
            case "FVG" :
                extendedName = "FriuliVeneziaGiulia";
                break;
            case "LAZ" :
                extendedName = "Lazio";
                break;
            case "LIG" :
                extendedName = "Liguria";
                break;
            case "LOM" :
                extendedName = "Lombardia";
                break;
            case "MAR" :
                extendedName = "Marche";
                break;
            case "MOL" :
                extendedName = "Molise";
                break;
            case "PAB" :
                extendedName = "ProvinciaAutonomaBolzano";
                break;
            case "PAT" :
                extendedName = "ProvinciaAutonomaTrento";
                break;
            case "PIE" :
                extendedName = "Piemonte";
                break;
            case "PUG" :
                extendedName = "Puglia";
                break;
            case "SAR" :
                extendedName = "Sardegna";
                break;
            case "SIC" :
                extendedName = "Sicilia";
                break;
            case "TOS" :
                extendedName = "Toscana";
                break;
            case "UMB" :
                extendedName = "Umbria";
                break;
            case "VDA" :
                extendedName = "ValleD'Aosta";
                break;
            case "VEN" :
                extendedName = "Veneto";
                break;
        }

        return extendedName;
    }



    public static String dateConverter(Date date){

        SimpleDateFormat month_date = new SimpleDateFormat("MMMM-yyyy", Locale.ITALIAN);
        String month_name = month_date.format(date);
        return month_name;
    }



    public static double computeDailyDoses(Date date, double monthlyDosesPerCenter ){

        return monthlyDosesPerCenter/getDaysPerMonth(date);
    }


    public static int getDaysPerMonth(Date date){

        List<Integer> months31 = Arrays.asList(3, 5, 8, 10);

        //return (double) Thread.currentThread().getId();

        //LocalDate dateLocal = date.toLocalDate();
        //int month = dateLocal.getMonthValue();

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Rome"));
        cal.setTime(date);
        int month = cal.get(Calendar.MONTH);


        if(month == 1){
            return 28;
        }
        else if(months31.contains(month)){

            return 30;
        }
        else {
            return 31;
        }

    }

    public static class daysGroupedByMonth implements PairFlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Date, Integer>>>,
            Tuple2<String,String>, List<Tuple2<Date,Integer>> > {

        @Override
        public Iterator<Tuple2<Tuple2<String, String>, List<Tuple2<Date, Integer>>>>
        call(Tuple2<Tuple2<String, String>, Iterable<Tuple2<Date, Integer>>> row) throws Exception {

            String area = row._1._1 ;
            String age = row._1._2 ;

            List<Tuple2<Tuple2<String, String>, List<Tuple2<Date, Integer>>>> results = new ArrayList<>();
            Tuple2<Tuple2<String, String>, List<Tuple2<Date, Integer>>> newRow ;

            List<Tuple2<Date, Integer>> dates = Lists.newArrayList( row._2);
            dates.sort(Comparator.comparing(Tuple2::_1));

            List<Tuple2<Date,Integer>> datesPerMonth = new ArrayList<>();


            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Paris"));
            cal.setTime(dates.get(0)._1);
            int previousMonth = cal.get(Calendar.MONTH);
            int currentMonth = 0;

            //List<Iterable<Tuple2<Date, Integer>>> daysByMonth = new ArrayList<>();
            //Date date =


            for (Tuple2<Date, Integer> date : dates) {

                cal.setTime(date._1);
                currentMonth = cal.get(Calendar.MONTH);

                if (currentMonth==previousMonth){
                    datesPerMonth.add(date);
                    previousMonth = currentMonth ;
                }
                else if(currentMonth!=previousMonth){

                    newRow = new Tuple2<>(new Tuple2<>(area,age),new ArrayList<>(datesPerMonth)) ;
                    results.add(newRow);
                    datesPerMonth.clear();
                    datesPerMonth.add(date);
                    previousMonth = currentMonth ;

                }
            }

            newRow = new Tuple2<>(new Tuple2<>(area,age),new ArrayList<>(datesPerMonth)) ;
            results.add(newRow);


            return results.iterator();

        }
    }

}
