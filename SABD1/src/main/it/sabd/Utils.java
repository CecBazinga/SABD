package it.sabd;
import java.text.SimpleDateFormat;
import java.util.*;

public class Utils {


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



        List<Integer> months31 = Arrays.asList(3, 5, 8, 10);

        //return (double) Thread.currentThread().getId();

        //LocalDate dateLocal = date.toLocalDate();
        //int month = dateLocal.getMonthValue();

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Paris"));
        cal.setTime(date);
        int month = cal.get(Calendar.MONTH);


        if(month == 1){
            return (monthlyDosesPerCenter/28);
        }
        else if( months31.contains(month)){

            return (monthlyDosesPerCenter/30);
        }
        else {
            return ((monthlyDosesPerCenter/31));
        }

    }




}
