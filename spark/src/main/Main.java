package main;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class Main {

    public static int NUM_SAMPLES = 10000;

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1;
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);

        System.out.println("Ciao");

    }
}
