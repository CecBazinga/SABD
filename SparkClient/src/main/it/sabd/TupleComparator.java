package it.sabd;

import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Comparator;



//Classe per comparare le tuple per la Query 2

public class TupleComparator<Date,Long> implements Comparator<Tuple3<Date,Long,Long>>, Serializable {

    private Comparator<Date> comparatorDates;
    private Comparator<Long> comparatorLong;


    public TupleComparator(Comparator<Date> comparatorDates,Comparator<Long> comparatorLong) {
        this.comparatorDates = comparatorDates;
        this.comparatorLong = comparatorLong;
    }

    @Override
    public int compare(Tuple3<Date, Long,Long> o1, Tuple3<Date, Long,Long> o2) {
        if (o1._1() == o2._1()){
            if (o1._2() == o2._2()){
                return comparatorLong.compare(o1._3(), o2._3());
            }
            return comparatorLong.compare(o1._2(), o2._2());
        }
        return comparatorDates.compare(o1._1(),o2._1());
    }
}


