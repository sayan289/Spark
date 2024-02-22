package org.spark.join;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class join {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("All type of join!!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        JavaPairRDD<Integer,String>customerpair;
        
        List<Tuple2<Integer,String>> customer=new ArrayList<>();
        List<Tuple2<Integer,Double>> bill=new ArrayList<>();
        customer.add(new Tuple2<>(1,"sayan"));
        customer.add(new Tuple2<>(2,"subhajit"));
        customer.add(new Tuple2<>(3,"Arghay"));
        bill.add(new Tuple2<>(8,150.50D));
        bill.add(new Tuple2<>(2,199.9D));
        bill.add(new Tuple2<>(3,120.20D));
        customerpair=sc.parallelizePairs(customer);
        JavaPairRDD<Integer, Double> billpair = sc.parallelizePairs(bill);
        JavaPairRDD<Integer, Tuple2<String, Double>> innerjoinrdd = customerpair.join(billpair);
        System.out.println("Inner join");
        innerjoinrdd.collect().forEach(System.out::println);
//        System.out.println("----------------------------------");
//        innerjoinrdd.collect().forEach(j->{
//            System.out.println(j._2._1+" "+j._2._2);
//        });
        System.out.println("-------------------------");
//        Leftouter join
        JavaPairRDD<Integer, Tuple2<String, Optional<Double>>> leftOuterJoin = customerpair.leftOuterJoin(billpair);
        System.out.println("Leftouter join");
        leftOuterJoin.collect().forEach(System.out::println);
        System.out.println("-------------------------");
//        Rightouter join
        JavaPairRDD<Integer, Tuple2<Optional<String>, Double>> rightouterjoin = customerpair.rightOuterJoin(billpair);
        System.out.println("RightOuter join");
        rightouterjoin.collect().forEach(System.out::println);
        JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Double>>> fullouterjoin = customerpair.fullOuterJoin(billpair);
        System.out.println("------------------------------------");;
        System.out.println("Full outer join");
        fullouterjoin.collect().forEach(System.out::println);
        System.out.println("------------------------");
        System.out.println("Cartesian ");
        JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, Double>> cartesian = customerpair.cartesian(billpair);
        cartesian.collect().forEach(System.out::println);
    }
}
