package org.spark.filter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class filter {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("My First Spark Code !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        String path = "/home/cbnits-51/Downloads/Iris.csv";
        JavaRDD<String> lines = sc.textFile(path);
        //System.out.println("Flat Mapping");
        JavaRDD<String> flatMapLine = lines.flatMap(line -> List.of(line.split(",")).iterator());
        //flatMapLine.take(20).forEach(System.out::println);
        System.out.println(flatMapLine.count());
        JavaRDD<String> filter = flatMapLine.filter(word -> (word != null) && (word.trim().length() > 3));
        System.out.println("After filter "+filter.count());
        //Filter any RDD based on any condition. and create a new RDD.
        flatMapLine.take(10).forEach(System.out::println);//It brings some data into driven and then by using java forEach method print that

    }
}
