package org.spark.tuple;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class tuple {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("Tuple !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        String path="/home/cbnits-51/Downloads/Iris.csv";
        JavaRDD<String> myrdd = sc.textFile(path);
        JavaRDD<Tuple2<String, Integer>> tuple2javardd = myrdd.map(line -> new Tuple2<>(line, line.length()));
        System.out.println(myrdd.count());
        System.out.println(tuple2javardd.count());
        myrdd.foreach(x->{
            System.out.println(x);
        });
        System.out.println("--------------------------------");
        tuple2javardd.take(10).forEach(System.out::println);
    }
}
