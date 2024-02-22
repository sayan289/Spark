package org.spark.pairrdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class pairrdd {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("PairRdd !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        String path="/home/cbnits-51/Downloads/Iris.csv";
        JavaRDD<String> myrdd = sc.textFile(path);
        JavaRDD<Tuple2<Integer, String>> pairrdd = myrdd.map(line -> new Tuple2<>(line.length(), line));
        pairrdd.take(10).forEach(System.out::println);
    }
}
