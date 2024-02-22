package org.spark.pairrdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class reducebykey {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("PairRdd !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        String path="/home/cbnits-51/Downloads/Iris.csv";
        JavaRDD<String> myrdd = sc.textFile(path);
        JavaPairRDD<Integer, Long> pairrdd = myrdd.mapToPair(line -> new Tuple2<>(line.length(), 1L));
        JavaPairRDD<Integer, Long> counts = pairrdd.reduceByKey(Long::sum);
        counts.take(5).forEach(System.out::println);

//        counts.take(5).forEach(tuple->{
//            System.out.println(tuple._1+" ->"+tuple._2);
//        });

    }
}
