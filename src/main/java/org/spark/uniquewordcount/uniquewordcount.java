package org.spark.uniquewordcount;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class uniquewordcount {
    public static void main(String ars[]) {
        SparkSession spark = SparkSession.builder()
                .appName("Unique word count !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        String path = "/home/cbnits-51/Desktop/sayan/test.txt";
        JavaRDD<String> uniquewordcount = sc.textFile(path).map(line -> line.replaceAll("[^a-zA-z\\s]", "").toLowerCase())
                .flatMap(line -> List.of(line.split("\\s")).iterator())
                .filter(word -> (word != null) && word.trim().length() > 0);
//        uniquewordcount.collect().forEach(System.out::println);
        JavaPairRDD<String, Long> pairrdd = uniquewordcount.mapToPair(line -> new Tuple2<>(line, 1L));

        JavaPairRDD<String, Long> unique = pairrdd.reduceByKey(Long::sum);
        AtomicInteger count = new AtomicInteger(0);
        unique.take(8).forEach(System.out::println);
        //Here unique.foreach does not works because spark is lazy evaluation.
        unique.collect().forEach(line -> {
            if (line._2 == 1) {
                count.incrementAndGet();
            }
        });
        System.out.println("Unique word " + count.get());
        System.out.println("Total word " + uniquewordcount.count());
        uniquewordcount.take(50).forEach(System.out::println);
        JavaRDD<String> distinct = uniquewordcount.distinct();
        System.out.println("Distinct word " + distinct.count());
        distinct.take(50).forEach(System.out::println);

    }
}
