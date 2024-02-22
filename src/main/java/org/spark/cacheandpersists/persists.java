package org.spark.cacheandpersists;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;

public class persists {
    public static void main(String ars[]) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Cache and persists !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        String path="/home/cbnits-51/Downloads/Iris.csv";
        JavaRDD<String> myrdd = sc.textFile(path);
        JavaPairRDD<Integer, Long> pairrdd = myrdd.mapToPair(line -> new Tuple2<>(line.length(), 1L));
        JavaPairRDD<Integer, Iterable<Long>> counts = pairrdd.groupByKey().persist(StorageLevel.MEMORY_AND_DISK());
        counts.take(5).forEach(line->{
            System.out.println(line._1+" "+ Iterables.size(line._2));
        });
    }
}
