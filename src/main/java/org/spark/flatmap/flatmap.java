package org.spark.flatmap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class flatmap {
    public static void main(String ars[]) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Flat Mapping Test !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        String path = "/home/cbnits-51/Downloads/Iris.csv";
        JavaRDD<String> lines = sc.textFile(path);
        System.out.println("Total Number Of Lines =" + lines.count());
        JavaRDD<List<String>> mapLine = lines.map(line -> List.of(line.split(",")));
        mapLine.take(5).forEach(System.out::println);
        System.out.println("Flat Mapping");
        JavaRDD<String> flatMapLine = lines.flatMap(line -> List.of(line.split(",")).iterator());
        flatMapLine.take(20).forEach(System.out::println);
//        map() transforms each element independently and maintains the same structure.
//        flatMap() transforms each element and can produce multiple output elements, which are then flattened into a single RDD
    }

}
