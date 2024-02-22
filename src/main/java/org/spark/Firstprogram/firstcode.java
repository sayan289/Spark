package org.spark.Firstprogram;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class firstcode {
    public static void main(String[] args) {

            SparkSession spark = SparkSession.builder()
                    .appName("My First Spark Code !!")
                    .master("local[*]")
                    .getOrCreate();
            JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
//           List<Integer>data= Stream.iterate(1, n->n+1).limit(5)
//                   .collect(Collectors.toList());
//            JavaRDD<Integer>myRdd= sc.parallelize(data);
//            System.out.println("Total Elements in Rdd = "+myRdd.count());
//        System.out.println("Default Number of partition in Rdd = "+myRdd.getNumPartitions());
//        int max=myRdd.reduce(Integer::max);
//        int min=myRdd.reduce(Integer::min);
//        int sum=myRdd.reduce((Integer::sum));
//        System.out.println(max +" "+min+" "+sum);
//        Scanner sc1=new Scanner(System.in);
//        System.out.println("Enter a to exit:");
//        String p=sc1.next();
        String path="/home/cbnits-51/Downloads/Iris.csv";
        Dataset<Row> df = spark.read().csv(path);
        df.show(Integer.MAX_VALUE);
        Scanner sc1=new Scanner(System.in);
        System.out.println("Enter a to exit:");
        String p=sc1.next();
    }
}
