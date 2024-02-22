package org.spark.readcsv;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class readcsv {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("My First Spark Code !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        String path="/home/cbnits-51/Downloads/Iris.csv";
        Dataset<Row> df = spark.read().csv(path);
        df.show(Integer.MAX_VALUE);
        Scanner sc1=new Scanner(System.in);
        System.out.println("Enter a to exit:");
        String p=sc1.next();
    }
}
