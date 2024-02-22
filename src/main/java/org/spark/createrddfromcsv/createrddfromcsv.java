package org.spark.createrddfromcsv;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class createrddfromcsv {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("Create Rdd from csv!!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        String path="/home/cbnits-51/Downloads/Iris.csv";
        JavaRDD<String> myrdd=sc.textFile(path);
        System.out.println("Header ");
        System.out.println(myrdd.first());
        System.out.println("--------------------");
        System.out.println("First 10 line ");
        myrdd.take(10).forEach(System.out::println);
        System.out.println("--------------------");
        JavaRDD<String[]> csvfields=myrdd.map(line->line.split(","));
        csvfields.take(5).forEach(feilds->System.out.println(String.join("|",feilds)));
    }
}
