package org.spark.readtext;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class readtextfile {
    public static void main(String ars[])
    {
        SparkConf sparkConf=new SparkConf().setAppName("read text file")
                                            .setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        String path="/home/cbnits-51/Downloads/Basics";
        JavaRDD<String> myRdd= sc.textFile(path);
        for (String line : myRdd.collect()) {
               System.out.println(line);
        }
    }
}
