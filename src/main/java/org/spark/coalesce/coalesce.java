package org.spark.coalesce;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class coalesce {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("PairRdd !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        List<Integer> data=new ArrayList<>();
        int n=10000;
        int i;
        for(i=1;i<=n;i++)
        {
            data.add(i);
        }
        System.out.println("Default partition ="+sc.defaultMinPartitions());
        JavaRDD<Integer> repartition = sc.parallelize(data, 14);
        JavaRDD<Integer> repartition1 = repartition.coalesce(10);//repartition method create new Rdd.
        System.out.println("coalesce of new rdd is "+repartition1.getNumPartitions());
    }
}
