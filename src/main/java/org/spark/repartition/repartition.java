package org.spark.repartition;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class repartition {
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
        System.out.println("Repartition is "+repartition.getNumPartitions());
        repartition.mapPartitions(partition->{
            long partitionsize=0;
            while(partition.hasNext())
            {
                partitionsize++;
                partition.next();
            }
            return Collections.singletonList(partitionsize).iterator();
        }).foreachPartition(partitionsize->{
            if(partitionsize.hasNext())
            {
                System.out.println("partition size :"+partitionsize.next());
            }
        });
        JavaRDD<Integer> repartition1 = repartition.repartition(10);//repartition method create new Rdd.
        System.out.println("Repartition of new rdd is "+repartition1.getNumPartitions());
    }
}
