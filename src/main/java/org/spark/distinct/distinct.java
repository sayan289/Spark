package org.spark.distinct;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class distinct {
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
        JavaPairRDD<Integer,Long> counts = pairrdd.distinct();
        System.out.println(pairrdd.count());
        //System.out.println(counts.count());
        counts.take(5).forEach(line->{
            System.out.println(line._1+" "+line._2);

        });

    }
}
