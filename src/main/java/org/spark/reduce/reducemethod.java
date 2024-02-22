package org.spark.reduce;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class reducemethod {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("My First Spark Code !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        List<Integer> data=new ArrayList<>();
        int n=10000,i;
        for(i=1;i<=n;i++)
        {
            data.add(i);
        }
        JavaRDD<Integer> myrdd = sc.parallelize(data, 4);
//        Spark Reduce
//        int sum=myrdd.reduce(Integer::sum);
//        System.out.println(sum);
//        final Instant start= Instant.now();
//        int sum1=0;
//        for(i=1;i<=10;i++)
//        {
//            sum1=myrdd.reduce(Integer::sum);
//        }
//        long time=(Duration.between(start,Instant.now()).toMillis())/10;
//        System.out.println("Time taken "+time);
//        System.out.println("Sum is "+sum1);
//        spark fold
//        int zeroValue = 0;
//        int sum=myrdd.fold(zeroValue,Integer::sum);
//        System.out.println(sum);
          int zerovalue=0;
//        for(i=1;i<=10;i++)
//        {
//            int sum = myrdd.aggregate(zerovalue, Integer::sum, Integer::sum);
//            int max=myrdd.aggregate(zerovalue,Integer::sum,Integer::max);
//            int min=myrdd.aggregate(zerovalue,Integer::sum,Integer::min);
//            System.out.println("Sum of all partition is :"+sum);
//            System.out.println("Max of each partition is :"+max);
//            System.out.println("Min of each partition is "+min);
//        }
//        int sum = myrdd.aggregate(zerovalue, Integer::sum, Integer::sum);
//        int max=myrdd.aggregate(zerovalue,Integer::sum,Integer::max);
//        int min=myrdd.aggregate(zerovalue,Integer::sum,Integer::min);
//        System.out.println("Sum of all partition is :"+sum);
//        System.out.println("Max of each partition is :"+max);
//        System.out.println("Min of each partition is "+min);
//          JavaRDD<Integer>mylist=myrdd.mapPartitions(iterator->{
//            int sum=0;
//            while(iterator.hasNext())
//            {
//                sum+=iterator.next();
//            }
//            return Collections.singletonList(sum).iterator();
//          });
//          List<Integer> sumofpartition=mylist.collect();
//          for(i=0;i<sumofpartition.size();i++)
//          {
//              System.out.println("sum of "+i+"th partition is "+sumofpartition.get((i)));
//          }
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> squaredNumbers = numbers.map(x -> x * x);
        numbers.foreach(x->{
            System.out.println(x);
        });
        squaredNumbers.foreach(number->{
            System.out.println(number);
        });
        System.out.println(squaredNumbers.collect());//It also sow the data of that rdd.
        //System.out.println(numbers.count());//It return how many data present within that Rdd.
    }

}
