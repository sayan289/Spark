package org.spark.readalltextfile;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class readalltextfile {
    public static void main(String ars[])
    {
        SparkConf sparkConf=new SparkConf().setAppName("read all text file")
                                            .setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        String path="/home/cbnits-51/Desktop/sayan/textfile";
        JavaPairRDD<String, String> myRdd=sc.wholeTextFiles(path);
        //Display the name of the file
        myRdd.foreach(file->{
            if(file._1.endsWith(".txt"))
            {
                System.out.println(file._1);
            }
        });
        System.out.println(myRdd.count());
//        Display the content of the file
//        myRdd.foreach(file1->{
//            if(file1._1.endsWith(".txt"))
//            {
//                System.out.println(file1._2);
//            }
//        });
    }
}
