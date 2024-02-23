package org.spark.test;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.*;
import scala.Tuple2;
public class normaloperation {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("Irish Dataframe")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //Reading a Csv file  path
        String path = "/home/cbnits-51/Downloads/Iris.csv";
        Dataset<Row> df = spark.read().csv(path).toDF();
//        df.show();
        Dataset<Row> selectColumn = df.select("_c0");
        selectColumn.show();
        System.out.println("All the dataset");
        df.show(5);
        //It checks the value of -c0 column and eleminate the entire row.
        Dataset<Row> filter = df.filter(functions.col("_c0").notEqual("Id"));
        System.out.println("After deleteing Id, Sepallength ....");
        filter.show(5);
        StringIndexerModel stringindexmodel=new StringIndexer()
                .setInputCol("_c5")
                .setOutputCol("label")
                .fit(filter);
        Dataset<Row> indexedDF = stringindexmodel.transform(filter);
        System.out.println("After encoding");
        indexedDF.show();
        Dataset<Row> droppingc5 = indexedDF.drop("_c5");
        System.out.println("After dropping c5 column");
        droppingc5.show();
        System.out.println("Printing the dataset description");
        Dataset<Row> describe = droppingc5.describe();
        describe.show();
        System.out.println("DataTypes");
        Tuple2<String, String>[] dtypes = describe.dtypes();
        for (Tuple2<String, String> dtype : dtypes) {
            System.out.println("Column: " + dtype._1 + ", Data Type: " + dtype._2);
        }
    }
}
