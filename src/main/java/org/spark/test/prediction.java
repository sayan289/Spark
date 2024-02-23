package org.spark.test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.col;

public class prediction {
    public static void main(String ars[])
    {
        SparkSession spark = SparkSession.builder()
                .appName("Iris DataFrame")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Reading a Csv file path
        String path = "/home/cbnits-51/Downloads/Iris.csv";
        Dataset<Row> df = spark.read().csv(path).toDF();
        // Dropped First Row
        System.out.println("Dropped Row");
        Dataset<Row> filter = df.filter(col("_c0").notEqual("Id"));
        filter.show();

        // Encode the "_c5" column to numerical values
        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("_c5")
                .setOutputCol("target")
                .fit(filter);
        System.out.println("Encode c0 column");

        Dataset<Row> encodeDf = indexer.transform(filter);
        encodeDf.show(5);

        // Convert all columns to DoubleType
        for (String columnName : encodeDf.columns()) {
            encodeDf = encodeDf.withColumn(columnName, encodeDf.col(columnName).cast("double"));
        }

        // Assemble features into a single vector column
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"_c1", "_c2", "_c3", "_c4"})
                .setOutputCol("features");

        Dataset<Row> assembledDf = assembler.transform(encodeDf);

        // Split the data into training and testing sets (80% train, 20% test)
        Dataset<Row>[] splits = assembledDf.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Create a Logistic Regression model
        LogisticRegression lr = new LogisticRegression()
                .setLabelCol("target")
                .setFeaturesCol("features");

        // Train the model
        LogisticRegressionModel lrModel = lr.fit(trainingData);

        // Make predictions on the test data
        Dataset<Row> predictions = lrModel.transform(testData);

        // Evaluate the model
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("target")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        // Calculate and print the accuracy
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Accuracy: " + accuracy);
    }
}
