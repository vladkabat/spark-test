package com.epam.vladkabat;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("task1")
        .getOrCreate();

    SparkData sparkData = new SparkData();
    Dataset<Row> dataset = sparkData.loadExpediaDataSet(spark);
    Dataset<Row> result = sparkData.transformExpediaDataSet(dataset);
    sparkData.writeDataSet(result);

    spark.stop();
  }
}
