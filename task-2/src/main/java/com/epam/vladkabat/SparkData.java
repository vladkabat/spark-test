package com.epam.vladkabat;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SparkData {

  protected void writeDataSet(Dataset<Row> dataset) {
    dataset
        .write()
        .format("com.databricks.spark.avro")
        .save("hdfs://sandbox-hdp.hortonworks.com:8020/user/root/orc-homework");
  }

  protected Dataset<Row> transformExpediaDataSet(Dataset<Row> dataset) {
    return dataset.filter(year(col("srch_ci")).equalTo(2017))
        .withColumnRenamed("hotel_id", "ex_hotel_id");
  }

  protected Dataset<Row> loadExpediaDataSet(SparkSession spark) {
    return spark
        .read()
        .format("com.databricks.spark.avro")
        .schema(getExpediaSchema())
        .load("hdfs://sandbox-hdp.hortonworks.com:8020/user/root/expedia");
  }

  protected StructType getExpediaSchema() {
    return new StructType()
        .add("id", "long")
        .add("date_time", "string")
        .add("site_name", "integer")
        .add("posa_continent", "integer")
        .add("user_location_country", "integer")
        .add("user_location_region", "integer")
        .add("user_location_city", "integer")
        .add("orig_destination_distance", "double")
        .add("user_id", "integer")
        .add("is_mobile", "integer")
        .add("is_package", "integer")
        .add("channel", "integer")
        .add("srch_ci", "string")
        .add("srch_co", "string")
        .add("srch_adults_cnt", "integer")
        .add("srch_children_cnt", "integer")
        .add("srch_rm_cnt", "integer")
        .add("srch_destination_id", "integer")
        .add("srch_destination_type_id", "integer")
        .add("hotel_id", "long");
  }
}
