package com.epam.vladkabat;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Application {

  public static void main(String[] args) {

    Map<String, String> properties = new HashMap<>();
    properties.put("span.receiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
    properties.put("sampler.classes", "AlwaysSampler");
    HTraceConfiguration conf = HTraceConfiguration.fromMap(properties);

    Tracer tracer = new Tracer.Builder("Widget Tracer").conf(conf).build();

    try (TraceScope newScope = tracer.newScope("MyScope")) {

      SparkSession spark = SparkSession
          .builder()
          .appName("task1")
          .getOrCreate();

      Dataset<WeatherHotel> dsKafka = spark
          .read()
          .format("kafka")
          .option("kafka.bootstrap.servers", "172.18.0.2:6667")
          .option("subscribe", "input-topic")
          .option("startingOffsets", "earliest")
          .option("kafka.max.partition.fetch.bytes", "524288")
          .load()
          .selectExpr("CAST(value AS STRING)")
          .map((MapFunction<Row, WeatherHotel>) row -> new Gson()
              .fromJson(row.getString(0), WeatherHotel.class), Encoders.bean(WeatherHotel.class));

      dsKafka.takeAsList(10).forEach(System.out::println);

      Dataset<Row> ds = spark
          .read()
          .format("com.databricks.spark.avro")
          .load("hdfs://sandbox-hdp.hortonworks.com:8020/user/root/expedia");

      Dataset<Row> resultTask1 = ds
          .groupBy(col("hotel_id"))
          .agg(max(col("srch_co")).as("srch_co"))
          .withColumn("idle_days", datediff(current_date(), col("srch_co")));

      resultTask1.show();

      Dataset<Row> dsTask2_1 = ds
          .sort(col("hotel_id"), col("srch_ci").desc())
          .select(col("hotel_id"), col("srch_ci"), col("srch_co"))
          .withColumn("rowId", monotonically_increasing_id());

      Dataset<Row> dsTask2_2 = dsTask2_1.as("dsTask2_2").select(col("rowId").as("1_rowId"),
          col("hotel_id").as("1_hotel_id"), col("srch_ci").as("1_srch_ci"),
          col("srch_co").as("1_srch_co"))
          .filter(col("1_rowId").notEqual(0));

      Dataset<Row> joinDs = dsTask2_1
          .join(dsTask2_2, col("rowId").equalTo(col("1_rowId").minus(1)))
          .withColumn("idle_date", datediff(col("srch_ci"), col("1_srch_co")))
          .filter(col("idle_date").$greater$eq(2).and(col("idle_date").$less(30)).notEqual(true))
          .select(col("hotel_id"), col("srch_ci"), col("srch_co"));

      joinDs.withColumn("year", year(col("srch_ci")))
          .write().partitionBy("year")
          .csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/root/task3");

      Dataset<Row> csvDs = spark
          .read()
          .option("header", "true")
          .csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/root/hotels");

      Dataset<Row> joinDsTask3 = dsTask2_1
          .join(dsTask2_2, col("rowId").equalTo(col("1_rowId").minus(1)))
          .withColumn("idle_date", datediff(col("srch_ci"), col("1_srch_co")))
          .filter(col("idle_date").$greater$eq(2).and(col("idle_date").$less(30)))
          .select(col("hotel_id"), col("srch_ci"), col("srch_co"));

      Dataset<Row> resultTask3 = joinDsTask3
          .join(csvDs, col("Id").equalTo(col("hotel_id")))
          .groupBy(col("hotel_id"))
          .agg(first(col("Name")), first(col("Country")),
              first(col("City")), first(col("Address")));

      resultTask3.show();

      Dataset<Row> resultTask4 = joinDsTask3
          .join(csvDs, col("Id").equalTo(col("hotel_id")))
          .select(count(col("City")), count(col("Country")));

      resultTask4.show();

      joinDs.show();

      spark.stop();
    }
    tracer.close();
  }
}
