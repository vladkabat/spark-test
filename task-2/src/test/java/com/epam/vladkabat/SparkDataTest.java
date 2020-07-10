package com.epam.vladkabat;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.Before;
import org.junit.Test;

public class SparkDataTest extends JavaDatasetSuiteBase implements Serializable {

  private SparkData sparkData;

  @Before
  public void runBefore() {
    sparkData = new SparkData();
    super.runBefore();
  }

  @Test
  public void testTransformExpediaDataSet() {
    Dataset<Row> dataset = dataset();

    Dataset<Row> result = sparkData.transformExpediaDataSet(dataset);

    assertTrue(result.count() == 1);
    assertTrue(result.columns()[result.columns().length - 1].equals("ex_hotel_id"));
  }

  private Dataset<Row> dataset() {
    List<Row> rows = Arrays.asList(
        RowFactory
            .create(null, null, null, null, null, null, null, null, null, null, null, null,
                "2017-08-18", null,
                null, null, null, null, null, null),
        RowFactory
            .create(null, null, null, null, null, null, null, null, null, null, null, null,
                "2016-08-18", null,
                null, null, null, null, null, null));
    return sqlContext()
        .createDataset(rows, RowEncoder.apply(sparkData.getExpediaSchema()));
  }
}
