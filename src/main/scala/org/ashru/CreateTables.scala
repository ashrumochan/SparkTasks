package org.ashru

import java.io.File

import org.apache.spark.sql.SparkSession

object CreateTables {
  def main(args: Array[String]): Unit = {
    var warehouseLocation = new File("src/main/resources/warehouse").getAbsolutePath

    val spark = SparkSession
      .builder
      .master("local")
      .appName("Hive Env Ready")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    warehouseLocation = warehouseLocation.replaceAllLiterally("\\", "/")
    val main_table = s"""create table if not exists regression_suite(
                       |TEST_CASE_ID string,
                       |TEST_SCENARIO string,
                       |RECORDS_COUNT int,
                       |EXPECTED_RESULT string,
                       |TEST_RESULT string,
                       |RUN_TIME timestamp
                       |)
                       |stored as parquet
                       |location '${warehouseLocation}/regression_suite'""".stripMargin
    spark.sql("drop table if exists regression_suite")
    spark.sql(main_table)

    val history_table = s"""create external table regression_suite_history(
                          |TEST_CASE_ID string,
                          |TEST_SCENARIO string,
                          |RECORDS_COUNT int,
                          |EXPECTED_RESULT string,
                          |TEST_RESULT string,
                          |RUN_TIME timestamp
                          |)
                          |stored as parquet
                          |location '${warehouseLocation}/regression_suite_history'""".stripMargin
    spark.sql("drop table regression_suite_history")
    spark.sql(history_table)
  }

}
