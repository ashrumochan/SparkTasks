package org.ashru

import java.io.File

import org.apache.spark.sql.SparkSession

object TestTable {
  def main(args: Array[String]): Unit = {
    var sqlFilesLocation = new File("src/main/resources/sqls").getAbsolutePath
    var warehouseLocation = new File("src/main/resources/warehouse").getAbsolutePath

    val spark = SparkSession
      .builder
      .master("local")
      .appName("Hive Env Ready")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    spark.table("regression_suite").show(false)
  }

}
