package org.ashru

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object HiveDataLoad {
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
    spark.sparkContext.setLogLevel("ERROR")
    /*val x = new File("src/main/resources/warehouse").getAbsolutePath
    val y = new File("src/main/resources/warehouse").getAbsolutePath.mkString
    print(x.replaceAllLiterally("\\","/"))
    print(y)
    print(spark.sql("show create table students").first().mkString)
    System.exit(0)*/

    warehouseLocation = warehouseLocation.replaceAllLiterally("\\","/")
    //Department Table
    var sql =
      s"""create external table if not exists department(id int,name string)
        |  row format delimited
        |  fields terminated by ','
        |  lines terminated by '\n'
        |  stored as textfile
        |  location '${warehouseLocation}/department' """.stripMargin
    spark.sql(sql)
    spark.sql("drop table if exists students")
    sql =
      s"""create external table if not exists students(st_id string,name string,department_name string)
        |  stored as parquet
        |  location '${warehouseLocation}/students'
        | """.stripMargin

    spark.sql(sql)

    val schemaFile = new File("src/main/resources/schema.txt").getAbsolutePath
    val structure = spark.sparkContext.textFile(schemaFile).flatMap(x=>x.split(",")).collect.toSeq

    val origDf = spark.read.format("xml").option("rowTag","student").load(new File("src/main/resources/old_student.xml").getAbsolutePath).toDF(structure:_*)
    origDf.show()
    val departmentDf = spark.table("department")
    val deltaWithDepartment = origDf.as("orig").join(broadcast(departmentDf).as("dept"),$"orig.department"===$"dept.id").select($"orig.st_id",$"orig.name",$"dept.name".as("department_name"))

    deltaWithDepartment.show()
    deltaWithDepartment.write.mode(SaveMode.Overwrite).parquet(new File("src/main/resources/warehouse").getAbsolutePath+"/students")

    spark.sql("select * from students").show()
  }
}
