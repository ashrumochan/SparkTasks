package org.ashru

import java.io.File
import java.util.Date
import util.matching._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.types.{ArrayType, StructType}

object Task1 {
  def main(args: Array[String]): Unit = {

    var warehouseLocation = new File("src/main/resources/warehouse").getAbsolutePath
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Hive Env Ready")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    warehouseLocation = warehouseLocation.replaceAllLiterally("\\","/")

    import spark.implicits._

    val departmentDf = spark.table("department")
    val schemaFile = new File("src/main/resources/schema.txt").getAbsolutePath
    val structure = spark.sparkContext.textFile(schemaFile).flatMap(x=>x.split(",")).collect.toSeq

    val origDf = spark.table("students")

    var deltaDf = spark.read.format("xml").option("rowTag","student").load(new File("src/main/resources/updated_student.xml").getAbsolutePath).toDF(structure:_*)
    deltaDf  = flattenDataframe(deltaDf)

    val deltaWithDepartment = deltaDf.as("delta").join(broadcast(departmentDf).as("dept"),$"delta.department"===$"dept.id")
      .select($"delta.st_id",$"delta.name",$"dept.name".as("department_name"))

    //Implementation of SCD type-2
    val unchangedDf = origDf.as("orig").join(deltaWithDepartment.as("delta"),$"orig.st_id"===$"delta.st_id","leftanti")
      .select($"orig.st_id",$"orig.name",$"orig.department_name")
    val finalDf = unchangedDf.union(deltaWithDepartment)

    finalDf.createOrReplaceTempView("final_data")

    val dtString = new Date().getTime.toString
    val resultDf = spark.sql("select * from final_data")
    resultDf.write.mode(SaveMode.Overwrite).parquet(warehouseLocation+"/students/"+dtString)
    spark.sql(s"alter table students set location '"+warehouseLocation+"/students/"+dtString+"'")
    print("Verifying Results")
    spark.sql("select * from students").show()

  }

  def flattenDataframe(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case arrayType: ArrayType =>
          val fieldName_pos = fieldName+"_pos"
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"posexplode_outer($fieldName) as ($fieldName_pos,$fieldName)")
          val explodedDf = df.selectExpr (fieldNamesAndExplode: _*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldNames = structType.fieldNames.map(childName => fieldName + "." + childName)
          val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
          val renamedCols = newFieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("@", "").replace("%","").replace(" ", "").replace("-", ""))))
          val explodeDf = df.select(renamedCols: _*)
          return flattenDataframe(explodeDf)
        case _ =>
      }
    }
    df
  }

  def removeArrayColumns(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val explodedDf = df.selectExpr (fieldNamesExcludingArray: _*)
          return removeArrayColumns(explodedDf)
        case _ =>
      }
    }
    df
  }
}