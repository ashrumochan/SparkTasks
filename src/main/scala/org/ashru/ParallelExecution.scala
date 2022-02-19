package org.ashru

import java.io.File

import org.apache.spark.sql.{Row, SparkSession}
import org.ashru.ParallelProcessingNew.{prepareRegressionSuite, spark, wholeFiles}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object ParallelExecution {

  def backupRegressionSuite(sparkSession: SparkSession): Unit ={
    sparkSession.sql("insert into regression_suite_history select * from regression_suite")
    val temp = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],sparkSession.table("regression_suite").schema)
    temp.write.mode("overwrite").format("hive").saveAsTable("regression_suite")
    //sparkSession.sql("truncate table regression_suite")
  }

  def prepareRegressionSuite(sparkSession: SparkSession,query: String): Future[Unit]= Future{
    var df = sparkSession.emptyDataFrame
    query.split(";").foreach { x=>
      if(df.isEmpty){
        df=sparkSession.sql(x)
      }
      else{
        val temp_df = sparkSession.sql(x)
        df = df.union(temp_df)
      }

    }
    df.show(false)
    //df.write.mode("append").format("hive").saveAsTable("regression_suite")
  }

  def main(args: Array[String]): Unit = {
    //var sqlFilesLocation = new File("src/main/resources/sqls").getAbsolutePath
    var sqlFilesLocation = new File("C:/Users/beher/IdeaProjects/Tasks/src/main/resources/sqls").getAbsolutePath
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
    backupRegressionSuite(spark)

    val wholeFiles = spark.sparkContext.broadcast(spark.sparkContext.wholeTextFiles(sqlFilesLocation).collect())

    /*print(wholeFiles.getClass)
    wholeFiles.value.foreach( f => {
      prepareRegressionSuite(spark,f._2)
    })
*/
    val results: Seq[Future[Unit]] = wholeFiles.value.map(f=> prepareRegressionSuite(spark,f._2) )
    val allDone: Future[Seq[Unit]] = Future.sequence(results)
    Await.result(allDone, scala.concurrent.duration.Duration.Inf)

    spark.table("regression_suite").show(false)
    spark.table("regression_suite_history").show(false)
  }

}
