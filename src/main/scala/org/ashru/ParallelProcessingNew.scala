package org.ashru

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, blocking}
import scala.util.{Failure, Success}

object ParallelProcessingNew extends App {
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Hive Env Ready")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  var sqlFilesLocation = "C:/Users/beher/IdeaProjects/Tasks/src/main/resources/sqls"
  //var sqlFilesLocation = args(0)
  backupRegressionSuite(spark)
  val wholeFiles = spark.sparkContext.broadcast(spark.sparkContext.wholeTextFiles(sqlFilesLocation).collect())

  val results: Seq[Future[Unit]] = wholeFiles.value.map(f=> prepareRegressionSuite(spark,f._2) )
  //val results: Seq[Future[Unit]] = wholeFiles.value.map(f=> writeDFParallel(f._2) )
  val allDone: Future[Seq[Unit]] = Future.sequence(results)
  //Await.ready(Future{results}, scala.concurrent.duration.Duration.Inf)
  Await.ready(allDone, scala.concurrent.duration.Duration.Inf)

  spark.table("regression_suite").show(false)


  /*def prepareRegressionSuite(sparkSession: SparkSession,query: String): Future[Unit]= Future{
    var df = sparkSession.emptyDataFrame
    println("hello")
    var cond = true
    query.split(";").foreach { x=>
      if(df.isEmpty){
        df=sparkSession.sql(x)
      }
      else{
        val temp_df = sparkSession.sql(x)
        df = df.union(temp_df)
      }
    }
    println("Printing the records")
    df.show(false)
    Thread.sleep(3000)
    //df.write.mode("append").format("hive").saveAsTable("regression_suite")
  }*/

  def prepareRegressionSuite(sparkSession: SparkSession,query: String): Future[Unit]= Future{
    var df = sparkSession.emptyDataFrame
    println("hello")
    var cond = true
    query.split(";").foreach { x=>
      if(cond){
        println("Inside If block",x)
        df=sparkSession.sql(x)
        cond=false
      }
      else{
        println("Inside else block",x)
        val temp_df = sparkSession.sql(x)
        df = df.union(temp_df)
      }
    }
    println("Printing the records")
    df.show(false)
    Thread.sleep(3000)
    //df.write.mode("append").format("hive").saveAsTable("regression_suite")
  }

  def backupRegressionSuite(sparkSession: SparkSession): Unit ={
    sparkSession.sql("insert into regression_suite_history select * from regression_suite")
    val temp = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],sparkSession.table("regression_suite").schema)
    temp.write.mode("overwrite").format("hive").saveAsTable("regression_suite")
  }

  def writeDFParallel(queries : String ) : Future[Unit]= Future{
    var qries = queries.split('\n').map(_.filter(_ >= ' ')).mkString.split(";").map(_.trim)
    var df_list_1 : Future[Unit]= Future{
      val yld =for (query <- qries) yield spark.sql(query)
      Thread.sleep(10000)
      var write_df = yld.reduce((x,y)=> x.union(y))
      write_df.show(false)
      write_df.write.mode("append").format("hive").saveAsTable("regression_suite")
    }
    df_list_1.onComplete{
      case Success(s) => {
        println("Job Succeded")
      }
      case Failure(f) => {
        Thread.sleep(10000)
        println(f)
      }
    }
    Await.ready(df_list_1, scala.concurrent.duration.Duration.Inf)
  }
}
