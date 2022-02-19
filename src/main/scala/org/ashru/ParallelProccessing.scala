package org.ashru

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import org.apache.spark.util.CollectionAccumulator
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

import scala.concurrent.Future

object ParallelProccessing extends App {
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Hive Env Ready")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  val sc= spark.sparkContext
  var df_list :  CollectionAccumulator[(String,List[DataFrame])]= sc.collectionAccumulator("df_list")
  val path = new File("C:/Users/beher/IdeaProjects/Tasks/src/main/resources/sql_sourab/").getAbsolutePath

  def getDF(df: String ,queries : String ) : Future[String]= Future{
    var qries = queries.split(";").map(_.trim)
    qries.foreach(println)
    var df_list_1= for (query <- qries) yield spark.sql(query)
    var df_list_yield = df_list_1.toList
    df_list_yield.map(x=> x.show())
    println("Dataframe names "+df)
    var df_name = df.split("\\.")(0)
    println(df_name)
    df_list.add(df_name,df_list_yield)
    "DFs created successfully"
  }

  val df1= sc.parallelize(List(1,2,3)).toDF()
  df1.createOrReplaceTempView("df1")

  val df2= sc.parallelize(List(4,5,6)).toDF()
  df2.createOrReplaceTempView("df2")

  var queries = sc.wholeTextFiles(path)
  queries.collect().foreach(println)
  var query_map = queries.map(x=>(x._1.split("/").last,x._2)).collectAsMap
  var query_map_broadcast = sc.broadcast(query_map)

  try{
    query_map_broadcast.value.map(x=>getDF(x._1,x._2))
    query_map_broadcast.destroy()
    val df_list_scala = df_list.value.asScala
    df_list_scala.map(x=>x._2.map(y=>y.show()))
  }
  catch {
    case ex :Throwable => {
      println(ex)
      throw(ex)
    }
  }

  finally{
    spark.stop()
  }

}
