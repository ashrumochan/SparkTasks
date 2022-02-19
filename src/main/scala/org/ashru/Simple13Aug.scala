package org.ashru

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{regexp_extract,coalesce}

object Simple13Aug extends App {
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Hive Env Ready")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val cust_hier = spark.table("tfs_raw_hashed.customer_hierarchy_detail_raw_v1")
                        .select("marketing_primary_segment_code","marketing_primary_segment")
                        .distinct()
  val rev_bp = spark.table("vcg_tfs_dev.business_profile_segment").as("bp")
                .join(spark.table("tfs_raw_hashed.lesof_raw_v1").as("les"),
                $"bp.cust_id"===$"les.le_id".cast(StringType),"left")
                .withColumn("bp_hqduns",$"hq_duns")
                .withColumn("bp_guduns",$"gu_duns")
                .withColumn("cle_segment",$"pre_seg_cd")
                .withColumn("country",$"les.entity_country")
                .withColumn("le_hqduns",$"les.duns")
                .withColumn("le_guduns",regexp_extract($"duns_lineage","([\\d]+)",1))
                .select($"cust_id",
                              $"cust_id_type",
                              $"bp_hqduns",
                              $"bp_guduns",
                              $"rev_profile",
                              $"cle_segment",
                              $"country",
                              $"le_hqduns",
                              $"le_guduns"
                      )

  val temp = rev_bp.as("rev_bp")
              .join(spark.table("tfs_raw_hashed.allnasp_raw_v1").as("nasp"),
              $"rev_bp.cust_id"===$"nasp.naspl","left")
              .withColumn("hq_duns",coalesce($"le_hqduns",$"bp_hqduns"))
              .withColumn("gu_duns",coalesce($"nasp.gu_duns_number",$"le_guduns",$"bp_guduns"))
              .withColumn("segment_code",coalesce($"nasp.cus_seg",$"cle_segment"))
              .select("cust_id","cust_id_type","hq_duns","gu_duns","rev_profile","segment_code")
  val cust_acct_info = temp.as("temp").join(cust_hier.as("cust_hier"),
                          $"temp.segment_code"===$"cust_hier.marketing_primary_segment_code","left")
                        .select($"temp.cust_id",
                                      $"temp.cust_id_type",
                                      $"temp.hq_duns",
                                      $"temp.gu_duns",
                                      $"temp.rev_profile",
                                      $"temp.segment_code",
                                      $"cust_hier.marketing_primary_segment")
  cust_acct_info.write.saveAsTable("")
}


