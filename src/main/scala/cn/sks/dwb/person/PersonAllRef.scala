package cn.sks.dwb.person

import org.apache.spark.sql.SparkSession

/**
 * 成果对应的融合后对应的总的关系
 */
object PersonAllRef {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      //      .config("spark.deploy.mode", "8g")
      //      .config("spark.drivermemory", "32g")
      //      .config("spark.cores.max", "16")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("Criterion")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    spark.sql("select count(*),count(distinct person_id) from dwb.wb_relation_product_person ").show()
    spark.sql("select count(*),count(distinct person_id) from dwb.wb_relation_product_person ").show()

//    val person_nsfc_sts_academician_csai_ms_rel = spark.read.table("dwb.wb_person_nsfc_sts_academician_csai_ms_rel")
//    val person_nsfc_sts_academician_csai_rel = spark.read.table("dwb.wb_person_nsfc_sts_academician_csai_rel")
//    val person_nsfc_sts_academician_rel = spark.read.table("dwb.wb_person_nsfc_sts_academician_rel")
//    val person_nsfc_sts_rel = spark.read.table("dwb.wb_person_nsfc_sts_rel")
//
//    person_nsfc_sts_academician_csai_ms_rel.unionAll(person_nsfc_sts_academician_csai_rel).unionAll(person_nsfc_sts_academician_rel).unionAll(person_nsfc_sts_rel)
//        .createOrReplaceTempView("person_rel")
//
//
//    spark.sql("insert overwrite table dwb.wb_person_rel select * from person_rel")





  }
}
