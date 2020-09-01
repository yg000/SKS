package cn.sks.dwb.dataTrace

import cn.sks.util.CommonUtil
import org.apache.spark.sql.SparkSession

object Person {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      //      .config("spark.deploy.mode", "8g")
      //      .config("spark.drivermemory", "32g")
      //      .config("spark.cores.max", "16")
      .config("spark.sql.shuffle.partitions","10")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("DataTrace")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    CommonUtil.getDataTrace(spark,"dwd.wd_person_nsfc","dwb.wb_person_nsfc_sts")
    CommonUtil.getDataTrace(spark,"dwd.wd_person_arp","dwb.wb_person_nsfc_sts")

    CommonUtil.getDataTrace(spark,"dwb.wb_person_nsfc_sts","dwb.wb_person_nsfc_sts_academician")
    CommonUtil.getDataTrace(spark,"dwd.wd_person_academician","dwb.wb_person_nsfc_sts_academician")

    CommonUtil.getDataTrace(spark,"dwb.wb_person_nsfc_sts_academician","dwb.wb_person_nsfc_sts_academician_csai")
    CommonUtil.getDataTrace(spark,"dwd.wd_person_csai","dwb.wb_person_nsfc_sts_academician_csai")

    CommonUtil.getDataTrace(spark,"dwb.wb_person_nsfc_sts_academician_csai","dwb.wb_person_nsfc_sts_academician_csai_ms")
    CommonUtil.getDataTrace(spark,"dwd.wd_person_ms","dwb.wb_person_nsfc_sts_academician_csai_ms")








  }
}

