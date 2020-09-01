package cn.sks.dwb.dataTrace

import cn.sks.util.CommonUtil
import org.apache.spark.sql.SparkSession

object Organization {

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


    CommonUtil.getDataTrace(spark,"dwd.wd_organization_manual","dwb.wb_organization_manual_sts")
    CommonUtil.getDataTrace(spark,"dwd.wd_organization_sts","dwb.wb_organization_manual_sts")
    CommonUtil.getDataTrace(spark,"dwd.wd_organization_nsfc","dwb.wb_organization_manual_sts_nsfc")
    CommonUtil.getDataTrace(spark,"dwb.wb_organization_manual_sts","dwb.wb_organization_manual_sts_nsfc")
    CommonUtil.getDataTrace(spark,"ods.o_csai_organization_all","dwb.wb_organization")
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_organization_psn","dwb.wb_organization")
    CommonUtil.getDataTrace(spark,"ods.o_ms_organization","dwb.wb_organization")
    CommonUtil.getDataTrace(spark,"dwb.wb_organization_manual_sts_nsfc","dwb.wb_organization")






  }
}

