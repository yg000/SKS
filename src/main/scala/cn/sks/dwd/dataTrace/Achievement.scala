package cn.sks.dwd.dataTrace

import cn.sks.util.CommonUtil
import org.apache.spark.sql.SparkSession

object Achievement {
  val spark: SparkSession = SparkSession.builder()
    .master("local[15]")
    .config("spark.deploy.mode", "clent")
    .config("executor-memory", "12g")
    .config("executor-cores", "6")
    .config("spark.local.dir","/data/tmp")
    //      .config("spark.drivermemory", "32g")
    //      .config("spark.cores.max", "16")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","20")
    .appName("Achievement")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {
    //product
    CommonUtil.getDataTrace(spark,"dwb.wb_product_journal_csai_nsfc_ms_orcids","dm.dm_neo4j_product_journal")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_conference_ms_nsfc_orcid","dm.dm_neo4j_product_conference")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_patent_csai_nsfc_ms","dm.dm_neo4j_product_patent")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_monograph_csai_nsfc_ms","dm.dm_neo4j_product_monograph")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_criterion_csai_nsfc","dm.dm_neo4j_product_criterion")

    //person
    CommonUtil.getDataTrace(spark,"ods.o_ms_product_author","dwd.wd_person_ms")
    CommonUtil.getDataTrace(spark,"ods.o_arp_person","dwd.wd_person_arp")
    CommonUtil.getDataTrace(spark,"ods.o_csai_person_academician","dwd.wd_person_academician")
    CommonUtil.getDataTrace(spark,"ods.o_csai_person_all","dwd.wd_person_csai")
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_person","dwd.wd_person_nsfc")

    //organization
    CommonUtil.getDataTrace(spark,"ods.o_manual_organization_standards_institutions","dwd.wd_organization_manual")
    CommonUtil.getDataTrace(spark,"ods.o_manual_organization_zky_inst","dwd.wd_organization_manual")
    CommonUtil.getDataTrace(spark,"ods.o_manual_organization_grid","dwd.wd_organization_manual")
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_organization","dwd.wd_organization_nsfc")
    CommonUtil.getDataTrace(spark,"ods.o_manual_organization_sts","dwd.wd_organization_sts")

    //project
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_project","dwd.wd_project_nsfc")

    //experience
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_experience_study","dwd.wd_nsfc_experience_study")
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_experience_work","dwd.wd_nsfc_experience_work")
    CommonUtil.getDataTrace(spark,"ods.o_csai_person_work_experience_all","dwd.wd_nsfc_experience_work")
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_experience_postdoctor","dwd.wd_nsfc_experience_postdoctor")

    CommonUtil.getDataTrace(spark,"nsfc.o_person_resume","ods.o_nsfc_experience_study")
    CommonUtil.getDataTrace(spark,"nsfc.o_person_resume","ods.o_nsfc_experience_work")
    CommonUtil.getDataTrace(spark,"nsfc.o_person_resume","ods.o_nsfc_experience_postdoctor")

  }
}
