package cn.sks.dm.dataTrace

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

    CommonUtil.getDataTrace(spark,"dwb.wb_product_journal_csai_nsfc_ms_orcids","dm.dm_neo4j_product_journal")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_conference_ms_nsfc_orcid","dm.dm_neo4j_product_conference")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_patent_csai_nsfc_ms","dm.dm_neo4j_product_patent")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_monograph_csai_nsfc_ms","dm.dm_neo4j_product_monograph")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_criterion_csai_nsfc","dm.dm_neo4j_product_criterion")


    CommonUtil.getDataTrace(spark,"dwb.wb_product_journal_rel_journal","dm.dm_neo4j_journal_rel_product_journal")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_conference_rel_conference","dm.dm_neo4j_conference_rel_product_conference")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_subject","dm.dm_neo4j_product_subject")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_project","dm.dm_neo4j_project_product")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_person","dm.dm_neo4j_person_product")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_keyword","dm.dm_neo4j_product_keyword")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_organization","dm.dm_neo4j_organization_product")


    CommonUtil.getDataTrace(spark,"dwd.wd_nsfc_experience_study","dm.dm_neo4j_experience_study")
    CommonUtil.getDataTrace(spark,"dwd.wd_nsfc_experience_work","dm.dm_neo4j_experience_work")
    CommonUtil.getDataTrace(spark,"dwd.wd_nsfc_experience_postdoctor","dm.dm_neo4j_experience_postdoctor")

    CommonUtil.getDataTrace(spark,"dwb.wb_organization","dm.dm_neo4j_organization")
    CommonUtil.getDataTrace(spark,"dwb.wb_organization_add","dm.dm_neo4j_organization_add")
    CommonUtil.getDataTrace(spark,"dwb.wb_organization","dm.dm_es_organization")

    CommonUtil.getDataTrace(spark,"dwb.wb_organization_person","dm.dm_neo4j_person_organization")
    CommonUtil.getDataTrace(spark,"dwb.wb_organization_society","dm.dm_neo4j_society_organization")
    CommonUtil.getDataTrace(spark,"dwb.wb_organization_journal","dm.dm_neo4j_journal_organization")


    CommonUtil.getDataTrace(spark,"dwd.wd_product_subject_nsfc_csai","dm.dm_neo4j_project_subject")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_project_keyword","dm.dm_neo4j_project_keyword")
    CommonUtil.getDataTrace(spark,"dwb.wb_keyword_subject","dm.dm_neo4j_keyword_subject")


    CommonUtil.getDataTrace(spark,"ods.o_csai_society","dm.dm_neo4j_society")
    CommonUtil.getDataTrace(spark,"ods.o_csai_subject_constant","dm.dm_neo4j_subject")
    CommonUtil.getDataTrace(spark,"dwb.wb_keyword","dm.dm_neo4j_keyword")
    CommonUtil.getDataTrace(spark,"ods.o_csai_journal","dm.dm_neo4j_journal")
    CommonUtil.getDataTrace(spark,"ods.o_const_conference","dm.dm_neo4j_conference")


    CommonUtil.getDataTrace(spark,"dwb.wb_person_nsfc_sts_academician_csai_ms","dm.dm_neo4j_person")
    CommonUtil.getDataTrace(spark,"dwb.wb_person_add","dm.dm_neo4j_person_add")


    CommonUtil.getDataTrace(spark,"dwb.wb_person_subject","dm.dm_neo4j_person_subject")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_person_keyword","dm.dm_neo4j_person_keyword")
    CommonUtil.getDataTrace(spark,"ods.o_csai_society_person","dm.dm_neo4j_society_person")

  }
}
