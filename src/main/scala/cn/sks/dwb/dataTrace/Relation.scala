package cn.sks.dwb.dataTrace

import cn.sks.util.CommonUtil
import org.apache.spark.sql.SparkSession

object Relation {

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


    //product_subject
    CommonUtil.getDataTrace(spark,"dwd.wd_product_subject_nsfc_csai","dwb.wb_relation_product_subject")
    CommonUtil.getDataTrace(spark,"ods.o_csai_product_journal_subject","dwb.wb_relation_product_subject")
    CommonUtil.getDataTrace(spark,"ods.o_csai_product_patent_subject","dwb.wb_relation_product_subject")
    CommonUtil.getDataTrace(spark,"ods.o_csai_subject_constant","dwb.wb_relation_product_subject")

    //project_product
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_project_product","dwb.wb_relation_product_project")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_rel","dwb.wb_relation_product_project")

    //product_person
    CommonUtil.getDataTrace(spark,"dwd.wd_product_person_ext_csai","dwb.wb_relation_product_person")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_person_ext_nsfc","dwb.wb_relation_product_person")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_person_ext_orcid","dwb.wb_relation_product_person")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_rel","dwb.wb_relation_product_person")



    //organization_person
    CommonUtil.getDataTrace(spark,"dm.dm_neo4j_person","dwb.wb_organization_person")

    //product_organization
    CommonUtil.getDataTrace(spark,"ods.o_csai_criterion_org","dwb.wb_product_organization")
    CommonUtil.getDataTrace(spark,"ods.o_csai_product_journal_author","dwb.wb_product_organization")
    CommonUtil.getDataTrace(spark,"ods.o_csai_product_patent_inventor","dwb.wb_product_organization")
    CommonUtil.getDataTrace(spark,"ods.o_csai_product_monograph_author","dwb.wb_product_organization")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_rel","dwb.wb_product_organization")

    //org_society
    CommonUtil.getDataTrace(spark,"ods.o_csai_society_support_org","dwb.wb_organization_society")

    //organization_journal
    CommonUtil.getDataTrace(spark,"ods.o_csai_journal","dwb.wb_organization_journal")

    //person_subject
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_person","dwb.wb_person_subject")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_person","dwb.wb_person_subject")

    //person_subject
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_person","dwb.wb_person_subject")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_person","dwb.wb_person_subject")
  }
}

