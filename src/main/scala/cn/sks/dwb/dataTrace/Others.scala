package cn.sks.dwb.dataTrace

import cn.sks.util.CommonUtil
import org.apache.spark.sql.SparkSession

object Others {

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

    //subject
    CommonUtil.getDataTrace(spark,"subject.csv","ods.o_csai_subject_constant")
    CommonUtil.getDataTrace(spark,"subject_code.csv","dwd.wd_subject_nsfc_csai")
    CommonUtil.getDataTrace(spark,"ods.o_csai_subject_constant","dwd.wd_subject_nsfc_csai")

    CommonUtil.getDataTrace(spark,"dwd.wd_subject_nsfc_csai","dwd.wd_project_subject_nsfc_csai")
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_project","dwd.wd_project_subject_nsfc_csai")

    CommonUtil.getDataTrace(spark,"dwd.wd_project_subject_nsfc_csai","dwd.wd_product_subject_nsfc_csai")
    CommonUtil.getDataTrace(spark,"ods.o_nsfc_project_product","dwd.wd_product_subject_nsfc_csai")


    //journal
    CommonUtil.getDataTrace(spark,"dwb.wb_product_rel","dwb.wb_product_journal_rel_journal")
    CommonUtil.getDataTrace(spark,"ods.o_csai_product_journal_relationship_journal","dwb.wb_product_journal_rel_journal")

    //conference
    CommonUtil.getDataTrace(spark,"dwb.wb_product_conference_ms_nsfc_orcid","dwb.wb_product_conference_rel_conference")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_conference_ms_nsfc_orcid","ods.o_const_conference")

    //keywords

    //nsfc.o_product_keyword_nsfc_split
    //
    CommonUtil.getDataTrace(spark,"dwb.wb_keywords_product_rel_add_uuid","dwb.wb_relation_product_keyword")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_keyword_split_nsfc_translated_en","dwb.wb_relation_product_keyword")
    CommonUtil.getDataTrace(spark,"ods.o_csai_product_journal_keyword","dwb.wb_relation_product_keyword")

    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_keyword","dwb.wb_keyword_subject")
    CommonUtil.getDataTrace(spark,"dwb.wb_relation_product_subject","dwb.wb_keyword_subject")







  }
}

