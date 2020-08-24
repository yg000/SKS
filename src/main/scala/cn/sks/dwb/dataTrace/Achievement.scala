package cn.sks.dwb.dataTrace


import cn.sks.jutil.H2dbUtil
import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.CommonUtil
object Achievement {

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


    CommonUtil.getDataTrace(spark,"dwd.wd_product_criterion_csai","dwb.wb_product_criterion_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_criterion_nsfc","dwb.wb_product_criterion_csai_nsfc")


    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_nsfc","dwb.wb_product_monograph_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_project_nsfc","dwb.wb_product_monograph_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_npd_nsfc","dwb.wb_product_monograph_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_csai","dwb.wb_product_monograph_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_monograph_csai_nsfc","dwb.wb_product_monograph_csai_nsfc_ms")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_ms","dwb.wb_product_monograph_csai_nsfc_ms")



    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_nsfc","dwb.wb_product_conference_ms_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_project_nsfc","dwb.wb_product_conference_ms_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_npd_nsfc","dwb.wb_product_conference_ms_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_ms","dwb.wb_product_conference_ms_nsfc")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_conference_ms_nsfc","dwb.wb_product_conference_ms_nsfc_orcid")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_orcid","dwb.wb_product_conference_ms_nsfc_orcid")



    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_csai","dwb.wb_product_journal_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_nsfc","dwb.wb_product_journal_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_project_nsfc","dwb.wb_product_journal_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_npd_nsfc","dwb.wb_product_journal_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_journal_csai_nsfc","dwb.wb_product_journal_csai_nsfc_ms")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_ms","dwb.wb_product_journal_csai_nsfc_ms")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_journal_csai_nsfc_ms","dwb.wb_product_journal_csai_nsfc_ms_orcid")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_orcid","dwb.wb_product_journal_csai_nsfc_ms_orcid")


    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_csai","dwb.wb_product_patent_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_nsfc","dwb.wb_product_patent_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_project_nsfc","dwb.wb_product_patent_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_npd_nsfc","dwb.wb_product_patent_csai_nsfc")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_patent_csai_nsfc","dwb.wb_product_patent_csai_nsfc_ms")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_ms","dwb.wb_product_patent_csai_nsfc_ms")



    CommonUtil.getDataTrace(spark,"dwd.wd_product_criterion_csai","dwb.wb_product_criterion_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_criterion_nsfc","dwb.wb_product_criterion_csai_nsfc_rel")


    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_nsfc","dwb.wb_product_monograph_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_project_nsfc","dwb.wb_product_monograph_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_npd_nsfc","dwb.wb_product_monograph_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_csai","dwb.wb_product_monograph_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_monograph_csai_nsfc","dwb.wb_product_monograph_csai_nsfc_ms_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_monograph_ms","dwb.wb_product_monograph_csai_nsfc_ms_rel")



    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_nsfc","dwb.wb_product_conference_ms_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_project_nsfc","dwb.wb_product_conference_ms_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_npd_nsfc","dwb.wb_product_conference_ms_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_ms","dwb.wb_product_conference_ms_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_conference_ms_nsfc","dwb.wb_product_conference_ms_nsfc_orcid_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_conference_orcid","dwb.wb_product_conference_ms_nsfc_orcid_rel")



    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_csai","dwb.wb_product_journal_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_nsfc","dwb.wb_product_journal_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_project_nsfc","dwb.wb_product_journal_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_npd_nsfc","dwb.wb_product_journal_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_journal_csai_nsfc","dwb.wb_product_journal_csai_nsfc_ms_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_ms","dwb.wb_product_journal_csai_nsfc_ms_rel")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_journal_csai_nsfc_ms","dwb.wb_product_journal_csai_nsfc_ms_orcid_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_journal_orcid","dwb.wb_product_journal_csai_nsfc_ms_orcid_rel")


    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_csai","dwb.wb_product_patent_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_nsfc","dwb.wb_product_patent_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_project_nsfc","dwb.wb_product_patent_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_npd_nsfc","dwb.wb_product_patent_csai_nsfc_rel")
    CommonUtil.getDataTrace(spark,"dwb.wb_product_patent_csai_nsfc","dwb.wb_product_patent_csai_nsfc_ms_rel")
    CommonUtil.getDataTrace(spark,"dwd.wd_product_patent_ms","dwb.wb_product_patent_csai_nsfc_ms_rel")







  }
}

