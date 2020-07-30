package cn.sks.dwb.achievement

import org.apache.spark.sql.SparkSession

/**
 * 成果对应的融合后对应的总的关系
 */
object ProductAllRef {
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



    val conference_ms_nsfc_orcid_rel = spark.read.table("dwb.wb_product_conference_ms_nsfc_orcid_rel")
      .toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val conference_ms_nsfc_rel = spark.read.table("dwb.wb_product_conference_ms_nsfc_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val criterion_csai_nsfc_rel = spark.read.table("dwb.wb_product_criterion_csai_nsfc_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val journal_csai_nsfc_ms_orcid_rel = spark.read.table("dwb.wb_product_journal_csai_nsfc_ms_orcid_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val journal_csai_nsfc_ms_rel = spark.read.table("dwb.wb_product_journal_csai_nsfc_ms_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val journal_csai_nsfc_rel = spark.read.table("dwb.wb_product_journal_csai_nsfc_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val monograph_csai_nsfc_ms_rel = spark.read.table("dwb.wb_product_monograph_csai_nsfc_ms_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val monograph_csai_nsfc_rel = spark.read.table("dwb.wb_product_monograph_csai_nsfc_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val patent_csai_nsfc_ms_rel = spark.read.table("dwb.wb_product_patent_csai_nsfc_ms_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")
    val patent_csai_nsfc_rel = spark.read.table("dwb.wb_product_patent_csai_nsfc_rel").toDF("achievement_id","achievement_id_origin","product_type","flow_source")


    conference_ms_nsfc_orcid_rel.union(conference_ms_nsfc_rel).union(criterion_csai_nsfc_rel)
      .union(journal_csai_nsfc_ms_orcid_rel).union(journal_csai_nsfc_ms_rel).union(journal_csai_nsfc_rel)
      .union(monograph_csai_nsfc_ms_rel).union(monograph_csai_nsfc_rel)
      .union(patent_csai_nsfc_ms_rel).union(patent_csai_nsfc_rel).createOrReplaceTempView("product_rel")

    //spark.sql("create table dwb.wb_product_rel as select * from product_rel")
    spark.sql("insert overwrite table dwb.wb_product_rel select * from product_rel")





  }
}
