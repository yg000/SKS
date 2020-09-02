package cn.sks.dm.achievement

import cn.sks.util.AchievementUtil
import org.apache.spark.sql.SparkSession

object Product {
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

//
//    spark.read.table("dm.dm_neo4j_product_journal").select("id","paper_type")
//      .unionAll(spark.read.table("dm.dm_neo4j_product_conference").select("id","paper_type"))
//      .unionAll(spark.read.table("dm.dm_neo4j_product_patent").select("id","paper_type"))
//      .unionAll(spark.read.table("dm.dm_neo4j_product_monograph").select("id","paper_type"))
//      .unionAll(spark.read.table("dm.dm_neo4j_product_criterion").select("id","paper_type")).createOrReplaceTempView("tmp")
//
//    spark.sql(
//      """
//        |select * from tmp where id = '81027a85174b4691199a3fd2f931399e'
//        |""".stripMargin)//.show()
//
//    spark.sql(
//      """
//        |select * from dwb.wb_product_journal_csai_nsfc_ms_orcid where achievement_id = '81027a85174b4691199a3fd2f931399e'
//        |""".stripMargin).show(false)
//    spark.sql(
//      """
//        |select * from dwb.wb_product_conference_ms_nsfc_orcid where achievement_id = '81027a85174b4691199a3fd2f931399e'
//        |""".stripMargin).show(false)

    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_product_journal
        |select
        |achievement_id,
        |'1' as paper_type,
        |chinese_title as chinese_name,
        |english_title as english_name,
        |authors,
        |publish_date,
        |language,
        |includes,
        |journal,
        |flow_source,
        |source
        | from dwb.wb_product_journal_csai_nsfc_ms_orcid
      """.stripMargin)



    //会议相关

    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_product_conference
        |select
        |achievement_id,
        |'2' as paper_type,
        |chinese_title as chinese_name,
        |english_title as english_name,
        |authors,
        |publish_date,
        |language,
        |include,
        |conference,
        |flow_source,
        |source
        | from dwb.wb_product_conference_ms_nsfc_orcid
      """.stripMargin)



    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_product_patent
        |select
        |achievement_id,
        |"5" as paper_type,
        |chinese_title as chinese_name,
        |english_title as english_name,
        |inventor as  authors,
        |language,
        |applicant,
        |flow_source,
        |source
        | from dwb.wb_product_patent_csai_nsfc_ms
      """.stripMargin)



    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_product_monograph
        |select
        |achievement_id,
        |"4" as paper_type,
        |chinese_title as chinese_name,
        |english_title as english_name,
        |author as  authors,
        |language,
        |book_name,
        |editor,
        |flow_source,
        |source
        | from dwb.wb_product_monograph_csai_nsfc_ms
      """.stripMargin)



    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_product_criterion
        |select
        |achievement_id,
        |"6" as paper_type,
        |chinese_title as chinese_name,
        |english_title as english_name,
        |authors as  authors,
        |language,
        |applicant,
        |flow_source,
        |source
        | from dwb.wb_product_criterion_csai_nsfc
      """.stripMargin)


    spark
    
    
  }
}
