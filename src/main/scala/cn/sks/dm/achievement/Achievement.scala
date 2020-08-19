package cn.sks.dm.achievement

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
        |journal
        | from dwb.wb_product_journal_csai_nsfc_ms_orcid
      """.stripMargin).show()


    //spark.sql("insert overwrite table dm.dm_neo4j_product_journal select * from journal_person_re")


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
        |conference
        | from dwb.wb_product_conference_ms_nsfc_orcid
      """.stripMargin).show()


    //spark.sql("insert overwrite table dm.dm_neo4j_product_conference select * from conference_person_re")

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
        |applicant
        | from dwb.wb_product_patent_csai_nsfc_ms
      """.stripMargin).show()

    //spark.sql("insert overwrite table dm.dm_neo4j_product_patent select * from patent")

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
        |editor
        | from dwb.wb_product_monograph_csai_nsfc_ms
      """.stripMargin).show()

    //spark.sql("insert overwrite table dm.dm_neo4j_product_monograph select * from monograph")

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
        |applicant
        | from dwb.wb_product_criterion_csai_nsfc
      """.stripMargin).show()

    //spark.sql("insert overwrite table dm.dm_neo4j_product_criterion select * from criterion")
  }
}
