package cn.sks.dm.achievement

import cn.sks.dm.achievement.RelProductOthers.spark
import cn.sks.dm.organization.RelOrganization.spark
import org.apache.spark.sql.SparkSession

object RelProduct {
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
    .appName("RelProduct")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {


    spark.read.table("dm.dm_neo4j_product_journal").select("id","paper_type")
      .unionAll(spark.read.table("dm.dm_neo4j_product_conference").select("id","paper_type"))
      .unionAll(spark.read.table("dm.dm_neo4j_product_patent").select("id","paper_type"))
      .unionAll(spark.read.table("dm.dm_neo4j_product_monograph").select("id","paper_type"))
      .unionAll(spark.read.table("dm.dm_neo4j_product_criterion").select("id","paper_type")).createOrReplaceTempView("product_tb")



//    val conference = spark.read.table("dwb.wb_product_conference_ms_nsfc_orcid").select("achievement_id")
//    val journal = spark.read.table("dwb.wb_product_journal_csai_nsfc_ms_orcid").select("achievement_id")
//    //期刊对应得
//    val wb_product_journal_rel_journal = spark.sql("select achievement_id ,journal_id from dwb.wb_product_journal_rel_journal")
//
//    journal.join(wb_product_journal_rel_journal, Seq("achievement_id"))
//      .repartition(30).createOrReplaceTempView("product_journal_re")
//    spark.sql("insert overwrite table dm.dm_neo4j_journal_rel_product_journal select journal_id,achievement_id from product_journal_re")
//
//
//    //会议对应的
//    val wb_product_conference_rel_conference = spark.sql("select achievement_id,conference_id from dwb.wb_product_conference_rel_conference")
//
//    conference.select("achievement_id").join(wb_product_conference_rel_conference, Seq("achievement_id"))
//      .repartition(5).createOrReplaceTempView("product_conference_re")
//    spark.sql("insert overwrite table dm.dm_neo4j_conference_rel_product_conference select conference_id,achievement_id from product_conference_re")


    //product_subject
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_product_subject
//        |select a.*
//        | from (select * from dwb.wb_relation_product_subject where two_rank_id is not null)a
//        |join product_tb b on a.achievement_id = b.id
//        |""".stripMargin)
//
//
//    //project_product
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_project_product
//        |select
//        |project_id,
//        |achievement_id
//        | from dwb.wb_relation_product_project a
//        |join product_tb b on a.achievement_id = b.id
//        |""".stripMargin)


    //product_person
    val product_person = spark.read.table("dwb.wb_relation_product_person")
    product_person.createOrReplaceTempView("product_person")

    spark.sql(
      """
        |select person_id
        | ,achievement_id
        | from product_person a
        | join dm.dm_neo4j_person b on a.person_id = b.id
        | join product_tb c on a.achievement_id = c.id
        |""".stripMargin)
      .write.format("hive").mode("overwrite").insertInto("dm.dm_neo4j_person_product")

    //product_keyword

//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_product_keyword
//        |select
//        |distinct achievement_id,
//        |keyword_id
//        |from dwb.wb_relation_product_keyword a
//        |join product_tb b on a.achievement_id = b.id
//        |join dm.dm_neo4j_keyword c on a.keyword_id = c.id
//        |""".stripMargin)


//    //organization_prouduct
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_organization_product
//        |select
//        |organization_id,
//        |achievement_id
//        |from dwb.wb_product_organization a
//        |join product_tb b on a.achievement_id = b.id
//        |""".stripMargin)
  }
}
