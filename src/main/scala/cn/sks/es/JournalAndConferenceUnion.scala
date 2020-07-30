package cn.sks.es

import org.apache.spark.sql.SparkSession

object JournalAndConferenceUnion {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[40]")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("neo4jcsv")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val journal = spark.sql(
      """
        |select
        |journal_id
        |,chinese_name
        |,english_name
        |,former_name
        |,cn
        |,issn
        |,language
        |,url
        |,cover_page
        |,impact_factor
        |,fullimpact
        |,compleximpact
        |,publish_cycle
        |,establishtime
        |,publish_region
        |,field
        |,field_sub
        |,honor
        |,database_include
        |,include
        |from ods.o_csai_journal
      """.stripMargin)

    val journal_org = spark.sql("select journal_id,org_id from dm.dm_neo4j_journal_rel_organization")
    val org = spark.sql("select id as org_id,org_name from dm.dm_es_organization")

    journal_org.join(org, Seq("org_id")).createOrReplaceTempView("journal")


    val journal_2 = spark.sql(
      """
        |select
        |journal_id,
        |concat("[",concat_ws(",",collect_set(to_json(struct(org_name as name)))),"]")  as journal_2
        |from journal group by journal_id
      """.stripMargin)


    journal.join(journal_2, Seq("journal_id"), "left")
      .createOrReplaceTempView("journal_2")

    spark.sql(
      """
        |select
        |journal_id as id
        |,chinese_name
        |,english_name
        |,former_name
        |,cn
        |,issn
        |,language
        |,url
        |,cover_page
        |,journal_2 as organizer
        |,impact_factor
        |,fullimpact
        |,compleximpact
        |,publish_cycle
        |,establishtime
        |,publish_region
        |,field
        |,field_sub
        |,honor
        |,database_include
        |,include
        |from journal_2
      """.stripMargin).repartition(5).
      createOrReplaceTempView("result")

    //  spark.sql("insert overwrite table dm.dm_es_journal   select * from result")

    spark.sql("select conference_id as id,conference,conference_type,conference_address,organization,country,city from ods.o_const_conference")
      .repartition(5).createOrReplaceTempView("co_re")

    spark.sql("insert overwrite table dm.dm_es_conference   select * from co_re")


    spark.stop()
  }
}
