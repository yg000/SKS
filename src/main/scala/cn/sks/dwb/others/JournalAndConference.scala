package cn.sks.dwb.others

import org.apache.spark.sql.SparkSession

/**
  * 期刊对应得成果抽取
  * 会议对应得成果抽取和会议得常量表得生成
  */
object JournalAndConference {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")

      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("JournalAndConference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")



    val product_id = spark.sql("select achievement_id as id,achievement_id_origin as achievement_id from dwb.wb_product_rel")

    //journal

    val paper_journal = spark.sql("select achievement_id,journal_id,journal_name from ods.o_csai_product_journal_relationship_journal")
    paper_journal.join(product_id, Seq("achievement_id"), "left")
      .createOrReplaceTempView("journal")


    spark.sql(
      """
        |select
        |ifnull(id,achievement_id) as achievement_id,
        |journal_id,
        |journal_name
        |from journal
      """.stripMargin).dropDuplicates()
      .repartition(40).createOrReplaceTempView("product_journal_rel_journal")


    spark.sql("insert overwrite table dwb.wb_product_journal_rel_journal select * from product_journal_rel_journal")

    //conference
   val conference= spark.sql("select achievement_id,md5(conference) as conference_id,conference,conference_type,conference_address,organization,start_date,end_date,country,city from dwb.wb_product_conference_ms_nsfc_orcid where conference is not null and conference!='' ")


    conference.select("achievement_id","conference_id","conference").repartition(10).createOrReplaceTempView("conference")

    spark.sql("insert overwrite table dwb.wb_product_conference_rel_conference select * from conference")

    conference.drop("achievement_id").dropDuplicates("conference_id")
      .repartition(5).createOrReplaceTempView("conference_2")

    spark.sql("insert overwrite table ods.o_const_conference select * from conference_2")
















  }
}
