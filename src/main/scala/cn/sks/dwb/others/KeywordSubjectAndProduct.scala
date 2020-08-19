package cn.sks.dwb.others

import org.apache.spark.sql.{Column, SparkSession}

/**
  * 成果的关键词关系（基金委+科协）
  *关键词的学科
  *
  */
object KeywordSubjectAndProduct {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[40]")
      // .config("spark.deploy.mode", "8g")
      //.config("spark.drivermemory", "32g")
      //.config("spark.cores.max", "16")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    //只有期刊
    val nsfc_product_keyword = spark.sql("select  achievement_id,keywords_id as keyword_id from dwb.wb_keywords_product_rel_add_uuid")
    val nsfc_csai_keyword = spark.sql("select keywords_id as keyword_id ,zh_keywords as keyword from dwb.wb_keywords_nsfc_csai_all")

    val csai_journal_keyword = spark.sql("select achivement_id as achievement_id,keyword_id,keyword from ods.o_csai_product_journal_keyword")


    val nsfc_keyword = nsfc_product_keyword.join(nsfc_csai_keyword, Seq("keyword_id"))
      .select("achievement_id", "keyword_id", "keyword")


    val keyword_all = csai_journal_keyword.union(nsfc_keyword)

    val product_id = spark.sql("select achievement_id as id,original_achievement_id as achievement_id from dwb.wb_product_all_rel")

    keyword_all.join(product_id, Seq("achievement_id"), "left")
      .createOrReplaceTempView("keyword")

    val col: Column = new Column("original_achievement_id")

    val product_all_keyword = spark.sql(
      """
        |select
        |if(id is null,achievement_id,id) as achievement_id,
        |id as original_achievement_id,
        |keyword_id,
        |keyword
        |from keyword
      """.stripMargin).filter(col.isNull).drop("original_achievement_id")

    val journal = spark.sql("select achievement_id from dwb.wb_product_journal_ms_csai_nsfc_orcid")
    val conference = spark.sql("select achievement_id from dwb.wb_product_conference_ms_nsfc_orcid")
    product_all_keyword.join(journal, Seq("achievement_id")).createOrReplaceTempView("journal")

  val journal_2= spark.sql(
      """
        |select
        |achievement_id,
        |keyword_id,
        |keyword,
        |"journal" as type
        |from journal
      """.stripMargin)

    product_all_keyword.join(conference, Seq("achievement_id")).createOrReplaceTempView("conference")


    val conference_2= spark.sql(
      """
        |select
        |achievement_id,
        |keyword_id,
        |keyword,
        |"conference" as type
        |from conference
      """.stripMargin)

    journal_2.union(conference_2).dropDuplicates().repartition(200)
      .createOrReplaceTempView("keyword")


    spark.sql("insert overwrite table dwb.wb_product_all_keyword select * from keyword")

    val wb_product_all_subject = spark.sql("select * from dwb.wb_product_all_subject")

    val wb_product_all_keyword = spark.sql("select * from dwb.wb_product_all_subject")

    wb_product_all_keyword.join(wb_product_all_subject,Seq("achievement_id"))
      .select("keyword_id","keyword","one_rank_id","one_rank_no","one_rank_name"
      ,"two_rank_id","two_rank_no","two_rank_name").dropDuplicates()
      .createOrReplaceTempView("keyword_re")


    spark.sql("insert overwrite table dwb.wb_keyword_subject select * from keyword_re")


  }
}
