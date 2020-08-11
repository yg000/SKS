package cn.sks.dwb.achievement

import cn.sks.util.{AchievementUtil, DefineUDF, NameToPinyinUtil}
import org.apache.spark.sql.{Column, SparkSession}

/*

论文数据的整合的整体的代码

 */
object PaperConference {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[40]")
      .config("spark.deploy.mode", "clent")
      .config("executor-memory", "12g")
      .config("executor-cores", "6")
      .config("spark.local.dir","/data/tmp")
      //      .config("spark.drivermemory", "32g")
      //      .config("spark.cores.max", "16")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .config("spark.sql.shuffle.partitions","120")
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)
    spark.udf.register("clean_separator", DefineUDF.clean_separator _)
    //融合的函数
    spark.udf.register("clean_fusion", DefineUDF.clean_fusion _)
    spark.udf.register("union_flow_source", DefineUDF.unionFlowSource _)

    //项目产出成果===================
    val product_nsfc_person = spark.read.table("dwd.wd_product_conference_nsfc")
    val product_nsfc_project = spark.read.table("dwd.wd_product_conference_project_nsfc")
    val product_nsfc_npd = spark.read.table("dwd.wd_product_conference_npd_nsfc")
    val product_ms =  spark.read.table("dwd.wd_product_conference_ms")
    val product_orcid =  spark.read.table("dwd.wd_product_conference_orcid ")
    //将基金委对应的论文成果对应的作者和论文的字段合并到一块儿


    val product_nsfc = product_nsfc_person.union(product_nsfc_project).union(product_nsfc_npd).dropDuplicates("achievement_id")

    val fusion_data_nsfc = AchievementUtil.explodeAuthors(spark,product_nsfc,"authors")
    val fushion_data_ms = AchievementUtil.explodeAuthors(spark,product_ms,"authors")
    NameToPinyinUtil.nameToPinyin(spark, fusion_data_nsfc, "person_name")
      .createOrReplaceTempView("fushion_data_nsfc_pinyin")
    NameToPinyinUtil.nameToPinyin(spark, fushion_data_ms, "person_name")
      .createOrReplaceTempView("fushion_data_ms_pinyin")
    AchievementUtil.getComparisonTable(spark,"fushion_data_nsfc_pinyin","fushion_data_ms_pinyin")
      .createOrReplaceTempView("wb_product_conference_ms_nsfc_rel")

    spark.sql("insert overwrite table dwb.wb_product_conference_ms_nsfc_rel  select achievement_id_to,achievement_id_from,product_type,source  from wb_product_conference_ms_nsfc_rel")

    AchievementUtil.getSource(spark,"wb_product_conference_ms_nsfc_rel").createOrReplaceTempView("get_source")

    product_nsfc.union(product_ms).createOrReplaceTempView("o_product_conference_ms_nsfc")


    spark.sql(
      """
        |select
        |a.achievement_id
        |,product_type
        |,chinese_title
        |,english_title
        |,doi
        |,first_author
        |,first_author_id
        |,correspondent_author
        |,authors
        |,fund_project
        |,publish_date
        |,article_no
        |,article_type
        |,keyword
        |,include
        |,reference
        |,paper_rank
        |,language
        |,page_start
        |,page_end
        |,received_time
        |,revised_time
        |,accepted_time
        |,firstonline_time
        |,print_issn
        |,online_issn
        |,paper_award
        |,isFullText
        |,fulltext_url
        |,fulltext_path
        |,abstract
        |,citation
        |,conference
        |,conference_type
        |,conference_address
        |,organization
        |,start_date
        |,end_date
        |,country
        |,city
        |,if(b.source is not null, union_flow_source(b.source,flow_source),flow_source  )as flow_source
        |,a.source
        |from o_product_conference_ms_nsfc a left join get_source b on a.achievement_id = b.achievement_id
      """.stripMargin).dropDuplicates("achievement_id").createOrReplaceTempView("product_conference_ms_nsfc_get_source")


    spark.sql(
      """
        |insert overwrite table dwb.wb_product_conference_ms_nsfc
        |select a.*
        |from product_conference_ms_nsfc_get_source a left join  dwb.wb_product_conference_ms_nsfc_rel b on a.achievement_id = b.achievement_id_nsfc where b.achievement_id_nsfc is null
        |""".stripMargin)

    // ms_nsfc_orcid

    val product_ms_nsfc = spark.read.table("dwb.wb_product_conference_ms_nsfc")

    val fushion_data_ms_nsfc = AchievementUtil.explodeAuthors(spark,product_ms_nsfc,"authors")

    NameToPinyinUtil.nameToPinyin(spark, fushion_data_ms_nsfc, "person_name")
      .createOrReplaceTempView("fushion_data_ms_nsfc_pinyin")

    // spark.sql("insert overwrite table dwd.wd_product_fusion_data_monograph_orcid_ms_nsfc  select * from fushion_data_ms_nsfc_pinyin")

    val fushion_data_orcid = AchievementUtil.explodeAuthors(spark,product_orcid,"authors")

    NameToPinyinUtil.nameToPinyin(spark, fushion_data_orcid, "person_name")
      .createOrReplaceTempView("fushion_data_orcid_pinyin")

    AchievementUtil.getComparisonTable(spark,"fushion_data_ms_nsfc_pinyin","fushion_data_orcid_pinyin").createOrReplaceTempView("wb_product_conference_ms_nsfc_orcid_rel")

    spark.sql("insert overwrite table dwb.wb_product_conference_ms_nsfc_orcid_rel  select achievement_id_to,achievement_id_from,product_type,source  from wb_product_conference_ms_nsfc_orcid_rel")

    AchievementUtil.getSource(spark,"wb_product_conference_ms_nsfc_orcid_rel").createOrReplaceTempView("get_source")


    product_ms_nsfc.union(product_orcid).createOrReplaceTempView("o_product_conference")

    spark.sql(
      """
        |select
        |a.achievement_id
        |,product_type
        |,chinese_title
        |,english_title
        |,doi
        |,first_author
        |,first_author_id
        |,correspondent_author
        |,authors
        |,fund_project
        |,publish_date
        |,article_no
        |,article_type
        |,keyword
        |,include
        |,reference
        |,paper_rank
        |,language
        |,page_start
        |,page_end
        |,received_time
        |,revised_time
        |,accepted_time
        |,firstonline_time
        |,print_issn
        |,online_issn
        |,paper_award
        |,isFullText
        |,fulltext_url
        |,fulltext_path
        |,abstract
        |,citation
        |,conference
        |,conference_type
        |,conference_address
        |,organization
        |,start_date
        |,end_date
        |,country
        |,city
        |,if(b.source is not null, union_flow_source(b.source,flow_source),flow_source  )as flow_source
        |,a.source
        |from o_product_conference a left join get_source b on a.achievement_id = b.achievement_id
      """.stripMargin).dropDuplicates("achievement_id").createOrReplaceTempView("product_conference_ms_nsfc_orcid_get_source")


    spark.sql(
      """
        |insert overwrite table dwb.wb_product_conference_ms_nsfc_orcid
        |select a.*
        |from product_conference_ms_nsfc_orcid_get_source a left join  dwb.wb_product_conference_ms_nsfc_orcid_rel b on a.achievement_id = b.achievement_id_orcid where b.achievement_id_orcid is null
        |""".stripMargin)


    spark.stop()


  }
}
