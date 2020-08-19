package cn.sks.dwb.achievement

import cn.sks.jutil.H2dbUtil
import cn.sks.util.{AchievementUtil, DefineUDF, NameToPinyinUtil}
import org.apache.spark.sql.{Column, SparkSession}

/*

论文数据的整合的整体的代码

 */
object Monograph {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[12]")
     // .config("spark.deploy.mode", "8g")
      //.config("spark.drivermemory", "32g")
      //.config("spark.cores.max", "16")
      .config("spark.sql.shuffle.partitions","10")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)
    spark.udf.register("clean_separator", DefineUDF.clean_separator _)
    //融合的函数
    spark.udf.register("clean_fusion", DefineUDF.clean_fusion _)
    spark.udf.register("union_flow_source", DefineUDF.unionFlowSource _)

    //数据处理过程中的代码===================
    val product_nsfc_person = spark.read.table("dwd.wd_product_monograph_nsfc")
    val product_nsfc_project = spark.read.table("dwd.wd_product_monograph_project_nsfc")
    val product_nsfc_npd = spark.read.table("dwd.wd_product_monograph_npd_nsfc")
    val product_csai = spark.read.table("dwd.wd_product_monograph_csai")
    val product_ms = spark.read.table("dwd.wd_product_monograph_ms")

    H2dbUtil.useH2("dwd.wd_product_monograph_nsfc","dwb.wb_product_monograph_csai_nsfc")
    H2dbUtil.useH2("dwd.wd_product_monograph_project_nsfc","dwb.wb_product_monograph_csai_nsfc")
    H2dbUtil.useH2("dwd.wd_product_monograph_npd_nsfc","dwb.wb_product_monograph_csai_nsfc")
    H2dbUtil.useH2("dwd.wd_product_monograph_csai","dwb.wb_product_monograph_csai_nsfc")
    H2dbUtil.useH2("dwb.wb_product_monograph_csai_nsfc","dwb.wb_product_monograph_csai_nsfc_ms")
    H2dbUtil.useH2("dwd.wd_product_monograph_ms","dwb.wb_product_monograph_csai_nsfc_ms")
    //将基金委对应的论文成果对应的作者和论文的字段合并到一块儿
    val product_nsfc = product_nsfc_person.union(product_nsfc_project).union(product_nsfc_npd).dropDuplicates("achievement_id")


    val fusion_data_nsfc = AchievementUtil.explodeAuthors(spark,product_nsfc,"author")
    val fushion_data_csai = AchievementUtil.explodeAuthors(spark,product_csai,"author")
    NameToPinyinUtil.nameToPinyin(spark, fusion_data_nsfc, "person_name")
      .createOrReplaceTempView("fushion_data_nsfc_pinyin")
    NameToPinyinUtil.nameToPinyin(spark, fushion_data_csai, "person_name")
      .createOrReplaceTempView("fushion_data_csai_pinyin")
    AchievementUtil.getComparisonTable(spark,"fushion_data_nsfc_pinyin","fushion_data_csai_pinyin")
      .createOrReplaceTempView("wb_product_monograph_csai_nsfc_rel")

    spark.sql("insert overwrite table dwb.wb_product_monograph_csai_nsfc_rel  select achievement_id_to,achievement_id_from,product_type,source  from wb_product_monograph_csai_nsfc_rel")

    AchievementUtil.getSource(spark,"wb_product_monograph_csai_nsfc_rel").createOrReplaceTempView("get_source")

   product_nsfc.union(product_csai).createOrReplaceTempView("o_product_monograph_csai_nsfc")

    spark.sql(
      """
        |select
        |a.achievement_id
        |,chinese_title
        |,english_title
        |,author
        |,publish_date
        |,hasFullText
        |,book_name
        |,bookSeriesName
        |,language
        |,status
        |,isbn
        |,editor
        |,page_range
        |,word_count
        |,publisher
        |,if(b.source is not null, union_flow_source(b.source,flow_source),flow_source  )as flow_source
        |,a.source
        |from o_product_monograph_csai_nsfc a left join get_source b on a.achievement_id = b.achievement_id
      """.stripMargin).dropDuplicates("achievement_id").createOrReplaceTempView("product_monograph_csai_nsfc_get_source")


    spark.sql(
      """
        |insert overwrite table dwb.wb_product_monograph_csai_nsfc
        |select a.*
        |from product_monograph_csai_nsfc_get_source a left join  dwb.wb_product_monograph_csai_nsfc_rel b on a.achievement_id = b.achievement_id_nsfc where b.achievement_id_nsfc is null
        |""".stripMargin)


// csai_nsfc_ms

    val product_csai_nsfc = spark.read.table("dwb.wb_product_monograph_csai_nsfc")

    val fushion_data_csai_nsfc = AchievementUtil.explodeAuthors(spark,product_csai_nsfc,"author")

    NameToPinyinUtil.nameToPinyin(spark, fushion_data_csai_nsfc, "person_name")
      .createOrReplaceTempView("fushion_data_csai_nsfc_pinyin")

   // spark.sql("insert overwrite table dwd.wd_product_fusion_data_monograph_ms_csai_nsfc  select * from fushion_data_csai_nsfc_pinyin")

    val fushion_data_ms = AchievementUtil.explodeAuthors(spark,product_ms,"author")

    NameToPinyinUtil.nameToPinyin(spark, fushion_data_ms, "person_name")
      .createOrReplaceTempView("fushion_data_ms_pinyin")

    AchievementUtil.getComparisonTable(spark,"fushion_data_ms_pinyin","fushion_data_csai_nsfc_pinyin").createOrReplaceTempView("wb_product_monograph_csai_nsfc_ms_rel")

    spark.sql("insert overwrite table dwb.wb_product_monograph_csai_nsfc_ms_rel  select achievement_id_to,achievement_id_from,product_type,source  from wb_product_monograph_csai_nsfc_ms_rel")

    AchievementUtil.getSource(spark,"wb_product_monograph_csai_nsfc_ms_rel").createOrReplaceTempView("get_source")

    product_csai_nsfc.union(product_ms).createOrReplaceTempView("o_product_monograph")


    spark.sql(
      """
        |select
        |a.achievement_id
        |,chinese_title
        |,english_title
        |,author
        |,publish_date
        |,hasFullText
        |,book_name
        |,bookSeriesName
        |,language
        |,status
        |,isbn
        |,editor
        |,page_range
        |,word_count
        |,publisher
        |,if(b.source is not null, union_flow_source(b.source,flow_source),flow_source  )as flow_source
        |,a.source
        |from o_product_monograph a left join get_source b on a.achievement_id = b.achievement_id
      """.stripMargin).dropDuplicates("achievement_id").createOrReplaceTempView("product_monograph_csai_nsfc_ms_get_source")


    spark.sql(
      """
        |insert overwrite table dwb.wb_product_monograph_csai_nsfc_ms
        |select a.*
        |from product_monograph_csai_nsfc_ms_get_source a left join  dwb.wb_product_monograph_csai_nsfc_ms_rel b on a.achievement_id = b.achievement_id_ms where b.achievement_id_ms is null
        |""".stripMargin)

    spark.stop()


  }
}
