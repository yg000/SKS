package cn.sks.dwb.achievement

import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.{DefineUDF,NameToPinyinUtil,AchievementUtil}

/*

论文数据的整合的整体的代码

 */
object Criterion {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      //      .config("spark.deploy.mode", "8g")
      //      .config("spark.drivermemory", "32g")
      //      .config("spark.cores.max", "16")
      .config("spark.sql.shuffle.partitions","10")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("Criterion")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)
    spark.udf.register("clean_separator", DefineUDF.clean_separator _)
    spark.udf.register("clean_fusion", DefineUDF.clean_fusion _)

    val product_csai = spark.read.table("dwd.wd_product_criterion_csai")
    val product_nsfc_person = spark.read.table("dwd.wd_product_criterion_nsfc")


    //数据融合

    val fusion_data_nsfc = AchievementUtil.explodeAuthors(spark,product_nsfc_person,"authors")
    val fushion_data_csai = AchievementUtil.explodeAuthors(spark,product_csai,"authors")
    NameToPinyinUtil.nameToPinyin(spark, fusion_data_nsfc, "person_name")
      .createOrReplaceTempView("fushion_data_nsfc_pinyin")
    NameToPinyinUtil.nameToPinyin(spark, fushion_data_csai, "person_name")
      .createOrReplaceTempView("fushion_data_csai_pinyin")

    AchievementUtil.getComparisonTable(spark,"fushion_data_nsfc_pinyin","fushion_data_csai_pinyin").createOrReplaceTempView("wb_product_criterion_csai_nsfc_rel")

    spark.sql("insert overwrite table dwb.wb_product_criterion_csai_nsfc_rel  select * from wb_product_criterion_csai_nsfc_rel")

    AchievementUtil.getSource(spark,"wb_product_criterion_csai_nsfc_rel").createOrReplaceTempView("get_source")

    product_csai.union(product_nsfc_person).createOrReplaceTempView("o_product_criterion")
    spark.sql(
      """
        |select
        |a.achievement_id
        |,chinese_title
        |,englisth_title
        |,status
        |,publish_date
        |,implement_date
        |,abolish_date
        |,criterion_no
        |,china_citerion_classification_no
        |,in_criterion_classification_no
        |,language
        |,applicant
        |,authors
        |,charge_department
        |,responsibility_department
        |,publish_agency
        |,hasFullText
        |,fulltext_url
        |,ifnull(b.source,flow_source) as flow_source
        |,a.source
        |from o_product_criterion a left join get_source b on a.achievement_id = b.achievement_id
        |""".stripMargin).dropDuplicates("achievement_id").createOrReplaceTempView("product_criterion_get_source")

    spark.sql(
      """
        |insert overwrite table dwb.wb_product_criterion_csai_nsfc
        |select a.*
        |from product_criterion_get_source a left join  dwb.wb_product_criterion_csai_nsfc_rel b on a.achievement_id = b.achievement_id_nsfc where b.achievement_id_nsfc is null
        |""".stripMargin)


    spark.stop()


  }
}
