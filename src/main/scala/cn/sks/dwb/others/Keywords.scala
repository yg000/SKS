package cn.sks.dwb.others

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object Keywords {


  val spark: SparkSession = SparkSession.builder()
    //.master("local[40]")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    .appName("Keywords")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")
  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })

  def main(args: Array[String]): Unit = {


    spark.sql(
      """
        |select
        |md5(clean_fusion(zh_keyword)) as keyword_id,
        |zh_keyword,
        |en_keyword,
        |'nsfc' as source
        |from dwd.wd_project_person_keyword_split_nsfc_translated
        |union all
        |select
        |md5(clean_fusion(zh_keyword)) as keyword_id,
        |zh_keyword,
        |en_keyword,
        |'nsfc' as source
        |from dwd.wd_product_keyword_split_nsfc_translated
        |union all
        |select
        |md5(clean_fusion(zh_keyword)) as keyword_id,
        |zh_keyword,
        |en_keyword,
        |'csai' as source
        |from ods.o_csai_keyword_translate
        |""".stripMargin).dropDuplicates("keyword_id")
      //.write.format("hive").mode("overwrite").insertInto("dwb.wb_keyword")


    //product_keyword
    spark.sql(
      """
        |select
        |ifnull(achievement_id,md5(product_id)) as achievement_id,
        |md5(clean_fusion(zh_keyword)) as keyword_id
        |from dwd.wd_product_keyword_split_nsfc_translated_en a
        |left join dwb.wb_product_rel b on md5(a.product_id) = b.achievement_id_origin
        |union all
        |select ifnull(achievement_id,achivement_id) as achievement_id,
        |  md5(clean_fusion(keyword)) as keyword_id
        | from ods.o_csai_product_journal_keyword a
        |left join dwb.wb_product_rel b on a.achivement_id= b.achievement_id_origin
        |""".stripMargin).dropDuplicates()
      //.write.format("hive").mode("overwrite").insertInto("dwb.wb_relation_product_keyword")


    //project_keyword
    spark.sql(
      """
        |select
        |md5(code) as project_id,
        |md5(clean_fusion(zh_keyword)) as keyword_id
        |from dwd.wd_project_person_keyword_split_nsfc_translated_en where type in ('1','2')
        |""".stripMargin)
      //.write.format("hive").mode("overwrite").insertInto("dwb.wb_relation_project_keyword")


    //person_keyword
    spark.sql(
      """
        |select
        |ifnull(person_id_to,md5(a.code)) as person_id,
        |md5(clean_fusion(zh_keyword)) as keyword_id
        |from (select * from dwd.wd_project_person_keyword_split_nsfc_translated_en where type in ('3','4'))a
        |left join dwb.wb_person_rel b on md5(a.code)= b.person_id_from
        |""".stripMargin)
      .write.format("hive").mode("overwrite").insertInto("dwb.wb_relation_person_keyword")



    //keyword_subject
    val wb_product_all_subject = spark.read.table("dwb.wb_relation_product_subject")

    val wb_product_all_keyword = spark.read.table("dwb.wb_relation_product_keyword")

    wb_product_all_keyword.join(wb_product_all_subject,Seq("achievement_id"))
      .select("keyword_id","one_rank_id","one_rank_no","one_rank_name","two_rank_id","two_rank_no","two_rank_name").dropDuplicates()
     // .write.format("hive").mode("overwrite").insertInto("dwb.wb_keyword_subject")

  }
}
