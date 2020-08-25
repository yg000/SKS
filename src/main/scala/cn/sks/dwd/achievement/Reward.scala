package cn.sks.dwd.achievement

import cn.sks.util.DefineUDF
import org.apache.spark.sql.{Column, SparkSession}

/*

论文数据的整合的整体的代码

 */
object Reward {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[12]")
      .config("spark.deploy.mode", "8g")
      .config("spark.drivermemory", "32g")
      .config("spark.cores.max", "16")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)

    //项目产出成果===================
//    spark.sql(
//      """
//        |select
//        | achievement_id
//        |,clean_div(zh_title) as chinese_title
//        |,clean_div(en_title) as english_title
//        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
//        |,clean_div(authors) as project_owner
//        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day)))))  date
//        |,reward_type_name as reward_type
//        |,reward_type as reward_level
//        |,reward_rank_name as reward_rank
//        |,issued_by
//        |,reward_number as project_no
//        |,"nsfc" as source
//        |from ods.o_product_scientific_reward_nsfc
//      """.stripMargin).repartition(4).createOrReplaceTempView("wd_product_reward_nsfc")
//
//     spark.sql("insert overwrite  table dwd.wd_product_reward_nsfc   select * from wd_product_reward_nsfc")
//
//
//    spark.sql(
//      """
//        |select
//        | achievement_id
//        |,clean_div(zh_title) as chinese_title
//        |,clean_div(en_title) as english_title
//        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
//        |,clean_div(authors) as project_owner
//        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day)))))  date
//        |,reward_type_name as reward_type
//        |,null as reward_level
//        |,null as reward_rank
//        |,issued_by
//        |,reward_number as project_no
//        |,"nsfc" as source
//        |from ods.o_product_business_award_nsfc
//      """.stripMargin).repartition(2).createOrReplaceTempView("wd_product_reward_business_nsfc")
//
//      spark.sql("insert overwrite  table dwd.wd_product_reward_business_nsfc   select * from wd_product_reward_business_nsfc")
//
//
//    spark.sql(
//      """
//        |select
//        | achievement_id
//        |,clean_div(zh_title) as chinese_title
//        |,clean_div(en_title) as english_title
//        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
//        |,clean_div(authors) as project_owner
//        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day)))))  date
//        |,reward_type_name as reward_type
//        |,null as reward_level
//        |,null as reward_rank
//        |,issued_by
//        |,reward_number as project_no
//        |,"nsfc" as source
//        |from ods.o_npd_award_nsfc
//      """.stripMargin).repartition(3).createOrReplaceTempView("wd_product_reward_npd_nsfc")
//
//    spark.sql("insert overwrite  table dwd.wd_product_reward_npd_nsfc   select * from wd_product_reward_npd_nsfc")

    //csai
    spark.sql(
      """
        |select achivement_id as achievement_id,concat_ws(';',collect_set(person_name)) as authors from ods.o_csai_product_journal_author group by achivement_id
        |""".stripMargin).createOrReplaceTempView("csai_authors")

    spark.sql(
      """
        |select
        | a.achievement_id
        |,clean_div(chinese_name) as chinese_title
        |,null as english_title
        |,if(regexp_replace(chinese_name,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,b.authors as project_owner
        |,reward_date as date
        |,reward_type
        |,null as reward_level
        |,reward_rank
        |,reward_recommended_org issued_by
        |,project_no
        |,"csai" as source
        |from ods.o_csai_reward_project a left join csai_authors b on a.achievement_id  = b.achievement_id
      """.stripMargin).repartition(1).createOrReplaceTempView("wd_product_reward_csai")

    spark.sql("insert overwrite  table dwd.wd_product_reward_csai   select * from wd_product_reward_csai")


    spark.stop()


  }
}
