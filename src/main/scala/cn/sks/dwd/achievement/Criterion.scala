package cn.sks.dwd.achievement

import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.{AchievementUtil, DefineUDF, NameToPinyinUtil}
import cn.sks.jutil.H2dbUtil
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
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("Criterion")
      .enableHiveSupport()
      .getOrCreate()
    //spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)
    spark.udf.register("clean_separator", DefineUDF.clean_separator _)

    //项目产出成果===================

    spark.sql(
      """
        |select  achievement_id,concat_ws(';',collect_set(chinese_name)) as authors  from
        | (select  achievement_id, person_id  from dwb.wb_product_all_person where product_type ='6') a  join  ods.o_csai_person_all b on a.person_id = b.person_id group by achievement_id
      """.stripMargin).createOrReplaceTempView("csai_criterion_authors")



    val product_csai = spark.sql(
      """
        |select
        | a.achievement_id
        |,chinese_name  as chinese_title
        |,english_name  as englisth_title
        |,status
        |,release_date  as publish_date
        |,apply_date  as implement_date
        |,abolish_date  as abolish_date
        |,criterion_no  as criterion_no
        |,ch_criterion_no  as china_citerion_classification_no
        |,in_criterion_no  as in_criterion_classification_no
        |,if(regexp_replace(chinese_name,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,draft_org  as applicant
        |,b.authors  as authors
        |,charge_department
        |,responsibility_department
        |,publish_org  as publish_agency
        |,"0"  as hasFullText
        |,null  as fulltext_url
        |,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_product_criterion_csai\"","," ,"\"id\"",":","\"",a.achievement_id,"\"","}") as flow_source
        |,'csai' as source
        |from ods.o_csai_criterion a left join csai_criterion_authors b on a.achievement_id  = b.achievement_id
      """.stripMargin)

    product_csai.repartition(10).createOrReplaceTempView("wd_product_criterion_csai")

    //spark.sql("insert overwrite  table dwd.wd_product_criterion_csai  select * from wd_product_criterion_csai")
    AchievementUtil.getDataTrace(spark,"ods.o_csai_criterion","dwd.wd_product_criterion_csai")
    AchievementUtil.getDataTrace(spark,"dwb.wb_product_all_person","dwd.wd_product_criterion_csai")
    val product_nsfc_person = spark.sql(
      """
        |select
        | achievement_id
        |,clean_div(zh_title)  as chinese_title
        |,clean_div(en_title)  as englisth_title
        |,null status
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,null  as implement_date
        |,null  as abolish_date
        |,criterion_no  as criterion_no
        |,null  as china_citerion_classification_no
        |,null  as in_criterion_classification_no
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,null  as applicant
        |,clean_separator(clean_div(authors)) as authors
        |,null as charge_department
        |,null as responsibility_department
        |,published_angenties  as publish_agency
        |,"0"  as hasFullText
        |,null as fulltext_url
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_criterion_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_product_criterion
      """.stripMargin)

    product_nsfc_person.repartition(1).createOrReplaceTempView("wd_product_criterion_nsfc")
    //spark.sql("insert overwrite  table dwd.wd_product_criterion_nsfc  select * from wd_product_criterion_nsfc")
    AchievementUtil.getDataTrace(spark,"ods.o_nsfc_product_criterion","dwd.wd_product_criterion_nsfc")




    spark.stop()


  }
}
