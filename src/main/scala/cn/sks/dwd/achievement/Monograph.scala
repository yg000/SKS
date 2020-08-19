package cn.sks.dwd.achievement

import cn.sks.jutil.H2dbUtil
import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.{AchievementUtil, DefineUDF, NameToPinyinUtil}

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
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)
    spark.udf.register("clean_separator", DefineUDF.clean_separator _)

    //数据处理过程中的代码===================
    val product_nsfc_person = spark.sql(
      """
        |select
        |achievement_id
        |,clean_div(zh_title)  as chinese_title
        |,clean_div(en_title)  as english_title
        |,clean_separator(clean_div(authors))  as authors
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,"0"  as hasFullText
        |,book_name  as book_name
        |,book_series_name  as bookSeriesName
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,status
        |,isbn
        |,editor
        |,page_range
        |,word_count
        |,publisher
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_monograph_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_product_monograph
      """.stripMargin)


    product_nsfc_person.repartition(5).createOrReplaceTempView("wd_product_monograph_nsfc")
    //spark.sql("insert overwrite  table dwd.wd_product_monograph_nsfc   select * from wd_product_monograph_nsfc")
    AchievementUtil.getDataTrace(spark,"ods.o_nsfc_product_monograph","dwd.wd_product_monograph_nsfc")

    val product_nsfc_project = spark.sql(
      """
        |select
        |achievement_id
        |,clean_div(zh_title)  as chinese_title
        |,clean_div(en_title)  as english_title
        |,clean_separator(clean_div(authors))  as author
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,"0"  as hasFullText
        |,book_name  as book_name
        |,book_series_name  as bookSeriesName
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,status
        |,isbn
        |,editor
        |,page_range
        |,word_count
        |,publisher
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_monograph_project_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_project_monograph
      """.stripMargin)

    product_nsfc_project.repartition(1).createOrReplaceTempView("wd_product_monograph_business_nsfc")
    //spark.sql("insert overwrite  table dwd.wd_product_monograph_project_nsfc  select * from wd_product_monograph_business_nsfc")
    AchievementUtil.getDataTrace(spark,"ods.o_nsfc_project_monograph","dwd.wd_product_monograph_project_nsfc")

    val product_nsfc_npd = spark.sql(
      """
        |select
        |achievement_id
        |,clean_div(zh_title)  as chinese_title
        |,clean_div(en_title)  as english_title
        |,clean_separator(clean_div(authors))  as author
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,"0"  as hasFullText
        |,book_name  as book_name
        |,book_series_name  as bookSeriesName
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,status
        |,isbn
        |,editor
        |,page_range
        |,word_count
        |,publisher
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_monograph_npd_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_npd_monograph
      """.stripMargin)

    product_nsfc_npd.repartition(2).createOrReplaceTempView("wd_product_monograph_npd_nsfc")
    //spark.sql("insert overwrite  table dwd.wd_product_monograph_npd_nsfc  select * from wd_product_monograph_npd_nsfc")
    AchievementUtil.getDataTrace(spark,"ods.o_nsfc_npd_monograph","dwd.wd_product_monograph_npd_nsfc")


    spark.sql(
      """
        |select achievement_id,concat_ws(';',collect_set(person_name)) as authors from ods.o_csai_product_monograph_author group by achievement_id
        |""".stripMargin).createOrReplaceTempView("csai_monograph_authors")

    val product_csai = spark.sql(
      """
        |select
        |a.achievement_id
        |,chinese_name  as chinese_title
        |,null  as english_title
        |,b.authors as author
        |,publish_time as  publish_date
        |,"0"  as hasFullText
        |,null  as book_name
        |,null  as bookSeriesName
        |,if(regexp_replace(chinese_name,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,null as status
        |,null as isbn
        |,null as editor
        |,null as page_range
        |,null as word_count
        |,publish publisher
        |,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_product_monograph_csai\"","," ,"\"id\"",":","\"",a.achievement_id,"\"","}") as flow_source
        |,'csai' as source
        |from ods.o_csai_product_monograph a left join csai_monograph_authors b on a.achievement_id  = b.achievement_id
      """.stripMargin)


    product_csai.repartition(1).createOrReplaceTempView("wd_product_monograph_csai")
    //spark.sql("insert overwrite  table dwd.wd_product_monograph_csai  select * from wd_product_monograph_csai")
    AchievementUtil.getDataTrace(spark,"ods.o_csai_product_monograph","dwd.wd_product_monograph_csai")
    AchievementUtil.getDataTrace(spark,"ods.o_csai_product_monograph_author","dwd.wd_product_monograph_csai")

    //ms
    spark.sql(
      """
        |select achivement_id as achievement_id,concat_ws(';',collect_set(person_name)) as authors from ods.o_ms_product_author group by achievement_id
        |""".stripMargin).createOrReplaceTempView("ms_authors")

    val product_ms = spark.sql(
      """
        |select
        |a.achievement_id
        |,clean_div(english_name)  as chinese_title
        |,clean_div(english_name)  as english_title
        |,b.authors as author
        |,publish_date
        |,"0"  as hasFullText
        |,null  as book_name
        |,null  as bookSeriesName
        |,if(regexp_replace(english_name,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,null as status
        |,null as isbn
        |,null as editor
        |,null as page_range
        |,null as word_count
        |,null as publisher
        |,concat("{","\"source\"",":","\"ms\"",",""\"table\"",":","\"wd_product_monograph_ms\"","," ,"\"id\"",":","\"",a.achievement_id,"\"","}") as flow_source
        |,'ms' as source
        |from (select * from dwd.wd_product_ms_all where paper_type='4' or paper_type='5') a left join ms_authors b on a.achievement_id  = b.achievement_id
      """.stripMargin)
    product_ms.repartition(10).createOrReplaceTempView("product_ms")
    spark.sql("insert overwrite  table dwd.wd_product_monograph_ms  select * from product_ms")
    AchievementUtil.getDataTrace(spark,"dwd.wd_product_ms_all","dwd.wd_product_monograph_ms")
    AchievementUtil.getDataTrace(spark,"ods.o_ms_product_author","dwd.wd_product_monograph_ms")


    spark.stop()


  }
}
