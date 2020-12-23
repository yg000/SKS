package cn.sks.dwd.achievement_new

import cn.sks.util.{AchievementUtil, DefineUDF}
import org.apache.spark.sql.SparkSession

object achievement_nsfc_conference {
  val spark = SparkSession.builder()
    .master("local[40]")
    .appName("achievement_corpus  ")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("spark.local.dir", "/data/tmp")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    //.config("spark.sql.shuffle.partitions","10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })
  spark.sqlContext.udf.register("isContainChinese",(str:String) =>{
    DefineUDF.isContainChinese(str)
  })

  spark.sqlContext.udf.register("clean_authors",(str:String) =>{
    AchievementUtil.cleanNsfcAuthors(AchievementUtil.cleanNsfcAuthors(str))
  })
  spark.sqlContext.udf.register("clean_keywords",(str:String) =>{
    AchievementUtil.cleanNsfcKeywords(str)
  })
  spark.sqlContext.udf.register("clean_title",(str:String) =>{
    AchievementUtil.cleanNsfcTitle(str)
  })
  spark.sqlContext.udf.register("get_publish_date",(year_str:String,month_str:String,day_str:String) =>{
    AchievementUtil.getPublishDate(year_str,month_str,day_str)
    })
  def main(args: Array[String]): Unit = {

    spark.sql(
      """
        |insert overwrite table dwd.wd_achievement_nsfc partition(achievement_type = 'conference')
        |select
        |achievement_id
        |,clean_title(zh_title)
        |,clean_title(en_title)
        |,clean_authors(authors)
        |,null
        |,proceeding_organizer
        |,zh_abstract
        |,doi
        |,split(clean_authors(authors),'#')[0]
        |,null
        |,null
        |,article_type
        |,get_publish_date(publish_year,publish_month,publish_day)
        |,clean_keywords(zh_keyword)
        |,null
        |,null
        |,null
        |,language
        |,null
        |,null
        |,split(page_range,'-')[0]
        |,split(page_range,'-')[1]
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,proceeding_name
        |,proceeding_type
        |,proceeding_address
        |,start_time
        |,end_time
        |,country
        |,null
        |,city
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null ,'nsfc' from ods.o_nsfc_npd_conference
        |""".stripMargin)

    spark.sql(
      """
        |insert into table dwd.wd_achievement_nsfc partition(achievement_type = 'conference')
        |select
        |achievement_id
        |,clean_title(zh_title)
        |,clean_title(en_title)
        |,clean_authors(authors)
        |,null
        |,proceeding_organizer
        |,zh_abstract
        |,doi
        |,split(clean_authors(authors),'#')[0]
        |,null
        |,null
        |,article_type
        |,get_publish_date(publish_year,publish_month,publish_day)
        |,clean_keywords(zh_keyword)
        |,null
        |,null
        |,null
        |,language
        |,null
        |,null
        |,split(page_range,'-')[0]
        |,split(page_range,'-')[1]
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,proceeding_name
        |,proceeding_type
        |,proceeding_address
        |,start_time
        |,end_time
        |,country
        |,null
        |,city
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,'nsfc' from ods.o_nsfc_project_conference
        |""".stripMargin)

    spark.sql(
      """
        |insert into table dwd.wd_achievement_nsfc partition(achievement_type = 'conference')
        |select
        |achievement_id
        |,clean_title(zh_title)
        |,clean_title(en_title)
        |,clean_authors(authors)
        |,null
        |,proceeding_organizer
        |,zh_abstract
        |,doi
        |,split(clean_authors(authors),'#')[0]
        |,null
        |,null
        |,article_type
        |,get_publish_date(publish_year,publish_month,publish_day)
        |,clean_keywords(zh_keyword)
        |,null
        |,null
        |,null
        |,language
        |,volume
        |,null
        |,split(page_range,'-')[0]
        |,split(page_range,'-')[1]
        |,null
        |,citedby_count
        |,null
        |,null
        |,null
        |,null
        |,null
        |,proceeding_name
        |,proceeding_type
        |,proceeding_address
        |,start_time
        |,end_time
        |,country
        |,null
        |,city
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,null
        |,'nsfc' from ods.o_nsfc_product_conference
        |""".stripMargin)

  }
}

