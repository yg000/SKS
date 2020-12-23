package cn.sks.dwd.achievement_new

import cn.sks.util.{AchievementUtil, DefineUDF}
import org.apache.spark.sql.SparkSession

object achievement_nsfc_journal {
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
        |insert overwrite table dwd.wd_achievement_nsfc partition(achievement_type = 'journal')
        |select
        |achievement_id
        |,clean_title(zh_title)
        |,clean_title(en_title)
        |,clean_authors(authors)
        |,null
        |,null
        |,zh_abstract
        |,doi
        |,split(clean_authors(authors),'#')[0]
        |,null
        |,null
        |,type
        |,get_publish_date(publish_year,publish_month,publish_day)
        |,clean_keywords(zh_keyword)
        |,null
        |,null
        |,null
        |,language
        |,volume              
        |,series              
        |,split(page_range,'-')[0]
        |,split(page_range,'-')[1]
        |,null
        |,citedby_count       
        |,journal_name        
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
        |,'nsfc' from ods.o_nsfc_npd_journal
        |""".stripMargin)

    spark.sql(
      """
        |insert into table dwd.wd_achievement_nsfc partition(achievement_type = 'journal')
        |select
        |achievement_id
        |,clean_title(zh_title)
        |,clean_title(en_title)
        |,clean_authors(authors)
        |,null
        |,null
        |,zh_abstract
        |,doi
        |,split(clean_authors(authors),'#')[0]
        |,null
        |,null
        |,type
        |,get_publish_date(publish_year,publish_month,publish_day)
        |,clean_keywords(zh_keyword)
        |,null
        |,null
        |,null
        |,language
        |,volume
        |,series
        |,split(page_range,'-')[0]
        |,split(page_range,'-')[1]
        |,null
        |,citedby_count
        |,journal_name
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
        |,'nsfc' from ods.o_nsfc_project_journal
        |""".stripMargin)

    spark.sql(
      """
        |insert overwrite table dwd.wd_achievement_nsfc partition(achievement_type = 'journal')
        |select
        |achievement_id
        |,clean_title(zh_title)
        |,clean_title(en_title)
        |,clean_authors(authors)
        |,null
        |,null
        |,zh_abstract
        |,doi
        |,split(clean_authors(authors),'#')[0]
        |,null
        |,null
        |,null
        |,get_publish_date(publish_year,publish_month,publish_day)
        |,clean_keywords(zh_keyword)
        |,include_other
        |,null
        |,null
        |,language
        |,volume
        |,series
        |,split(page_range,'-')[0]
        |,split(page_range,'-')[1]
        |,null
        |,citedby_count
        |,journal_name
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
        |,'nsfc' from ods.o_nsfc_product_journal
        |""".stripMargin)
  }
}

