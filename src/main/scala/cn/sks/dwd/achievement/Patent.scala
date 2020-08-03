package cn.sks.dwd.achievement

import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.{DefineUDF,NameToPinyinUtil}
/*

论文数据的整合的整体的代码

 */
object Patent {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[15]")
      .config("spark.deploy.mode", "clent")
      .config("executor-memory", "12g")
      .config("executor-cores", "6")
      .config("spark.local.dir","/data/tmp")
      //      .config("spark.drivermemory", "32g")
      //      .config("spark.cores.max", "16")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .config("spark.sql.shuffle.partitions","120")
      .appName("patent")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)
    spark.udf.register("clean_separator", DefineUDF.clean_separator _)

    //nsfc
    //人产出成果
    spark.sql(
      """
        |select
        | achievement_id
        |,clean_div(zh_title)   as chinese_title
        |,clean_div(en_title)   as english_title
        |,null as abstract
        |,null as requirement
        |,doi
        |,clean_separator(clean_div(authors)) as inventor
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,null as apply_date
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) award_date
        |,patent_no
        |,patent_type
        |,null as patent_type_original
        |,country
        |,applicant
        |,null  as application_address
        |,null  as category_no
        |,null  as secondary_category_no
        |,null  as cpic_no
        |,null  as part_no
        |,null  as publish_no
        |,null  as agent
        |,null  as agency
        |,null  as agent_address
        |,patentee
        |,ipc
        |,cpc
        |,issuing_unit as issue_unit
        |,patent_status  as current_status
        |,null  as publish_agency
        |,"0"  as hasfullext
        |,null  as fulltext_url
        |,null as province
        |,null  as one_rank_id
        |,null  as one_rank_no
        |,null  as one_rank_name
        |,null  as two_rank_id
        |,null  as two_rank_no
        |,null  as two_rank_name
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_patent_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_product_patent
      """.stripMargin).repartition(5).createOrReplaceTempView("wd_product_patent_nsfc")

    spark.sql("insert overwrite table dwd.wd_product_patent_nsfc  select * from wd_product_patent_nsfc")

    //项目产出成果
    spark.sql(
      """
        |select
        | achievement_id
        |,clean_div(zh_title)   as chinese_title
        |,clean_div(en_title)   as englisth_title
        |,null as abstract
        |,null as requirement
        |,doi
        |,clean_separator(clean_div(authors)) as inventor
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,null as apply_date
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) award_date
        |,patent_no
        |,patent_type
        |,null as patent_type_original
        |,country
        |,applicant
        |,null  as application_address
        |,null  as category_no
        |,null  as secondary_category_no
        |,null  as cpic_no
        |,null  as part_no
        |,null  as publish_no
        |,null  as agent
        |,null  as agency
        |,null  as agent_address
        |,patentee
        |,ipc
        |,cpc
        |,issuing_unit as issue_unit
        |,patent_status  as current_status
        |,null  as publish_agency
        |,"0"  as hasfullext
        |,null  as fulltext_url
        |,null as province
        |,null  as one_rank_id
        |,null  as one_rank_no
        |,null  as one_rank_name
        |,null  as two_rank_id
        |,null  as two_rank_no
        |,null  as two_rank_name
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_patent_project_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_project_patent
      """.stripMargin).repartition(5).createOrReplaceTempView("wd_product_patent_business_nsfc")

    spark.sql("insert overwrite table dwd.wd_product_patent_project_nsfc  select * from wd_product_patent_business_nsfc")


    spark.sql(
      """
        |select
        | achievement_id
        |,clean_div(zh_title)   as chinese_title
        |,clean_div(en_title)   as englisth_title
        |,null as abstract
        |,null as requirement
        |,doi
        |,clean_separator(clean_div(authors)) as inventor
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,null as apply_date
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) award_date
        |,patent_no
        |,patent_type
        |,null as patent_type_original
        |,country
        |,applicant
        |,null  as application_address
        |,null  as category_no
        |,null  as secondary_category_no
        |,null  as cpic_no
        |,null  as part_no
        |,null  as publish_no
        |,null  as agent
        |,null  as agency
        |,null  as agent_address
        |,patentee
        |,ipc
        |,cpc
        |,issuing_unit as issue_unit
        |,patent_status  as current_status
        |,null  as publish_agency
        |,"0"  as hasfullext
        |,null  as fulltext_url
        |,null as province
        |,null  as one_rank_id
        |,null  as one_rank_no
        |,null  as one_rank_name
        |,null  as two_rank_id
        |,null  as two_rank_no
        |,null  as two_rank_name
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_patent_npd_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_npd_patent
      """.stripMargin).repartition(1).createOrReplaceTempView("wd_product_patent_npd_nsfc")
    spark.sql("insert overwrite table dwd.wd_product_patent_npd_nsfc  select * from wd_product_patent_npd_nsfc")





    //csai

    spark.sql(
      """
        |select achievement_id,concat_ws(';',collect_set(person_name)) as authors from ods.o_csai_product_patent_inventor group by achievement_id
        |""".stripMargin).createOrReplaceTempView("csai_patent_authors")
    spark.sql(
      """
        |select
        | a.achievement_id
        |,chinese_name   as chinese_title
        |,null  as englisth_title
        |,abstract
        |,requirement
        |,null  as doi
        |,b.authors as inventor
        |,if(regexp_replace(chinese_name,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,apply_date
        |,award_date
        |,patent_no
        |,patent_type
        |,patent_type_original
        |,country
        |,applicant
        |,address  as application_address
        |,rank_no  as category_no
        |,rank_no_2  as secondary_category_no
        |,cpic_no  as cpic_no
        |,part_no  as part_no
        |,publish_no  as publish_no
        |,agent_person  as agent
        |,agent_org  as agency
        |,agent_address  as agent_address
        |,null  as patentee
        |,null  as ipc
        |,null  as cpc
        |,null as issue_unit
        |,null  as current_status
        |,null  as publish_agency
        |,"0"  as hasfullext
        |,null  as fulltext_url
        |,province
        |,one_rank_id  as one_rank_id
        |,one_rank_no  as one_rank_no
        |,one_rank_name  as one_rank_name
        |,two_rank_id  as two_rank_id
        |,two_rank_no  as two_rank_no
        |,two_rank_name  as two_rank_name
        |,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_product_patent_csai\"","," ,"\"id\"",":","\"",a.achievement_id,"\"","}") as flow_source
        |,'csai' as source
        |from ods.o_csai_product_patent a left join csai_patent_authors b on a.achievement_id  = b.achievement_id
      """.stripMargin).repartition(10).createOrReplaceTempView("wd_product_patent_csai")

    spark.sql("insert overwrite table dwd.wd_product_patent_csai  select * from wd_product_patent_csai")

    //ms
    spark.sql(
      """
        |select achivement_id as achievement_id,concat_ws(';',collect_set(person_name)) as authors from ods.o_ms_product_author group by achievement_id
        |""".stripMargin).createOrReplaceTempView("ms_authors")

    spark.sql(
      """
        |select
        | a.achievement_id
        |,clean_div(english_name)   as chinese_title
        |,clean_div(english_name)   as englisth_title
        |,abstract
        |,null as requirement
        |,doi
        |,b.authors inventor
        |,if(regexp_replace(english_name,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,null as apply_date
        |,publish_date as  award_date
        |,null as patent_no
        |,null as patent_type
        |,null as patent_type_original
        |,null as country
        |,null as applicant
        |,null  as application_address
        |,null  as category_no
        |,null  as secondary_category_no
        |,null  as cpic_no
        |,null  as part_no
        |,null  as publish_no
        |,null  as agent
        |,null  as agency
        |,null  as agent_address
        |,null as patentee
        |,null as ipc
        |,null as cpc
        |,null as  issue_unit
        |,null as current_status
        |,null  as publish_agency
        |,"0"  as hasfullext
        |,null  as fulltext_url
        |,null as province
        |,null  as one_rank_id
        |,null  as one_rank_no
        |,null  as one_rank_name
        |,null  as two_rank_id
        |,null  as two_rank_no
        |,null  as two_rank_name
        |,concat("{","\"source\"",":","\"ms\"",",""\"table\"",":","\"wd_product_patent_ms\"","," ,"\"id\"",":","\"",a.achievement_id,"\"","}") as flow_source
        |,'ms' as source
        |from (select * from dwd.wd_product_ms_all where paper_type='2') a left join ms_authors b on a.achievement_id  = b.achievement_id
      """.stripMargin).repartition(10).createOrReplaceTempView("product_ms")

     spark.sql("insert overwrite table dwd.wd_product_patent_ms  select * from product_ms")

    spark.stop()


  }
}
