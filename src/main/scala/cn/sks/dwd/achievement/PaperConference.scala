package cn.sks.dwd.achievement

import cn.sks.jutil.H2dbUtil
import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.{DefineUDF, NameToPinyinUtil}
/*

论文数据的整合的整体的代码

 */
object PaperConference {
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
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()


    spark.udf.register("clean_div", DefineUDF.clean_div _)
    spark.udf.register("clean_separator", DefineUDF.clean_separator _)

    //微软的学术论文
    spark.sql(
      """
        |select achivement_id as achievement_id,concat_ws(';',collect_set(person_name)) as authors from ods.o_ms_product_author group by achievement_id
        |""".stripMargin).createOrReplaceTempView("ms_authors")
    spark.sql(
      """
        |select
        | a.achievement_id
        |,"2" as product_type
        |,clean_div(english_name) as chinese_title
        |,clean_div(english_name) as  english_title
        |,doi
        |,first_author
        |,first_author_id
        |,correspondent_author
        |,b.authors as authors
        |,fund_project
        |,publish_date
        |,null as article_no
        |,null as article_type
        |,keywords as keyword
        |,includes as include
        |,references as reference
        |,null as paper_rank
        |,if(regexp_replace(english_name,'([^\\u4E00-\\u9FA5]+)','')!='','cn','en') language
        |,null as page_start
        |,null as page_end
        |,null as received_time
        |,null as revised_time
        |,null as accepted_time
        |,null as firstonline_time
        |,null as print_issn
        |,null as online_issn
        |,null as paper_award
        |,"0" as isFullText
        |,null as fulltext_url
        |,null as fulltext_path
        |,abstract
        |,citation
        |,null as conference
        |,null as conference_type
        |,null as conference_address
        |,null organization
        |,null start_date
        |,null end_date
        |,null as country
        |,null as city
        |,concat("{","\"source\"",":","\"ms\"",",""\"table\"",":","\"wd_product_conference_ms\"","," ,"\"id\"",":","\"",a.achievement_id,"\"","}") as flow_source
        |,'ms' as source
        |from (select * from dwd.wd_product_ms_all where paper_type='3') a left join ms_authors b on a.achievement_id  = b.achievement_id
          """.stripMargin).repartition(10).createOrReplaceTempView("ms_product")
    //spark.sql("insert overwrite table dwd.wd_product_conference_ms  select * from ms_product")
    H2dbUtil.useH2("dwd.wd_product_ms_all","dwd.wd_product_conference_ms")
    H2dbUtil.useH2("ods.o_ms_product_author","dwd.wd_product_conference_ms")
    //orcid会议论文数据


    spark.sql(
      """
        |select works_uuid as achievement_id,concat_ws(';',collect_set(credit_name)) as authors from ods.o_orcid_product_journal_conference_author group by works_uuid
        |""".stripMargin).createOrReplaceTempView("orcid_authors")
    spark.sql(
      s"""
         |select
         | works_uuid achievement_id
         |,"2" as product_type
         |,clean_div(title) as chinese_title
         |,clean_div(title) as english_title
         |,null as doi
         |,null as first_author
         |,null as first_author_id
         |,null as correspondent_author
         |,b.authors as authors
         |,null as fund_project
         |,if(pub_year is null and pub_year!="","00000000",concat(pub_year,if(length(pub_month)=2,pub_month,concat("0",if(pub_month is null,"0",pub_month))),if(length(pub_day)=2,pub_day,concat("0",if(pub_day is null,"0",pub_day))))) publish_date
         |,null article_no
         |,null article_type
         |,null as keyword
         |,null as include
         |,null as reference
         |,null as paper_rank
         |,if(regexp_replace(title,'([^\\u4E00-\\u9FA5]+)','')!='','cn','en') language
         |,null as page_start
         |,null as page_end
         |,null as received_time
         |,null as revised_time
         |,null as accepted_time
         |,null as firstonline_time
         |,null as print_issn
         |,null as online_issn
         |,null as paper_award
         |,"0" as isFullText
         |,null as fulltext_url
         |,null as fulltext_path
         |,if(short_description=="",null,short_description) abstract
         |,"0" as citation
         |,null as conference
         |,null as conference_type
         |,null as conference_address
         |,null organization
         |,null start_date
         |,null end_date
         |,null as country
         |,null as city
         |,concat("{","\\"source\\"",":","\\"orcid\\"",",""\\"table\\"",":","\\"wd_product_conference_orcid\\"","," ,"\\"id\\"",":","\\"",works_uuid,"\\"","}") as flow_source
         |,'orcid' as source
         |from ods.o_orcid_product_conference a left join orcid_authors b on a.works_uuid  = b.achievement_id
      """.stripMargin).repartition(10).createOrReplaceTempView("orcid_conference")

    //spark.sql("insert overwrite table dwd.wd_product_conference_orcid   select * from orcid_conference")
    H2dbUtil.useH2("ods.o_orcid_product_conference","dwd.wd_product_conference_orcid")
    H2dbUtil.useH2("ods.o_orcid_product_journal_conference_author","dwd.wd_product_conference_orcid")

    //ren产出成果===================
    val nsfc_product_person = spark.sql(
      s"""
         |select
         | achievement_id
         |,"2" as product_type
         |,clean_div(zh_title) as chinese_title
         |,clean_div(en_title) as english_title
         |,if(doi=="",null,doi) as doi
         |,null as first_author
         |,psn_code as first_author_id
         |,null as correspondent_author
         |,clean_separator(clean_div(authors)) authors
         |,null as fund_project
         |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
         |,article_no
         |,article_type
         |,zh_keyword as keyword
         |,include_other as include
         |,null as reference
         |,null as paper_rank
         |,if(regexp_replace(zh_title,'([^\\u4E00-\\u9FA5]+)','')!='','cn','en') language
         |,page_range as page_start
         |,null as page_end
         |,null as received_time
         |,null as revised_time
         |,null as accepted_time
         |,null as firstonline_time
         |,null as print_issn
         |,null as online_issn
         |,null as paper_award
         |,"0" as isFullText
         |,null as fulltext_url
         |,null as fulltext_path
         |,if(zh_abstract=="",null,zh_abstract) abstract
         |,"0" as citation
         |,proceeding_name as conference
         |,proceeding_type as conference_type
         |,proceeding_address as conference_address
         |,proceeding_organizer organization
         |,start_time start_date
         |,end_time end_date
         |,null as country
         |,null as city
         |,concat("{","\\"source\\"",":","\\"nsfc\\"",",""\\"table\\"",":","\\"wd_product_conference_nsfc\\"","," ,"\\"id\\"",":","\\"",achievement_id,"\\"","}") as flow_source
         |,'ms' as source
         |from ods.o_nsfc_product_conference
      """.stripMargin)

    nsfc_product_person.repartition(10).createOrReplaceTempView("nsfc_product_person")

    //spark.sql("insert overwrite table dwd.wd_product_conference_nsfc   select * from nsfc_product_person")
    H2dbUtil.useH2("ods.o_nsfc_product_conference","dwd.wd_product_conference_nsfc")


    val nsfc_product_business = spark.sql(
      """
        |select
        | achievement_id
        |,"2" as product_type
        |,clean_div(zh_title) as chinese_title
        |,clean_div(en_title) as english_title
        |,if(doi=="",null,doi) as doi
        |,null as first_author
        |,psn_code as first_author_id
        |,null as correspondent_author
        |,clean_separator(clean_div(authors)) authors
        |,null as fund_project
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,article_no
        |,article_type
        |,zh_keyword as keyword
        |,list_info as include
        |,null as reference
        |,null as paper_rank
        |,if(regexp_replace(zh_title,'([^\\u4E00-\\u9FA5]+)','')!='','cn','en') language
        |,page_range as page_start
        |,null as page_end
        |,null as received_time
        |,null as revised_time
        |,null as accepted_time
        |,null as firstonline_time
        |,null as print_issn
        |,null as online_issn
        |,null as paper_award
        |,"0" as isFullText
        |,null as fulltext_url
        |,null as fulltext_path
        |,if(zh_abstract=="",null,zh_abstract) abstract
        |,"0" as citation
        |,proceeding_name as conference
        |,proceeding_type as conference_type
        |,proceeding_address as conference_address
        |,proceeding_organizer organization
        |,start_time start_date
        |,end_time end_date
        |,null as country
        |,null as city
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_conference_project_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_project_conference
      """.stripMargin).cache()

    nsfc_product_business.repartition(5).createOrReplaceTempView("nsfc_product_business")

    //spark.sql("insert overwrite table dwd.wd_product_conference_project_nsfc  select * from nsfc_product_business")
    H2dbUtil.useH2("ods.o_nsfc_project_conference","dwd.wd_product_conference_project_nsfc")

    spark.sql(
      """
        |select
        | achievement_id
        |,"2" as product_type
        |,clean_div(zh_title) as chinese_title
        |,clean_div(en_title) as english_title
        |,if(doi=="",null,doi) as doi
        |,null as first_author
        |,psn_code as first_author_id
        |,null as correspondent_author
        |,clean_separator(clean_div(authors)) authors
        |,null as fund_project
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,article_no
        |,article_type
        |,zh_keyword as keyword
        |,list_info as include
        |,null as reference
        |,null as paper_rank
        |,if(regexp_replace(zh_title,'([^\\u4E00-\\u9FA5]+)','')!='','cn','en') language
        |,page_range as page_start
        |,null as page_end
        |,null as received_time
        |,null as revised_time
        |,null as accepted_time
        |,null as firstonline_time
        |,null as print_issn
        |,null as online_issn
        |,null as paper_award
        |,"0" as isFullText
        |,null as fulltext_url
        |,null as fulltext_path
        |,if(zh_abstract=="",null,zh_abstract) abstract
        |,"0" as citation
        |,proceeding_name as conference
        |,proceeding_type as conference_type
        |,proceeding_address as conference_address
        |,proceeding_organizer organization
        |,start_time start_date
        |,end_time end_date
        |,null as country
        |,null as city
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_conference_npd_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_npd_conference
      """.stripMargin).repartition(5).createOrReplaceTempView("nsfc_product_npd")

    //spark.sql("insert overwrite table dwd.wd_product_conference_npd_nsfc  select * from nsfc_product_npd")
    H2dbUtil.useH2("ods.o_nsfc_npd_conference","dwd.wd_product_conference_npd_nsfc")


    spark.stop()


  }
}
