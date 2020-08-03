package cn.sks.dwd.achievement

import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.{DefineUDF,NameToPinyinUtil}
/*

论文数据的整合的整体的代码

 */
object PaperJournal {
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
      .appName("journal")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)
    spark.udf.register("clean_separator", DefineUDF.clean_separator _)

    //orcid


    spark.sql(
      """
        |select works_uuid as achievement_id,concat_ws(';',collect_set(credit_name)) as authors from ods.o_orcid_product_journal_conference_author group by works_uuid
        |""".stripMargin).createOrReplaceTempView("orcid_authors")

    spark.sql(
      """
        |select
        |works_uuid as achievement_id
        |,"1" as paper_type
        |,clean_div(title) as chinese_title
        |,clean_div(title) as english_title
        |,null as doi
        |,null as handle
        |,null as first_author
        |,null as first_author_id
        |,null as correspondent_author
        |,b.authors as authors
        |,null as fund_project
        |,if(pub_year is null and pub_year!="","00000000",concat(pub_year,if(length(pub_month)=2,pub_month,concat("0",if(pub_month is null,"0",pub_month))),if(length(pub_day)=2,pub_day,concat("0",if(pub_day is null,"0",pub_day))))) publish_date
        |,null as article_no
        |,null as keywords
        |,null as includes
        |,null as references
        |,null as paper_rank
        |,if(regexp_replace(title,'([^\u4E00-\u9FA5]+)','')!='','cn','en') language
        |,null volume
        |,null issue
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
        |,null as citation
        |,null as field_id
        |,null as field_name
        |,null as field_sub_id
        |,null as field_sub_name
        |,journal_title as journal
        |,null as journal_id
        |,concat("{","\"source\"",":","\"orcid\"",",""\"table\"",":","\"wd_product_journal_orcid\"","," ,"\"id\"",":","\"",works_uuid,"\"","}") as flow_source
        |,'orcid' as source
        |from ods.o_orcid_product_journal a left join orcid_authors b on a.works_uuid  = b.achievement_id
      """.stripMargin).repartition(10).createOrReplaceTempView("product_orcid")
     spark.sql("insert overwrite table dwd.wd_product_journal_orcid  select * from product_orcid")
    //微软

    spark.sql(
      """
        |select achivement_id as achievement_id,concat_ws(';',collect_set(person_name)) as authors from ods.o_ms_product_author group by achievement_id
        |""".stripMargin).createOrReplaceTempView("ms_authors")
    spark.sql(
      """
        |select
        | a.achievement_id
        |,"1" as paper_type
        |,clean_div(english_name) as chinese_title
        |,clean_div(english_name)  as english_title
        |,null as doi
        |,null as handle
        |,first_author
        |,first_author_id
        |,null as correspondent_author
        |,b.authors as authors
        |,fund_project
        |,publish_date
        |,null as article_no
        |,keywords
        |,includes
        |,references
        |,paper_rank
        |,if(regexp_replace(english_name,'([^\u4E00-\u9FA5]+)','')!='','cn','en') language
        |,volume
        |,issue
        |,null as page_start
        |,null as page_end
        |,null as received_time
        |,null as revised_time
        |,null as accepted_time
        |,null as firstonline_time
        |,null as print_issn
        |,null as online_issn
        |,null as paper_award
        |,null as isFullText
        |,fulltext_url
        |,null as fulltext_path
        |,if(abstract=="",null,abstract) abstract
        |,citation
        |,field_id
        |,field_name
        |,field_sub_id
        |,field_sub_name
        |,journal
        |,journal_id
        |,concat("{","\"source\"",":","\"ms\"",",""\"table\"",":","\"wd_product_journal_ms\"","," ,"\"id\"",":","\"",a.achievement_id,"\"","}") as flow_source
        |,'ms' as source
        |from (select * from ods.o_ms_product_all where paper_type='1') a left join ms_authors b on a.achievement_id  = b.achievement_id
      """.stripMargin).repartition(30).createOrReplaceTempView("product_ms")
      spark.sql("insert overwrite table dwd.wd_product_journal_ms  select * from product_ms")


    //csai
    spark.sql(
      """
        |select achivement_id as achievement_id,concat_ws(';',collect_set(person_name)) as authors from ods.o_csai_product_journal_author group by achivement_id
        |""".stripMargin).createOrReplaceTempView("csai_authors")
    spark.sql(
      """
        |select
        | a.achievement_id
        |,"1" as paper_type
        |,chinese_name as chinese_title
        |,null as english_title
        |,null as doi
        |,null as handle
        |,first_author
        |,first_author_id
        |,null as correspondent_author
        |,b.authors as authors
        |,null fund_project
        |,publish_date
        |,null as article_no
        |,null keywords
        |,null includes
        |,null references
        |,paper_rank
        |,if(regexp_replace(chinese_name,'([^\u4E00-\u9FA5]+)','')!='','cn','en') language
        |,volume
        |,issue
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
        |,fulltext_url
        |,null as fulltext_path
        |,if(abstract=="",null,abstract) abstract
        |,citation
        |,field_id
        |,field_name
        |,field_sub_id
        |,field_sub_name
        |,journal
        |,journal_id
        |,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_product_journal_csai\"","," ,"\"id\"",":","\"",a.achievement_id,"\"","}") as flow_source
        |,'csai' as source
        |from ods.o_csai_product_journal a left join csai_authors b on a.achievement_id  = b.achievement_id
      """.stripMargin).createOrReplaceTempView("product_csai")
     spark.sql("insert overwrite table dwd.wd_product_journal_csai  select * from product_csai")


    //o_product_business_journal_nsfc
    val product_nsfc_project = spark.sql(
      """
        |select
        | achievement_id
        |,"1" as paper_type
        |,clean_div(zh_title) as chinese_title
        |,clean_div(en_title) as english_title
        |,doi
        |,null as handle
        |,null as first_author
        |,psn_code as first_author_id
        |,null as correspondent_author
        |,clean_separator(clean_div(authors)) authors
        |,null as fund_project
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,article_no
        |,zh_keyword as keywords
        |,list_info as includes
        |,null as references
        |,null as paper_rank
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en') language
        |,volume
        |,null as issue
        |,page_range as page_start
        |,null as page_end
        |,null as received_time
        |,null as revised_time
        |,null as accepted_time
        |,null as firstonline_time
        |,null as print_issn
        |,null as online_issn
        |,null as paper_award
        |,has_full_text as isFullText
        |,null as fulltext_url
        |,null as fulltext_path
        |,if(zh_abstract=="",null,zh_abstract) as abstract
        |,"0" as citation
        |,null as field_id
        |,null as field_name
        |,null as field_sub_id
        |,null as field_sub_name
        |,journal_name as journal
        |,null as journal_id
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_journal_project_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_project_journal
      """.stripMargin)


    product_nsfc_project.repartition(10).createOrReplaceTempView("o_product_business_journal_nsfc")
    spark.sql("insert overwrite table dwd.wd_product_journal_project_nsfc  select * from o_product_business_journal_nsfc")


    //npd_paper
    val product_nsfc_npd = spark.sql(
      """
        |select
        | achievement_id
        |,"1" as paper_type
        |,clean_div(zh_title) as chinese_title
        |,clean_div(en_title) as english_title
        |,null as doi
        |,null as handle
        |,null as first_author
        |,null as first_author_id
        |,null as correspondent_author
        |,clean_separator(clean_div(authors)) authors
        |,null as fund_project
        |,if(publish_year is null,"00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,article_no
        |,null as keywords
        |,null as includes
        |,null as references
        |,null as paper_rank
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,volume
        |,series as issue
        |,if(page_range=="",null,translate(page_range,"-","#")) as page_start
        |,null as page_end
        |,null as received_time
        |,null as revised_time
        |,null as accepted_time
        |,null as firstonline_time
        |,issn as print_issn
        |,null as online_issn
        |,null as paper_award
        |,"0" as isFullText
        |,null as fulltext_url
        |,null as fulltext_path
        |,zh_abstract as abstract
        |,"0" as citation
        |,null as field_id
        |,null as field_name
        |,null as field_sub_id
        |,null as field_sub_name
        |,journal_name as journal
        |,null as journal_id
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_journal_npd_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_npd_journal
      """.stripMargin)

    product_nsfc_npd.repartition(10).createOrReplaceTempView("o_product_business_journal_npd_nsfc")
    spark.sql("insert overwrite table dwd.wd_product_journal_npd_nsfc  select * from o_product_business_journal_npd_nsfc")

    val product_nsfc_person = spark.sql(
      """
        |select
        | achievement_id
        |,product_type as paper_type
        |,clean_div(zh_title) as chinese_title
        |,clean_div(en_title) as english_title
        |,doi
        |,null as handle
        |,null as first_author
        |,psn_code as first_author_id
        |,null as correspondent_author
        |,clean_separator(clean_div(authors)) authors
        |,null as fund_project
        |,if(publish_year is null and publish_year!="","00000000",concat(publish_year,if(length(publish_month)=2,publish_month,concat("0",if(publish_month is null,"0",publish_month))),if(length(publish_day)=2,publish_day,concat("0",if(publish_day is null,"0",publish_day))))) publish_date
        |,article_no
        |,zh_keyword as keywords
        |,list_info as includes
        |,null as references
        |,null as paper_rank
        |,if(regexp_replace(zh_title,'([^\u4E00-\u9FA5]+)','')!='','cn','en')  as language
        |,volume
        |,series as issue
        |,page_range as page_start
        |,null as page_end
        |,null as received_time
        |,null as revised_time
        |,null as accepted_time
        |,null as firstonline_time
        |,null as print_issn
        |,null as online_issn
        |,null as paper_award
        |,has_full_text as isFullText
        |,null as fulltext_url
        |,full_text_file_code as fulltext_path
        |,if(zh_abstract=="",null,zh_abstract) as abstract
        |,"0" as citation
        |,null as field_id
        |,null as field_name
        |,null as field_sub_id
        |,null as field_sub_name
        |,journal_name as journal
        |,null as journal_id
        |,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_product_journal_nsfc\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as flow_source
        |,'nsfc' as source
        |from ods.o_nsfc_product_journal
      """.stripMargin)

    product_nsfc_person.repartition(10).createOrReplaceTempView("o_product_journal_nsfc")
    spark.sql("insert overwrite table dwd.wd_product_journal_nsfc  select * from o_product_journal_nsfc")



    spark.stop()


  }
}
