package cn.sks.es

import org.apache.spark.sql.SparkSession

object AllProductUnion {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      //.master("local[40]")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("neo4jcsv")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val person_product = spark.sql("select person_id,achievement_id from dwb.wb_product_person")

    val person_1 = spark.sql("select person_id,zh_name as person_name from dwb.wb_person_nsfc_sts_academician_artificial")
    val person_2 = spark.sql("select person_id,zh_name as person_name from dwb.wb_person_nsfc_sts_academician_csai_ms")
    val person = person_1.union(person_2).dropDuplicates()

    person.join(person_product, Seq("person_id"))
      .createOrReplaceTempView("person")

    val authors_2 = spark.sql(
      """
        |select
        |achievement_id,
        |concat("[",concat_ws(",",collect_set(to_json(struct(person_id as id,person_name as name)))),"]")  as authors_2
        |from person group by achievement_id
      """.stripMargin).cache()


    val journal_2 = spark.sql(
      """
        |select
        |achievement_id,
        |journal_id,
        |journal_name as journal
        |from dwb.wb_product_journal_rel_journal
      """.stripMargin)


    val keyword_2 = spark.sql(
      """
        |select
        |achievement_id,
        |concat("[",concat_ws(",",collect_set(to_json(struct(keyword as name,keyword_id as id)))),"]")  as keyword_2
        |from dwb.wb_product_all_keyword group by achievement_id
      """.stripMargin)

    val include_2 = spark.sql(
      """
        |select
        |achievement_id,
        |concat("[",concat_ws(",",collect_set(to_json(struct(include_name as name)))),"]")  as  include_2
        |from ods.o_csai_product_journal_include group by achievement_id
      """.stripMargin)


    val paper_ref_2 = spark.sql(
      """
        |select
        |achivement_id as achievement_id,
        |concat("[",concat_ws(",",collect_set(to_json(struct(ref_name as name)))),"]")  as paper_ref_2
        |from ods.o_csai_product_journal_relationship group by achivement_id
      """.stripMargin)


    val product_journal = spark.sql(
      """
        |select
        |achievement_id
        |,paper_type
        |,chinese_title
        |,english_title
        |,doi
        |,handle
        |,null as first_author
        |,null as first_author_id
        |,correspondent_author
        |,null as fund_project
        |,publish_date
        |,article_no
        |,paper_rank
        |,language
        |,volume
        |,issue
        |,page_start
        |,page_end
        |,received_time
        |,revised_time
        |,accepted_time
        |,firstonline_time
        |,print_issn
        |,online_issn
        |,null as paper_award
        |,isfulltext
        |,fulltext_url
        |,fulltext_path
        |,abstract
        |,citation
        |,field_id
        |,field_name
        |,field_sub_id
        |,field_sub_name
        |,source
        | from dwb.wb_product_journal_ms_csai_nsfc_orcid
      """.stripMargin)


    product_journal.join(paper_ref_2, Seq("achievement_id"), "left")
      .join(include_2, Seq("achievement_id"), "left")
      .join(keyword_2, Seq("achievement_id"), "left")
      .join(journal_2, Seq("achievement_id"), "left")
      .join(authors_2, Seq("achievement_id"), "left")
      .createOrReplaceTempView("product_journal")


    spark.sql(
      """
        |select
        |achievement_id as id
        |,'1' as paper_type
        |,chinese_title
        |,english_title
        |,doi
        |,handle
        |,null as first_author
        |,null as first_author_id
        |,correspondent_author
        |,authors_2 as authors
        |,null as fund_project
        |,publish_date
        |,article_no
        |,keyword_2 as keywords
        |,include_2 as includes
        |,paper_ref_2 as references
        |,paper_rank
        |,language
        |,volume
        |,issue
        |,page_start
        |,page_end
        |,received_time
        |,revised_time
        |,accepted_time
        |,firstonline_time
        |,print_issn
        |,online_issn
        |,null as paper_award
        |,isfulltext
        |,fulltext_url
        |,fulltext_path
        |,abstract
        |,citation
        |,field_id
        |,field_name
        |,field_sub_id
        |,field_sub_name
        |,journal
        |,journal_id
        |,source
        | from product_journal
      """.stripMargin).repartition(200).createOrReplaceTempView("journal_person_re")

    //spark.sql("insert overwrite table dm.dm_es_product_journal select * from journal_person_re")

    //会议es实体组装

    spark.sql(
      """
        |select
        |explode(split("SCIE;SSCI;ISTP;EI",";")) as include
      """.stripMargin).createOrReplaceTempView("include")

    spark.sql("select achievement_id as id,include from dwb.wb_product_conference_ms_nsfc_orcid where include is not null ")
      .createOrReplaceTempView("conference_include")

    spark.sql(
      """
        |select
        |id,
        |a.include
        |from conference_include a join include b on a.include like concat("%",b.include,"%")
      """.stripMargin).createOrReplaceTempView("include_co_2")


    val include_coference = spark.sql(
      """
        |select
        |id as achievement_id,
        |concat("[",concat_ws(",",collect_set(to_json(struct(include as name)))),"]")  as  include_2
        |from include_co_2 group by id
      """.stripMargin)


    val product_conference = spark.sql(
      """
        |select
        |achievement_id
        |,'2' as product_type
        |,chinese_title
        |,english_title
        |,doi
        |,null as first_author
        |,null as first_author_id
        |,correspondent_author
        |,null as fund_project
        |,publish_date
        |,article_no
        |,article_type
        |,null as reference
        |,null as paper_rank
        |,language
        |,page_start
        |,page_end
        |,received_time
        |,revised_time
        |,accepted_time
        |,firstonline_time
        |,print_issn
        |,online_issn
        |,null as paper_award
        |,isfulltext
        |,fulltext_url
        |,fulltext_path
        |,abstract
        |,citation
        |,conference
        |,conference_type
        |,conference_address
        |,organization
        |,start_date
        |,end_date
        |,country
        |,city
        |,source
        | from dwb.wb_product_conference_ms_nsfc_orcid
      """.stripMargin)


    product_conference
      .join(include_coference, Seq("achievement_id"), "left")
      .join(keyword_2, Seq("achievement_id"), "left")
      .join(authors_2, Seq("achievement_id"), "left")
      .createOrReplaceTempView("product_conference")

    spark.sql(
      """
        |select
        |achievement_id  as id
        |,'2' as product_type
        |,chinese_title
        |,english_title
        |,doi
        |,null as first_author
        |,null as first_author_id
        |,correspondent_author
        |,authors_2 as authors
        |,null as fund_project
        |,publish_date
        |,article_no
        |,article_type
        |,keyword_2 as keywords
        |,include_2 as includes
        |,null as references
        |,null as paper_rank
        |,language
        |,page_start
        |,page_end
        |,received_time
        |,revised_time
        |,accepted_time
        |,firstonline_time
        |,print_issn
        |,online_issn
        |,null as paper_award
        |,isfulltext
        |,fulltext_url
        |,fulltext_path
        |,abstract
        |,citation
        |,conference
        |,conference_type
        |,conference_address
        |,organization
        |,start_date
        |,end_date
        |,country
        |,city
        |,source
        | from product_conference
      """.stripMargin).repartition(30).createOrReplaceTempView("conference_person_re")

    //spark.sql("insert overwrite table dm.dm_es_product_conference select * from conference_person_re")





    //专利数据整合

    val patent_subject = spark.sql("select *  from  dm.dm_neo4j_product_subject_patent").withColumnRenamed("id", "achievement_id").dropDuplicates("achievement_id")

    val product_patent = spark.sql(
      """
        |select
        |achievement_id
        |,chinese_title
        |,english_title
        |,abstract
        |,requirement
        |,doi
        |,language
        |,apply_date
        |,award_date
        |,patent_no
        |,patent_type
        |,patent_type_original
        |,country
        |,applicant
        |,application_address
        |,category_no
        |,secondary_category_no
        |,cpic_no
        |,part_no
        |,publish_no
        |,agent
        |,agency
        |,agent_address
        |,patentee
        |,ipc
        |,cpc
        |,issue_unit
        |,current_status
        |,publish_agency
        |,hasfullext
        |,fulltext_url
        |,province
        |,source
        | from dwb.wb_product_patent_ms_csai_nsfc
      """.stripMargin)


    product_patent
      .join(authors_2, Seq("achievement_id"), "left")
      .join(patent_subject, Seq("achievement_id"), "left")
      .createOrReplaceTempView("product_patent")

    spark.sql(
      """
        |select
        |achievement_id as id
        |,chinese_title
        |,english_title
        |,abstract
        |,requirement
        |,doi
        |,authors_2 as authors
        |,language
        |,apply_date
        |,award_date
        |,patent_no
        |,patent_type
        |,patent_type_original
        |,country
        |,applicant
        |,application_address
        |,category_no
        |,regexp_replace(secondary_category_no,"rank","name") as secondary_category_no
        |,cpic_no
        |,part_no
        |,publish_no
        |,agent
        |,agency
        |,agent_address
        |,patentee
        |,ipc
        |,cpc
        |,issue_unit
        |,current_status
        |,publish_agency
        |,hasfullext
        |,fulltext_url
        |,province
        |,one_rank_id
        |,one_rank_no
        |,one_rank_name
        |,two_rank_id
        |,two_rank_no
        |,two_rank_name
        |,source
        | from product_patent
      """.stripMargin).repartition(150).createOrReplaceTempView("patent_person_re")

    spark.sql("insert overwrite table dm.dm_es_product_patent select * from patent_person_re")

    //标准数据

    val criterion_org = spark.sql(
      """
        |select
        |achievement_id,
        |concat("[",concat_ws(",",collect_set(to_json(struct(org_name as name)))),"]")  as  org
        |from ods.o_csai_criterion_org group by achievement_id
      """.stripMargin)

    val product_criterion = spark.sql(
      """
        |select
        |achievement_id
        |,chinese_title
        |,english_title
        |,status
        |,publish_date
        |,implement_date
        |,abolish_date
        |,criterion_no
        |,china_citerion_classification_no
        |,in_criterion_classification_no
        |,language
        |,charge_department
        |,responsibility_department
        |,publish_agency
        |,hasfulltext
        |,fulltext_url
        |,source
        | from dwb.wb_product_criterion_csai_nsfc
      """.stripMargin)


    product_criterion
      .join(authors_2, Seq("achievement_id"), "left")
      .join(criterion_org, Seq("achievement_id"), "left")
      .createOrReplaceTempView("product_criterion")

    spark.sql(
      """
        |select
        |achievement_id as id
        |,chinese_title
        |,english_title
        |,status
        |,publish_date
        |,implement_date
        |,abolish_date
        |,criterion_no
        |,china_citerion_classification_no
        |,in_criterion_classification_no
        |,language
        |,org as applicant
        |,authors_2 as authors
        |,charge_department
        |,responsibility_department
        |,publish_agency
        |,hasfulltext
        |,fulltext_url
        |,source
        | from product_criterion
      """.stripMargin).repartition(10).createOrReplaceTempView("criterion_person_re")

    //spark.sql("insert overwrite table dm.dm_es_product_criterion select * from criterion_person_re")

    //专著数据

    val product_monograph = spark.sql(
      """
        |select
        |achievement_id
        |,chinese_title
        |,english_title
        |,publish_date
        |,hasfulltext
        |,book_name
        |,bookseriesname
        |,language
        |,status
        |,isbn
        |,editor
        |,page_range
        |,word_count
        |,publisher
        |,source
        | from dwb.wb_product_monograph_ms_csai_nsfc
      """.stripMargin)


    product_monograph
      .join(authors_2, Seq("achievement_id"), "left")
      .createOrReplaceTempView("product_monograph")

    spark.sql(
      """
        |select
        |achievement_id
        |,chinese_title
        |,english_title
        |,authors_2 as authors
        |,publish_date
        |,hasfulltext
        |,book_name
        |,bookseriesname
        |,language
        |,status
        |,isbn
        |,editor
        |,page_range
        |,word_count
        |,publisher
        |,source
        | from product_monograph
      """.stripMargin)
      .repartition(10).createOrReplaceTempView("monograph_person_re")

   // spark.sql("insert overwrite table dm.dm_es_product_monograph select * from monograph_person_re")


  }
}
