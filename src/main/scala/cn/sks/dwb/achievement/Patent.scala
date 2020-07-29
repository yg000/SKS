package cn.sks.dwb.achievement

import cn.sks.util.{DefineUDF, NameToPinyinUtil}
import org.apache.spark.sql.{Column, SparkSession}

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
    //融合的函数
    spark.udf.register("clean_fusion", DefineUDF.clean_fusion _)

    //项目产出成果===================
    val product_csai = spark.read.table("dwd.wd_product_patent_csai")
    //人产出成果
    val product_nsfc_person = spark.read.table("dwd.wd_product_patent_nsfc")
    //项目产出成果
    val product_nsfc_project = spark.read.table("dwd.wd_product_patent_project_nsfc")
    val product_nsfc_npd = spark.read.table("dwd.wd_product_patent_npd_nsfc")

    val product_ms =  spark.read.table("dwd.wd_product_patent_ms")

    //将基金委对应的论文成果对应的作者和论文的字段合并到一块儿
    product_nsfc_person.union(product_nsfc_project).union(product_nsfc_npd).select("achievement_id", "chinese_title", "inventor", "flow_source").dropDuplicates("achievement_id")
      .createOrReplaceTempView("product_nsfc_patent")


    val fushion_data_nsfc = spark.sql(
      """
        |select
        |achievement_id as achievement_id_nsfc
        |,chinese_title as title
        |,explode(split(inventor,";")) as person_name
        |,flow_source as source_nsfc
        |from product_nsfc_patent where inventor is not null and inventor!=''
      """.stripMargin)
    NameToPinyinUtil.nameToPinyin(spark, fushion_data_nsfc, "person_name")
      .repartition(10).createOrReplaceTempView("fushion_data_nsfc_pinyin")

    //spark.sql("insert overwrite table dwd.wd_product_fusion_data_patent_nsfc  select * from person_product_co")


    //科协的专利数据

    val fushion_data_csai = spark.sql(
      """
        |select
        |achievement_id as achievement_id_csai
        |,zh_name as person_name
        |,zh_title as title
        |,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_product_patent_csai\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as  source_csai
        |from dwd.wd_product_person_ext_csai where product_type='5' and source='csai'
      """.stripMargin)

    NameToPinyinUtil.nameToPinyin(spark, fushion_data_csai, "person_name")
      .createOrReplaceTempView("fushion_data_csai_pinyin")

    //基金委专利需要融合的数据
    val nsfc_en_name_1 = spark.sql(
      """
        |select
        |b.achievement_id_csai
        |,a.achievement_id_nsfc
        |,"5" as product_type
        |,source_csai
        |,source_nsfc
        |from fushion_data_nsfc_pinyin a join fushion_data_csai_pinyin b on clean_fusion(a.title)=clean_fusion(b.title) and clean_fusion(a.en_name_normal)=clean_fusion(b.en_name_normal)
      """.stripMargin)

    val nsfc_en_name_2 = spark.sql(
      """
        |select
        |b.achievement_id_csai
        |,a.achievement_id_nsfc
        |,"5" as product_type
        |,source_csai
        |,source_nsfc
        |from fushion_data_nsfc_pinyin a join fushion_data_csai_pinyin b on clean_fusion(a.title)=clean_fusion(b.title) and clean_fusion(a.en_name_inverted)=clean_fusion(b.en_name_inverted)
      """.stripMargin)

    nsfc_en_name_1.union(nsfc_en_name_2).dropDuplicates("achievement_id_nsfc")//.dropDuplicates("achievement_id_csai")
      .createOrReplaceTempView("comparison_table")

    spark.sql(
      """
        |select
        |achievement_id_csai
        |,achievement_id_nsfc
        |,product_type
        |,concat("{","\"from\"",":",source_nsfc,",","\"to\"",":",source_csai,",","\"rule\"",":","\"name+title\"","}") as source
        |from comparison_table
      """.stripMargin)
      .repartition(1)
      .createOrReplaceTempView("wb_product_patent_csai_nsfc_rel")

    spark.sql("insert overwrite table dwb.wb_product_patent_csai_nsfc_rel  select * from wb_product_patent_csai_nsfc_rel")


    //合并基金委和科协的专利的成果数据
    spark.sql(
      """
        |select achievement_id_csai as achievement_id, concat_ws(',',collect_set(achievement_id_nsfc)) as  source  from dwb.wb_product_patent_csai_nsfc_rel group by achievement_id_csai
        |""".stripMargin).createOrReplaceTempView("get_source")

    product_nsfc_npd.union(product_nsfc_person).union(product_nsfc_project).union(product_csai).createOrReplaceTempView("o_product_patent_csai_nsfc")

    spark.sql(
      """
        |select
        |a.achievement_id
        |,chinese_title
        |,englisth_title
        |,abstract
        |,requirement
        |,doi
        |,inventor
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
        |,one_rank_id
        |,one_rank_no
        |,one_rank_name
        |,two_rank_id
        |,two_rank_no
        |,two_rank_name
        |,ifnull(b.source,flow_source) as flow_source
        |,a.source
        |from o_product_patent_csai_nsfc a left join get_source b on a.achievement_id = b.achievement_id
      """.stripMargin).dropDuplicates("achievement_id").createOrReplaceTempView("product_patent_csai_nsfc_get_source")


    spark.sql(
      """
        |insert overwrite table dwb.wb_product_patent_csai_nsfc
        |select a.*
        |from product_patent_csai_nsfc_get_source a left join  dwb.wb_product_patent_csai_nsfc_rel b on a.achievement_id = b.achievement_id_nsfc where b.achievement_id_nsfc is null
        |""".stripMargin)

// csai_nsfc_ms
    val fushion_data_csai_nsfc = spark.sql(
      """
        |select
        |achievement_id as achievement_id_csai_nsfc
        |,chinese_title as title
        |,explode(split(inventor,";")) as person_name
        |,flow_source as source_csai_nsfc
        |from dwb.wb_product_patent_csai_nsfc where inventor!="" and inventor is not null
      """.stripMargin)


    NameToPinyinUtil.nameToPinyin(spark, fushion_data_csai_nsfc, "person_name")
      .repartition(10).createOrReplaceTempView("fushion_data_csai_nsfc_pinyin")

    // spark.sql("insert overwrite table dwd.wd_product_fusion_data_patent_ms_csai_nsfc  select * from fushion_data_csai_nsfc_pinyin")

    val fushion_data_ms = spark.sql(
      """
        |select
        |achievement_id as achievement_id_ms
        |,zh_name as person_name
        |,zh_title as title
        |,concat("{","\"source\"",":","\"ms\"",",""\"table\"",":","\"wd_product_monograph_ms\"","," ,"\"id\"",":","\"",achievement_id,"\"","}") as source_ms
        |from dwd.wd_product_person_ext_csai where product_type='5' and source='ms'
      """.stripMargin)

    NameToPinyinUtil.nameToPinyin(spark, fushion_data_ms, "person_name")
      .createOrReplaceTempView("fushion_data_ms_pinyin")


    val nsfc_en_name_3 = spark.sql(
      """
        |select
        |a.achievement_id_csai_nsfc
        |,b.achievement_id_ms
        |,"4" as product_type
        |,source_csai_nsfc
        |,source_ms
        |from fushion_data_csai_nsfc_pinyin a join fushion_data_ms_pinyin b on clean_fusion(a.title) = clean_fusion(b.title) and clean_fusion(a.en_name_normal) = clean_fusion(b.en_name_normal)
      """.stripMargin)

    val nsfc_en_name_4 = spark.sql(
      """
        |select
        |a.achievement_id_csai_nsfc
        |,b.achievement_id_ms
        |,"4" as product_type
        |,source_csai_nsfc
        |,source_ms
        |from fushion_data_csai_nsfc_pinyin a join fushion_data_ms_pinyin b on clean_fusion(a.title) = clean_fusion(b.title) and clean_fusion(a.en_name_inverted) = clean_fusion(b.en_name_inverted)
      """.stripMargin)

    nsfc_en_name_3.union(nsfc_en_name_4).dropDuplicates("achievement_id_ms")//.dropDuplicates("achievement_id_csai_nsfc")
      .createOrReplaceTempView("comparison_table")


    spark.sql(
      """
        |select
        |achievement_id_csai_nsfc
        |,achievement_id_ms
        |,product_type
        |,concat("{","\"from\"",":",source_csai_nsfc,",","\"to\"",":",source_ms,",","\"rule\"",":","\"name+title\"","}") as source
        |from comparison_table
      """.stripMargin)
      .repartition(1)
      .createOrReplaceTempView("wb_product_patent_csai_nsfc_ms_rel")

    spark.sql("insert overwrite table dwb.wb_product_patent_csai_nsfc_ms_rel  select * from wb_product_patent_csai_nsfc_ms_rel")

    spark.sql(
      """
        |select achievement_id_csai_nsfc as achievement_id, concat_ws(',',collect_set(achievement_id_ms)) as  source  from dwb.wb_product_patent_csai_nsfc_ms_rel group by achievement_id_csai_nsfc
        |""".stripMargin).createOrReplaceTempView("get_source")
    val product_patent_csai_nsfc = spark.sql(
      """
        |select * from dwb.wb_product_patent_csai_nsfc
        |""".stripMargin)
    product_patent_csai_nsfc.union(product_ms).createOrReplaceTempView("o_product_patent")


    spark.sql(
      """
        |select
        |a.achievement_id
        |,chinese_title
        |,englisth_title
        |,abstract
        |,requirement
        |,doi
        |,inventor
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
        |,one_rank_id
        |,one_rank_no
        |,one_rank_name
        |,two_rank_id
        |,two_rank_no
        |,two_rank_name
        |,ifnull(b.source,flow_source) as flow_source
        |,a.source
        |from o_product_patent a left join get_source b on a.achievement_id = b.achievement_id
      """.stripMargin).dropDuplicates("achievement_id").createOrReplaceTempView("product_patent_csai_nsfc_ms_get_source")


    spark.sql(
      """
        |insert overwrite table dwb.wb_product_patent_csai_nsfc_ms
        |select a.*
        |from product_patent_csai_nsfc_ms_get_source a left join  dwb.wb_product_patent_csai_nsfc_ms_rel b on a.achievement_id = b.achievement_id_ms where b.achievement_id_ms is null
        |""".stripMargin)

  }
}
