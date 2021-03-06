package cn.sks.dwb.person_partition

import cn.sks.util.{AchievementUtil, DefineUDF, NameToPinyinUtil, PersonUtil}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object PersonFusionMs {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      //.master("local[30]")
      .appName("PersonFusionMs")
      //.config("spark.local.dir", "/data/tmp")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val flag = "person_nsfc_sts_academician_csai_ms_rel"

    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    spark.udf.register("union_flow_source", DefineUDF.unionFlowSource _)

    ////step 0. 准备数据

    spark.sql(
      s"""
         |ALTER TABLE dwb.wb_person_rel_partition DROP IF EXISTS PARTITION (flag='$flag')
         |""".stripMargin)
    spark.sql(
      s"""
         |ALTER TABLE dwb.wb_person_rel_partition DROP IF EXISTS PARTITION (flag='${flag}_tst')
         |""".stripMargin)


    spark.read.table("dwd.wd_product_person_ext_csai").select("person_id","zh_name","zh_title").dropDuplicates().cache().createOrReplaceTempView("wd_product_person_ext_csai")
    spark.read.table("dwd.wd_product_person_ext_nsfc").select("person_id","zh_name","zh_title").dropDuplicates().cache().createOrReplaceTempView("wd_product_person_ext_nsfc")

    spark.sql(
      """
        |select
        |ifnull(person_id_to,person_id) as person_id
        |,zh_title from wd_product_person_ext_csai a left join dwb.wb_person_rel_partition b on a.person_id = b.person_id_from
        |""".stripMargin).createOrReplaceTempView("wd_product_person_ext_csai_transform")

    spark.sql(
      """
        |select
        |ifnull(person_id_to,person_id) as person_id
        |,zh_title from wd_product_person_ext_nsfc a left join dwb.wb_person_rel_partition b on a.person_id = b.person_id_from
        |""".stripMargin).createOrReplaceTempView("wd_product_person_ext_nsfc_transform")



    spark.read.table("wd_product_person_ext_csai_transform").select("person_id","zh_title")
      .unionAll(spark.read.table("wd_product_person_ext_nsfc_transform").select("person_id","zh_title"))
      .createOrReplaceTempView("wd_product_person")


    // 62462674
    val person_to = spark.read.table("dwb.wb_person_nsfc_sts_academician_csai").cache()

    // 26316123
    val person_from = spark.read.table("dwd.wd_person_ms").cache()

    val person_from_distinct_pinyin = NameToPinyinUtil.nameToCleanPinyin(spark, person_from, "zh_name")

    val person_from_distinct_with_title  = PersonUtil.getAddTitle(spark,person_from_distinct_pinyin,"wd_product_person_ext_csai")


    //// step 1. owner fusion 自身融合
    val person_from_distinct_rule1 = person_from_distinct_with_title.dropDuplicates("clean_title","clean_en_name_normal")
    var person_from_distinct_rule1_rel = PersonUtil.getDistinctRelation(spark,person_from_distinct_with_title.select("clean_title","clean_en_name_normal","person_id","flow_source"),person_from_distinct_rule1.select("clean_title","clean_en_name_normal","person_id","flow_source"),"clean_title","clean_en_name_normal","","zh_name+title")
    person_from_distinct_rule1_rel = PersonUtil.getDeliverOwnerRelation(spark,person_from_distinct_rule1_rel)

    val person_from_distinct_rule2 = person_from_distinct_rule1.dropDuplicates("clean_title","clean_en_name_inverted")
    var person_from_distinct_rule2_rel = PersonUtil.getDistinctRelation(spark,person_from_distinct_rule1.select("clean_title","clean_en_name_inverted","person_id","flow_source"),person_from_distinct_rule2.select("clean_title","clean_en_name_inverted","person_id","flow_source"),"clean_title","clean_en_name_inverted","","zh_name+title")
    person_from_distinct_rule2_rel = PersonUtil.getDeliverOwnerRelation(spark,person_from_distinct_rule2_rel)

    val distinct_relation = PersonUtil.getDeliverRelation(spark,person_from_distinct_rule1_rel,person_from_distinct_rule2_rel).unionAll(person_from_distinct_rule2_rel).dropDuplicates("person_id_from","person_id_to")



    // 2. 融入数据
    val person_to_with_title  = PersonUtil.getAddTitle(spark,NameToPinyinUtil.nameToCleanPinyin(spark, person_to, "zh_name"),"wd_product_person").cache()

    //val person_from_with_title  = person_from_distinct_rule2.cache()
    val person_from_with_title  = PersonUtil.getAddTitle(spark,person_from_distinct_pinyin,"wd_product_person_ext_csai_transform").cache()

    val person_fusion_1 = PersonUtil.getComparisonTable(spark,person_to_with_title,person_from_with_title,"clean_en_name_normal","clean_title","","zh_name+title")

    val person_fusion_2 = PersonUtil.getComparisonTable(spark,person_to_with_title,person_from_with_title,"clean_en_name_inverted","clean_title","","zh_name+title")

    val person_fusion_3 = PersonUtil.getComparisonTableSecond(spark,person_to_with_title,person_from_with_title,"clean_en_name_normal","clean_en_name_inverted","clean_title","","zh_name+title")

    val person_fusion_relation = person_fusion_1.unionAll(person_fusion_2).unionAll(person_fusion_3).dropDuplicates("person_id_from","person_id_to").cache()

    person_to.unionAll(person_from).createOrReplaceTempView("person_nsfc_sts_academician_csai_ms")

    PersonUtil.getDeliverRelation (spark,distinct_relation,person_fusion_relation).unionAll(person_fusion_relation).dropDuplicates("person_id_from","person_id_to").cache()
      .repartition(20).createOrReplaceTempView("person_rel")
    spark.sql(
      s"""
        |insert overwrite table dwb.wb_person_rel_partition partition (flag='$flag')
        |select * from person_rel
        |""".stripMargin)




    //3.添加flow_source，去重，录入hive
    spark.read.table("dwb.wb_person_rel_partition").filter(s"flag='$flag'").createOrReplaceTempView("comparison_table")
    PersonUtil.getSource(spark,"comparison_table").createOrReplaceTempView("get_source")
    spark.sql(
      """
        |select
        |person_id
        |,zh_name
        |,en_name
        |,gender
        |,nation
        |,birthday
        |,birthplace
        |,org_id
        |,org_name
        |,dept_name
        |,id_card
        |,officer_no
        |,passport_no
        |,hkid_no
        |,twid_no
        |,position
        |,prof_title
        |,prof_title_id
        |,research_area
        |,mobile
        |,tel
        |,email
        |,fax
        |,backup_email
        |,address
        |,nationality
        |,province
        |,city
        |,postcode
        |,avatar_url
        |,degree
        |,degree_year
        |,degree_country
        |,major
        |,brief_description
        |,person_level
        |,a.source
        |,if(b.source is not null, union_flow_source(b.source,flow_source,b.rule),flow_source  )as flow_source
        |from person_nsfc_sts_academician_csai_ms a left join get_source b on a.person_id = b.person_id_to
        |""".stripMargin).createOrReplaceTempView("person_get_source")

    spark.sql(
      """
        |insert overwrite table dwb.wb_person_nsfc_sts_academician_csai_ms
        |select a.*
        |from person_get_source a left join  dwb.wb_person_nsfc_sts_academician_csai_ms_rel b on a.person_id = b.person_id_from where b.person_id_from is null
        |""".stripMargin)





  }
}

