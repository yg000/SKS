package cn.sks.dwb.person

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

    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    spark.udf.register("union_flow_source", DefineUDF.unionFlowSource _)


    spark.read.table("dwd.wd_product_person_ext_csai").select("person_id","achievement_id","product_type","zh_name","en_name","zh_title","en_title","source")
      .unionAll(spark.read.table("dwd.wd_product_person_ext_nsfc").select("person_id","achievement_id","product_type","zh_name","en_name","zh_title","en_title","source")).createOrReplaceTempView("wd_product_person")
    // 62462674
    val person_to = spark.read.table("dwb.wb_person_nsfc_sts_academician_csai")

    // 26316123
    val person_from = spark.read.table("dwd.wd_person_ms").cache()

    val person_from_distinct_pinyin = NameToPinyinUtil.nameToCleanPinyin(spark, person_from, "zh_name")
    person_from_distinct_pinyin.createOrReplaceTempView("person_from_distinct_pinyin")

    val person_from_distinct_with_title  = PersonUtil.getAddTitle(spark,person_from_distinct_pinyin,"dwd.wd_product_person_ext_csai")

    val person_from_distinct_rule1 = person_from_distinct_with_title.dropDuplicates("clean_title","clean_en_name_normal")
    val person_from_distinct_rule1_rel = PersonUtil.getDistinctRelation(spark,person_from_distinct_with_title.select("clean_title","clean_en_name_normal","person_id"),person_from_distinct_rule1.select("clean_title","clean_en_name_normal","person_id"),"clean_title","clean_en_name_normal","","zh_name+title")
    val person_from_distinct_rule2 = person_from_distinct_rule1.dropDuplicates("clean_title","clean_en_name_inverted")
    val person_from_distinct_rule2_rel = PersonUtil.getDistinctRelation(spark,person_from_distinct_rule1.select("clean_title","clean_en_name_inverted","person_id"),person_from_distinct_rule2.select("clean_title","clean_en_name_inverted","person_id"),"clean_title","clean_en_name_inverted","","zh_name+title")

    val distinct_relation = PersonUtil.getDeliverRelation(spark,person_from_distinct_rule1_rel,person_from_distinct_rule2_rel).unionAll(person_from_distinct_rule2_rel).dropDuplicates("person_id_from")

    val person_to_with_title  = PersonUtil.getAddTitle(spark,NameToPinyinUtil.nameToCleanPinyin(spark, person_to, "zh_name"),"wd_product_person").cache()

    val person_from_with_title  = person_from_distinct_rule2.cache()

    val person_fusion_1 = PersonUtil.getComparisonTable(spark,person_to_with_title,person_from_with_title,"clean_en_name_normal","clean_title","","zh_name+title")

    val person_fusion_2 = PersonUtil.getComparisonTable(spark,person_to_with_title,person_from_with_title,"clean_en_name_inverted","clean_title","","zh_name+title")

    val person_fusion_3 = PersonUtil.getComparisonTableSecond(spark,person_to_with_title,person_from_with_title,"clean_en_name_normal","clean_en_name_inverted","clean_title","","zh_name+title")

    val person_fusion_relation = person_fusion_1.unionAll(person_fusion_2).unionAll(person_fusion_3).dropDuplicates("person_id_from").cache()

    person_fusion_relation.createOrReplaceTempView("comparison_table")
    PersonUtil.getSource(spark,"comparison_table").createOrReplaceTempView("get_source")

    person_to.unionAll(person_from).createOrReplaceTempView("person_nsfc_sts_academician_csai_ms")

    PersonUtil.getDeliverRelation (spark,distinct_relation,person_fusion_relation).unionAll(person_fusion_relation.select("person_id_from","person_id_to")).dropDuplicates("person_id_from")
      .repartition(20).write.format("hive").mode("overwrite").insertInto("dwb.wb_person_nsfc_sts_academician_csai_ms_rel")

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

