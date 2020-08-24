package cn.sks.dwb.person

import cn.sks.util.{DefineUDF, PersonUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PersonFusionAcademician {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      //.master("local[30]")
      .appName("PersonFusionAcademician")
      .config("spark.local.dir", "/data/tmp")
      .config("spark.deploy.mode","4g")
      .config("spark.drivermemory","32g")
      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()
    spark.sqlContext.udf.register("clean_fusion", (str: String) => {
      DefineUDF.clean_fusion(str)
    })
    spark.udf.register("union_flow_source", DefineUDF.unionFlowSource _)

    //4137611
    val person_to = spark.sql("select *,clean_fusion(org_name) as clean_org,substr(birthday,0,4) as birth_year from dwb.wb_person_nsfc_sts").cache()
    person_to.createOrReplaceTempView("person_nsfc_sts")
    //2507
    val person_from = spark.sql("select *,clean_fusion(org_name) as clean_org,substr(birthday,0,4) as birth_year from dwd.wd_person_academician").cache()
    person_from.createOrReplaceTempView("person_academician")


    val person_from_distinct_rule1 = person_from.dropDuplicates("zh_name","clean_org","birth_year")
    //println(person_from_distinct_rule1.count())
    val person_from_distinct_rule1_rel = PersonUtil.getDistinctRelation(spark,person_from,person_from_distinct_rule1,"zh_name","clean_org","birth_year")

    val person_from_distinct_with_title  = PersonUtil.getAddTitle(spark,person_from_distinct_rule1,"dwd.wd_product_person_ext_csai")
    person_from_distinct_with_title.createOrReplaceTempView("mid")
    spark.sql(
      """
        |select count(*),count(distinct person_id) from  mid where clean_title is not null
        |""".stripMargin)//.show()
    //person_from_distinct_with_title.show()
    val person_from_distinct_rule2_rel = PersonUtil.getDistinctRelationSecond(spark,person_from_distinct_with_title,"zh_name","clean_title","birth_year")
    //person_from_distinct_rule2_rel.show()
    val person_from_distinct_rule2 = PersonUtil.getWithOut(spark,person_from_distinct_rule1,person_from_distinct_rule2_rel)
    val distinct_relation = PersonUtil.getDeliverRelation(spark,person_from_distinct_rule1_rel,person_from_distinct_rule2_rel).union(person_from_distinct_rule2_rel)

    val person_to_with_title  = PersonUtil.getAddTitle(spark,person_to,"dwd.wd_product_person_ext_nsfc")
    person_to_with_title.createOrReplaceTempView("mid")
    spark.sql(
      """
        |select count(*),count(distinct person_id) from  mid where clean_title is not null
        |""".stripMargin)//.show()
    val person_from_with_title  = PersonUtil.getAddTitle(spark,person_from_distinct_rule2,"dwd.wd_product_person_ext_csai")

    val person_fusion_1 = PersonUtil.getComparisonTable(spark,person_to,person_from_distinct_rule2,"zh_name","clean_org","birth_year","zh_name+org_name+birthday")
    println(person_fusion_1.count())

    val person_fusion_2 = PersonUtil.getComparisonTable(spark,person_to_with_title,person_from_with_title,"zh_name","clean_title","birth_year","zh_name+title+birthday")
    println(person_fusion_2.count())


    val person_fusion_relation = person_fusion_1.unionAll(person_fusion_2).dropDuplicates("person_id_from")
    person_fusion_relation.createOrReplaceTempView("comparison_table")
    PersonUtil.getSource(spark,"comparison_table").createOrReplaceTempView("get_source")

    person_to.unionAll(person_from_distinct_rule2).createOrReplaceTempView("person_nsfc_sts_academician")

    PersonUtil.getDeliverRelation (spark,distinct_relation,person_fusion_relation).unionAll(person_fusion_relation.select("person_id_from","person_id_to"))
      .repartition(2).write.format("hive").mode("overwrite").insertInto("dwb.wb_person_nsfc_sts_academician_rel")

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
        |,if(b.source is not null, union_flow_source(b.source,flow_source,b.rule),flow_source  )as flow_source
        |,a.source
        |from person_nsfc_sts_academician a left join get_source b on a.person_id = b.person_id_to
        |""".stripMargin).createOrReplaceTempView("person_get_source")

    spark.sql(
      """
        |select a.*
        |from person_get_source a left join  dwb.wb_person_nsfc_sts_academician_rel b on a.person_id = b.person_id_from where b.person_id_from is null
        |""".stripMargin)
      .write.format("hive").mode("overwrite").insertInto("dwb.wb_person_nsfc_sts_academician")





  }

}
