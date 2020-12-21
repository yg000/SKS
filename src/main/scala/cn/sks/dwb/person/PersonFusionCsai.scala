package cn.sks.dwb.person

import cn.sks.util.{DefineUDF, PersonUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PersonFusionCsai {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      //.master("local[40]")
      .appName("PersonFusionCsai")
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


    // 4139774
    val person_to = spark.read.table("dwb.wb_person_nsfc_sts_academician").cache()
    person_to.createOrReplaceTempView("person_nsfc_sts_academician")

    // 61682158
    val person_from = spark.read.table("dwd.wd_person_csai").cache()
    spark.sql("select count(*),count(distinct person_id) from dwd.wd_person_csai").show()

    val person_from_distinct_with_title  = PersonUtil.getAddTitle(spark,person_from,"dwd.wd_product_person_ext_csai")

    val person_from_distinct_rule1 = person_from_distinct_with_title.dropDuplicates("zh_name","clean_title")
    val person_from_distinct_rule1_rel = PersonUtil.getDistinctRelation(spark,person_from_distinct_with_title,person_from_distinct_rule1,"zh_name","clean_title","","zh_name+title")


    val distinct_relation = person_from_distinct_rule1_rel
    val person_to_with_title  = PersonUtil.getAddTitle(spark,person_to,"dwd.wd_product_person_ext_nsfc")

    val person_from_with_title  = person_from_distinct_rule1

    val person_fusion = PersonUtil.getComparisonTable(spark,person_to_with_title,person_from_with_title,"zh_name","clean_title","","zh_name+title")

    val person_fusion_relation = person_fusion.dropDuplicates("person_id_from")

    person_fusion_relation.createOrReplaceTempView("comparison_table")
    PersonUtil.getSource(spark,"comparison_table").createOrReplaceTempView("get_source")
    person_to.unionAll(person_from).createOrReplaceTempView("person_nsfc_sts_academician_csai")

    PersonUtil.getDeliverRelation (spark,distinct_relation,person_fusion_relation).unionAll(person_fusion_relation.select("person_id_from","person_id_to")).dropDuplicates("person_id_from")
      .repartition(20).write.format("hive").mode("overwrite").insertInto("dwb.wb_person_nsfc_sts_academician_csai_rel")

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
        |from person_nsfc_sts_academician_csai a left join get_source b on a.person_id = b.person_id_to
        |""".stripMargin).createOrReplaceTempView("person_get_source")

    spark.sql(
      """
        |insert overwrite table dwb.wb_person_nsfc_sts_academician_csai
        |select a.*
        |from person_get_source a left join  dwb.wb_person_nsfc_sts_academician_csai_rel b on a.person_id = b.person_id_from where b.person_id_from is null
        |""".stripMargin)





  }

}
