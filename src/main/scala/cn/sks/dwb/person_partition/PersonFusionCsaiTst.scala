package cn.sks.dwb.person_partition

import cn.sks.util.{DefineUDF, PersonUtil}
import org.apache.spark.sql.SparkSession

object PersonFusionCsaiTst {

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

    val flag = "person_nsfc_sts_academician_csai_rel"

    spark.sqlContext.udf.register("clean_fusion", (str: String) => {
      DefineUDF.clean_fusion(str)
    })
    spark.udf.register("union_flow_source", DefineUDF.unionFlowSource _)


    //ready
    spark.sql(
      s"""
         |ALTER TABLE dwb.wb_person_rel_partition DROP IF EXISTS PARTITION (flag='$flag')
         |""".stripMargin)
    spark.sql(
      s"""
         |ALTER TABLE dwb.wb_person_rel_partition DROP IF EXISTS PARTITION (flag='${flag}_tst')
         |""".stripMargin)
    spark.sql(
      s"""
         |ALTER TABLE dwb.wb_person_rel_partition DROP IF EXISTS PARTITION (flag='${flag}_tst0')
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

    // 4139774
    val person_to = spark.read.table("dwb.wb_person_nsfc_sts_academician").cache()

    // 61682158
    val person_from = spark.read.table("dwd.wd_person_csai").filter("org_name is not null").cache()

    val person_from_distinct_with_title  = PersonUtil.getAddTitle(spark,person_from,"wd_product_person_ext_csai")

    val person_from_distinct_rule1 = PersonUtil.dropDuplicates(spark,person_from_distinct_with_title,"zh_name","clean_title")
    val person_from_distinct_rule1_rel = PersonUtil.getDistinctRelation(spark,person_from_distinct_with_title,person_from_distinct_rule1,"zh_name","clean_title","","zh_name+title").cache()

    val distinct_from_relation = PersonUtil.getDeliverOwnerRelation(spark,person_from_distinct_rule1_rel).cache()

    distinct_from_relation.createOrReplaceTempView("tmp_rel")
    spark.sql(
      s"""
         |insert overwrite table dwb.wb_person_rel_partition partition (flag='$flag')
         |select * from tmp_rel
         |""".stripMargin)

    val person_to_with_title  = PersonUtil.getAddTitle(spark,person_to,"wd_product_person_ext_nsfc")
    //val person_from_with_title  = person_from_distinct_rule1
    val person_from_with_title  = PersonUtil.getAddTitle(spark,person_from,"wd_product_person_ext_csai_transform")


    val person_fusion = PersonUtil.getComparisonTable(spark,person_to_with_title,person_from_with_title,"zh_name","clean_title","","zh_name+title")

    val person_fusion_relation = person_fusion.dropDuplicates("person_id_from","person_id_to").cache()

    person_fusion_relation.createOrReplaceTempView("comparison_table")
    PersonUtil.getSource(spark,"comparison_table").createOrReplaceTempView("get_source")


    PersonUtil.getDeliverRelation (spark,distinct_from_relation,person_fusion_relation).unionAll(person_fusion_relation.select("person_id_from","person_id_to")).dropDuplicates("person_id_from","person_id_to")
      .createOrReplaceTempView("person_rel")

    spark.sql(
      s"""
         |insert overwrite table dwb.wb_person_rel_partition partition (flag='$flag')
         |select * from person_rel
         |""".stripMargin)


    spark.sql(
      """
        |select
        |ifnull(person_id_to,person_id) as person_id
        |,zh_title from wd_product_person_ext_csai a left join dwb.wb_person_rel_partition b on a.person_id = b.person_id_from
        |""".stripMargin).createOrReplaceTempView("wd_product_person_ext_csai_transform_2")


    val person_to_distinct = person_to.unionAll(person_from)

    val person_to_distinct_with_title  = PersonUtil.getAddTitle(spark,person_to_distinct,"wd_product_person")

    val person_to_distinct_rule1 = PersonUtil.dropDuplicates(spark,person_to_distinct_with_title,"zh_name","clean_title")
    val person_to_distinct_rule1_rel = PersonUtil.getDistinctRelation(spark,person_to_distinct_with_title,person_to_distinct_rule1,"zh_name","clean_title","","zh_name+title").cache()

    val distinct_to_relation = PersonUtil.getDeliverOwnerRelation (spark,person_to_distinct_rule1_rel).cache()


    distinct_to_relation.createOrReplaceTempView("tmp")

    spark.sql(
      s"""
         |insert overwrite table dwb.wb_person_rel_partition partition (flag='${flag}_tst0')
         |select * from tmp
         |""".stripMargin)

    val person_rel = spark.sql(
      s"""
        |select * from dwb.wb_person_rel_partition where flag='$flag'
        |""".stripMargin)


    PersonUtil.getDeliverRelation (spark,person_rel,distinct_to_relation).unionAll(distinct_to_relation).dropDuplicates("person_id_from","person_id_to")
      .repartition(20).createOrReplaceTempView("person_rel")

    spark.sql(
      s"""
         |insert overwrite table dwb.wb_person_rel_partition partition (flag='${flag}_tst')
         |select * from person_rel
         |""".stripMargin)

    // xie ru  dwb.wb_person_nsfc_sts_academician_csai
    person_to.unionAll(spark.read.table("dwd.wd_person_csai")).createOrReplaceTempView("person_nsfc_sts_academician_csai")
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
        |from person_get_source a left join  dwb.wb_person_rel_partition b on a.person_id = b.person_id_from where b.person_id_from is null
        |""".stripMargin)



  }

}
