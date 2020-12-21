package cn.sks.dwb.person_partition

import cn.sks.util.{DefineUDF, PersonUtil}
import org.apache.spark.sql.SparkSession

object PersonFusionSts {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      //.master("local[32]")
      .appName("PersonFusionSts")
      .config("spark.local.dir", "/data/tmp")
      .config("spark.deploy.mode","4g")
      .config("spark.drivermemory","16g")
      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val flag = "person_nsfc_sts_rel"

    spark.sqlContext.udf.register("clean_fusion", (str: String) => {
      DefineUDF.clean_fusion(str)
    })
    spark.udf.register("union_flow_source", DefineUDF.unionFlowSource _)

    val person_to = spark.read.table("dwd.wd_person_nsfc")
    val person_to_with_title  = PersonUtil.getAddTitle(spark,person_to,"dwd.wd_product_person_ext_nsfc")
    val person_to_distinct_rule = person_to_with_title.dropDuplicates("zh_name","clean_title")
    val person_to_distinct_rule_rel = PersonUtil.getDistinctRelation(spark,person_to_with_title,person_to_distinct_rule,"zh_name","clean_title","","zh_name+title")

    person_to_distinct_rule_rel.createOrReplaceTempView("person_to_distinct_rule_rel")

    spark.sql(
      """
        |select a.* from dwd.wd_person_nsfc a left join person_to_distinct_rule_rel b on a.person_id = b.person_id_from where b.person_id_from is null
        |""".stripMargin).createOrReplaceTempView("wd_person_nsfc")


    // 4109415
    val person_nsfc = spark.sql("select *,clean_fusion(email) as clean_email,clean_fusion(org_name) as clean_org,substr(birthday,0,4) as birth_year from wd_person_nsfc").cache()
    person_nsfc.createOrReplaceTempView("person_nsfc")

    // 70228
    val origin_arp = spark.sql("select *,clean_fusion(email) as clean_email,clean_fusion(org_name) as clean_org,substr(birthday,0,4) as birth_year from dwd.wd_person_arp").cache()

    val arp_distinct_pname_email = origin_arp.dropDuplicates("zh_name","clean_email")
    val arp_distinct_pname_email_rel = PersonUtil.getDistinctRelation(spark,origin_arp,arp_distinct_pname_email,"zh_name","clean_email","","zh_name+email")
    val arp_distinct_pname_org_birthday = arp_distinct_pname_email.dropDuplicates("zh_name","clean_org","birth_year")
    val arp_distinct_pname_org_birthday_rel = PersonUtil.getDistinctRelation(spark,arp_distinct_pname_email,arp_distinct_pname_org_birthday,"zh_name","clean_org","birth_year","zh_name+org_name+birthday")
    val distinct_relation = PersonUtil.getDeliverRelation(spark,arp_distinct_pname_email_rel,arp_distinct_pname_org_birthday_rel).union(arp_distinct_pname_org_birthday_rel)

    val person_fusion_1 = PersonUtil.getComparisonTable(spark,person_nsfc,arp_distinct_pname_org_birthday,"zh_name","clean_email","","zh_name+email")
    val person_fusion_2 = PersonUtil.getComparisonTable(spark,person_nsfc,arp_distinct_pname_org_birthday,"zh_name","clean_org","birth_year","zh_name+org_name+birthday")

    val person_fusion_relation = person_fusion_1.unionAll(person_fusion_2).dropDuplicates("person_id_from")
    person_nsfc.unionAll(arp_distinct_pname_org_birthday).createOrReplaceTempView("person_nsfc_sts")

    PersonUtil.getDeliverRelation (spark,distinct_relation,person_fusion_relation).unionAll(person_fusion_relation).unionAll(person_to_distinct_rule_rel).dropDuplicates("person_id_from").repartition(2).createOrReplaceTempView("person_rel")
    spark.sql(
      s"""
         |insert overwrite table dwb.wb_person_rel_partition partition (flag='$flag')
         |select * from person_rel
         |""".stripMargin)

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
        |from person_nsfc_sts a left join get_source b on a.person_id = b.person_id_to
        |""".stripMargin).createOrReplaceTempView("person_get_source")

    spark.sql(
      """
        |insert overwrite table dwb.wb_person_nsfc_sts
        |select a.*
        |from person_get_source a left join  dwb.wb_person_nsfc_sts_rel b on a.person_id = b.person_id_from where b.person_id_from is null
        |""".stripMargin)





  }



}

