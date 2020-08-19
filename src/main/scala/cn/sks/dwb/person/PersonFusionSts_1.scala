package cn.sks.dwb.person

import cn.sks.util.{CommonUtil, DefineUDF}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PersonFusionSts_1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("PersonFusionSts_1")
      .config("spark.deploy.mode","4g")
      .config("spark.drivermemory","16g")
      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()


    spark.sqlContext.udf.register("cleanField", (str: String) => {
      DefineUDF.clean_fusion(str)
    })

    val person_nsfc = spark.sql("select *,cleanField(email) as clean_email,cleanField(org_name) as clean_org,substr(birthday,0,4) as birth_year from dwd.wd_person_nsfc").cache()
    person_nsfc.createOrReplaceTempView("person_nsfc")

    // 70228
    val origin_arp = spark.sql("select *,cleanField(email) as clean_email,cleanField(org_name) as clean_org,substr(birthday,0,4) as birth_year from dwd.wd_person_arp").cache()


    val arp_distinct_email = DistinctArp(spark,origin_arp,"zh_name,clean_email")
    val arp_distinct_email_rel = arp_distinct_email._2
    arp_distinct_email_rel.createOrReplaceTempView("arp_distinct_email_rel")

    val arp_distinct_birth = DistinctArp(spark,arp_distinct_email._1,"zh_name,clean_org,birth_year")
    val arp_distinct_birth_rel = arp_distinct_birth._2

    // 67855
    val person_arp = arp_distinct_birth._1

    arp_distinct_birth_rel.createOrReplaceTempView("arp_distinct_birth_rel")
    person_arp.createOrReplaceTempView("person_arp")

    arp_distinct_email_rel.show(2)
    arp_distinct_birth_rel.show(3)
    println(person_arp.count())

    val df = spark.sql(
      """
        |
        |select a.old
        |    ,a.old_flow_source
        |    ,if(b.new is null,a.new,b.new) as new
        |    ,if(b.new_flow_source is null,a.new_flow_source,b.new_flow_source) as new_flow_source
        | from arp_distinct_email_rel a
        |left join arp_distinct_birth_rel b  on a.new =b.old
        |
      """.stripMargin).union(arp_distinct_birth_rel)

    println(df.count())
    println(arp_distinct_email_rel.count())



    println("----------------")
    while (true){

    }

    // 1 -------- fusion (zh_name  email  )
    //23193
    val rel_email = spark.sql(
      """
        |select * from (
        |    select
        |       a.person_id as originID
        |      ,a.flow_source as originFlowSource
        |      ,b.person_id as targetID
        |      ,b.flow_source as targetFlowSource
        |    from person_arp a
        |    join  person_nsfc b on a.zh_name=b.zh_name and cleanString(a.email)=cleanString(b.email) and a.email is not  null
        |)a group by originID,originFlowSource,targetID,targetFlowSource
      """.stripMargin)
    rel_email.createOrReplaceTempView("rel_email")

    //23166
    val rel_email_source: DataFrame = CommonUtil.splicFlowSource(spark,rel_email,"originFlowSource","targetID","targetFlowSource","zh_name+email")


    // 2 -------- fusion (zh_name  org_name   birthday)
    //47060
    val email_not_exists = spark.sql("select * from person_arp a where not exists (select * from rel_email b where a.person_id=b.originID)")
    email_not_exists.createOrReplaceTempView("email_not_exists")

    // 17675
    val rel_org_birthday =spark.sql(
      """
        |select * from (
        |    select
        |          a.person_id as originID
        |         ,a.flow_source as originFlowSource
        |         ,b.person_id as targetID
        |         ,b.flow_source as targetFlowSource
        |     from email_not_exists a
        |    join  person_nsfc b on a.zh_name=b.zh_name and cleanString(a.org_name)=cleanString(b.org_name) and substr(a.birthday,0,4) = substr(b.birthday,0,4)
        |    and a.org_name is not null and a.birthday is not null
        |)a group by originID,originFlowSource,targetID,targetFlowSource
      """.stripMargin)
    rel_org_birthday.createOrReplaceTempView("rel_org_birthday")

    // 17626
    val rel_org_birthday_source: DataFrame = CommonUtil.splicFlowSource(spark,rel_org_birthday,"originFlowSource","targetID","targetFlowSource","zh_name+org_name+birthday_year")

    // 29398
    val org_birthday_not_exists = spark.sql("select * from email_not_exists a where not exists (select * from rel_org_birthday b where a.person_id=b.originID)")
    org_birthday_not_exists.createOrReplaceTempView("org_birthday_not_exists")

    val rel_person = rel_email.union(rel_org_birthday)
    rel_person.createOrReplaceTempView("rel_person")

    val rel_person_source = rel_email_source.union(rel_org_birthday_source)
    rel_person_source.createOrReplaceTempView("rel_person_source")


    spark.sql("insert into table  dwb.wb_person_nsfc_sts_rel select originID as person_id_arp,targetID as person_id  from rel_person")

    spark.sql(
      """
        |insert into table dwb.wb_person_nsfc_sts
        | select
        |    person_id
        |   ,zh_name
        |   ,null as en_name
        |   ,gender
        |   ,null as nation
        |   ,birthday
        |   ,null as birthplace
        |   ,null as org_id
        |   ,org_name
        |   ,dept_name
        |   ,null as  id_card
        |   ,null as  officer_no
        |   ,null as  passport_no
        |   ,null as  hkid_no
        |   ,null as  twid_no
        |   ,position
        |   ,prof_title
        |   ,null as  prof_title_id
        |   ,null as  research_area
        |   ,null as  mobile
        |   ,null as  tel
        |   ,email
        |   ,null as  fax
        |   ,null as  backup_email
        |   ,null as  address
        |   ,null as  nationality
        |   ,null as  province
        |   ,null as  city
        |   ,null as  postcode
        |   ,null as  avatar_url
        |   ,null as  degree
        |   ,null as  degree_year
        |   ,null as  degree_country
        |   ,null as  major
        |   ,null as  brief_description
        |   ,source
        |   ,flow_source
        |from org_birthday_not_exists
      """.stripMargin)

    spark.sql(
      """
        | insert into table dwb.wb_person_nsfc_sts
        | select
        |     a.person_id
        |    ,a.zh_name
        |    ,a.en_name
        |    ,gender
        |    ,a.nation
        |    ,birthday
        |    ,a.birthplace
        |    ,org_id
        |    ,org_name
        |    ,dept_name
        |    ,a.id_card
        |    ,a.officer_no
        |    ,a.passport_no
        |    ,a.hkid_no
        |    ,a.twid_no
        |    ,a.position
        |    ,prof_title
        |    ,prof_title_id
        |    ,a.research_area
        |    ,a.mobile
        |    ,a.tel
        |    ,email
        |    ,a.fax
        |    ,a.backup_email
        |    ,a.address
        |    ,a.nationality
        |    ,a.province
        |    ,a.city
        |    ,a.postcode
        |    ,a.avatar_url
        |    ,a.degree
        |    ,a.degree_year
        |    ,a.degree_country
        |    ,a.major
        |    ,a.brief_description
        |    ,source
        |    ,if(b.flow_source is null ,a.flow_source,b.flow_source) as source
        | from person_nsfc a
        | left join rel_person_source b
        | on a.person_id = b.targetID
      """.stripMargin)




  }

  def DistinctArp(spark:SparkSession,originDf:DataFrame,distinctFields:String):(DataFrame,DataFrame) = {

    originDf.createOrReplaceTempView("temp")

    val distinctDf = spark.sql(
      s"""
        |select * from (
        |select *, row_number() over (partition by ${distinctFields} order by person_id ) rank from temp
        |)a where rank =1
      """.stripMargin).drop("rank")
    distinctDf.createOrReplaceTempView("distinctDf")


    var rel_df:DataFrame=null
    if (distinctFields.contains("birth_year")) {

      rel_df = spark.sql(
        """
          |select * from (
          |    select a.person_id as `old`,a.flow_source as old_flow_source,b.person_id as `new`,b.flow_source as new_flow_source from temp a left join distinctDf b
          |    on a.zh_name=b.zh_name and  a.clean_org=b.clean_org and a.birth_year=b.birth_year
          |)a where `new` <> old
        """.stripMargin)
    } else {
      rel_df= spark.sql(
        """
          |select * from (
          |    select a.person_id as `old`,a.flow_source as old_flow_source,b.person_id as `new`,b.flow_source as new_flow_source from temp a left join distinctDf b
          |    on a.zh_name=b.zh_name and  a.clean_email=b.clean_email
          |)a where `new` <> old
        """.stripMargin)
    }


    (distinctDf,rel_df)
  }

}

