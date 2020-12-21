package cn.sks.dwd.person

import org.apache.spark.sql.SparkSession

object PersonToDwd_new {

  def main(args: Array[String]): Unit = {
    person_to_dwd()
  }

  def person_to_dwd() ={
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("test")
      .config("spark.deploy.mode","4g")
      .config("spark.drivermemory","16g")
      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()





    println("------ms-------")
    spark.sql(
      """
        |select
        |person_id
        |,person_name  as zh_name
        |,null as en_name
        |,null as gender
        |,null as nation
        |,null as birthday
        |,null as birthplace
        |,org_id
        |,org_name
        |,null as dept_name
        |,null as id_card
        |,null as officerno
        |,null as passportno
        |,null as hkidno
        |,null as twidno
        |,null as position
        |,null as prof_title
        |,null as prof_title_id
        |,null as researcharea
        |,null as mobile
        |,null as tel
        |,null as email
        |,null as fax
        |,null as backupemail
        |,null as address
        |,null as nationality
        |,null as province
        |,null as city
        |,null as postcode
        |,null as avatar_url
        |,null as degree
        |,null as degreeyear
        |,null as degreecountry
        |,null as major
        |,null as brief_description
        |,'4'
        |,'ms' as source
        |,concat("{","\"source\"",":","\"ms\"",",""\"table\"",":","\"wd_person_ms\"","," ,"\"person_id\"",":","\"",person_id,"\"","}") as flow_source
        |from ods.o_ms_product_author
        |""".stripMargin).dropDuplicates("person_id")
    .repartition(200).write.format("hive").mode("overwrite").insertInto("dwd.wd_person_ms")



    // arp
    println("-----------arp--------------")
    spark.sql(
      """
        |select
        |person_id
        |,zh_name
        |,null as en_name
        |,gender
        |,null as nation
        |,birthday
        |,null as birthplace
        |,null as org_id
        |,org_name
        |,dept_name
        |,null as  id_card
        |,null as  officer_no
        |,null as  passport_no
        |,null as  hkid_no
        |,null as  twid_no
        |,position
        |,prof_title
        |,null as  prof_title_id
        |,null as  research_area
        |,null as  mobile
        |,null as  tel
        |,email
        |,null as  fax
        |,null as  backup_email
        |,null as  address
        |,null as  nationality
        |,null as  province
        |,null as  city
        |,null as  postcode
        |,null as  avatar_url
        |,null as  degree
        |,null as  degree_year
        |,null as  degree_country
        |,null as  major
        |,null as  brief_description
        |,'1'
        |,'arp' as source
        |,concat("{","\"source\"",":","\"arp\"",",""\"table\"",":","\"wd_person_arp\"","," ,"\"person_id\"",":","\"",person_id,"\"","}") as flow_source
        |from ods.o_arp_person
      """.stripMargin)
    .repartition(20).write.format("hive").mode("overwrite").insertInto("dwd.wd_person_arp")
    // 科协两院院士
    println("-----------academician--------------")
    spark.sql(
      """
        |select
        |a.person_id
        |    ,a.zh_name
        |    ,b.english_name as en_name
        |    ,a.gender
        |    ,a.ethnicity as nation
        |    ,b.birthday as birthday
        |    ,a.birthplace as birthplace
        |    ,null as org_id
        |    ,b.current_organization as org_name
        |    ,null as  dept_name
        |    ,null as  id_card
        |    ,null as  officer_no
        |    ,null as  passport_no
        |    ,null as  hkid_no
        |    ,null as  twid_no
        |    ,null as  position
        |    ,b.title as prof_title
        |    ,null as  prof_title_id
        |    ,null as  research_area
        |    ,null as  mobile
        |    ,null as  tel
        |    ,null as  email
        |    ,null as  fax
        |    ,null as  backup_email
        |    ,null as  address
        |    ,a.nationality as  nationality
        |    ,null as  province
        |    ,null as  city
        |    ,null as  postcode
        |    ,null as  avatar_url
        |    ,null as  degree
        |    ,null as  degree_year
        |    ,null as  degree_country
        |    ,null as  major
        |    ,a.brief_description as  brief_description
        |    ,'2'
        |  ,'academician' as source
        |  ,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_person_academician\"","," ,"\"person_id\"",":","\"",a.person_id,"\"","}") as flow_source
        |from ods.o_csai_person_academician a
        |left join ods.o_csai_person_all b
        |on a.person_id=b.person_id
      """.stripMargin)
      .dropDuplicates("person_id").repartition(2).write.format("hive").mode("overwrite").insertInto("dwd.wd_person_academician")

    // 科协的人员（排除两院院士）
    println("-----------csai--------------")
    spark.sql(
      """
        |select
        |person_id
        |,a.chinese_name         as zh_name
        |,a.english_name         as en_name
        |,a.gender
        |,a.ethnicity            as nation
        |,a.birthday
        |,a.birthplace
        |,null as org_id
        |,a.current_organization as org_name
        |,null as dept_name
        |,null as id_card
        |,null as officerno
        |,null as passportno
        |,null as hkidno
        |,null as twidno
        |,null as position
        |,a.title                as prof_title
        |,null as prof_title_id
        |,null as researcharea
        |,null as mobile
        |,null as tel
        |,null as email
        |,null as fax
        |,null as backupemail
        |,null as address
        |,a.nationality
        |,null as province
        |,null as city
        |,null as postcode
        |,null as avatar_url
        |,null as degree
        |,null as degreeyear
        |,null as degreecountry
        |,null as major
        |,a.brief_description
        |,'3'
        |,'csai' as source
        |,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_person_csai\"","," ,"\"person_id\"",":","\"",a.person_id,"\"","}") as flow_source
        |from ods.o_csai_person_all a
        | where not exists (select * from ods.o_csai_person_academician b where a.person_id=b.person_id)
      """.stripMargin)
    .repartition(200).write.format("hive").mode("overwrite").insertInto("dwd.wd_person_csai")






    // 基金委的人员
    println("-----------nsfc--------------")
    spark.sql(
      """
        |select
        |    person_id
        |   ,zh_name
        |   ,null  as en_name
        |   ,case when trim(gender)= 'M' then '男'  when trim(gender)= 'F' then '女' end as gender
        |   ,c.zh_cn_caption    as nation
        |   ,split(birthday,' ')[0] as birthday
        |   ,null as birthplace
        |   ,org_id
        |   ,org_name
        |   ,b.dept_name
        |
        |   ,identity_card       as   id_card
        |   ,military_id         as   officer_no
        |   ,passport            as   passport_no
        |   ,home_return_permit  as   hkid_no
        |   ,mainland_travel_permit_for_taiwan_residents  as  twid_no
        |
        |   ,position
        |   ,g.prof_title
        |   ,a.prof_title_id
        |   ,null as research_area
        |
        |   ,mobile
        |   ,tel
        |   ,email
        |   ,fax
        |   ,backupemail as backup_email
        |
        |   ,address
        |   ,d.zh_cn_caption   as  nationality
        |   ,province
        |   ,city
        |   ,postcode
        |   ,null as avatar_url
        |
        |   ,f.zh_cn_caption as     degree
        |   ,degreeyear  as degree_year
        |   ,e.zh_cn_caption as degree_country
        |   ,major
        |   ,null as brief_description
        |   ,'0'
        |   ,'nsfc' as source
        |   ,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_person_nsfc\"","," ,"\"person_id\"",":","\"",person_id,"\"","}") as flow_source
        |from ods.o_nsfc_person  a
        |left join ods.o_nsfc_organization_department b
        |on a.dept_code = b.dept_code
        |left join ods.o_nsfc_const_dictionary c
        |on a.ethnicity = c.code and c.category='nation'
        |left join ods.o_nsfc_const_dictionary d
        |on a.regioncode = d.code and d.category='4'
        |left join ods.o_nsfc_const_dictionary e
        |on a.degreecountry = e.code and e.category='4'
        |left join ods.o_nsfc_const_dictionary f
        |on a.degreecode = f.code and f.category='degree'
        |left join ods.o_nsfc_person_prof_title_comparison g
        |on trim(a.prof_title_id)=trim(g.prof_title_id)
      """.stripMargin)
      .repartition(100).write.format("hive").mode("overwrite").insertInto("dwd.wd_person_nsfc")



  }




}
