package cn.sks.dwd.person

import java.util.Date

import org.apache.spark.sql.SparkSession

object Person {

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
        |insert into table dwd.wd_person_ms
        |select
        |   person_id
        |  ,person_name  as zh_name
        |  ,org_id
        |  ,org_name
        |  ,concat("{","\"source\"",":","\"ms\"",",""\"table\"",":","\"wd_person_ms\"","," ,"\"person_id\"",":","\"",person_id,"\"","}") as source
        |from ods.o_ms_product_author
        |""".stripMargin)


    // arp
    println("-----------arp--------------")
    spark.sql(
      """
        |insert into dwd.wd_person_arp
        |select
        |   person_id
        |  ,zh_name
        |  ,gender
        |  ,birthday
        |  ,org_name
        |  ,dept_name
        |  ,email
        |  ,prof_title
        |  ,position
        |  ,concat("{","\"source\"",":","\"arp\"",",""\"table\"",":","\"wd_person_arp\"","," ,"\"person_id\"",":","\"",person_id,"\"","}") as source
        |from ods.o_arp_person
      """.stripMargin)

    // 科协两院院士
    println("-----------academician--------------")
    spark.sql(
      """
        |insert into table dwd.wd_person_academician
        |select
        |   a.person_id
        |  ,a.zh_name
        |  ,b.english_name as en_name
        |  ,b.current_organization as org_name
        |  ,a.gender
        |  ,a.birthday
        |  ,a.ethnicity          as nation
        |  ,a.nationality
        |  ,b.title as prof_title
        |  ,a.isacademician
        |  ,a.isoutstanding
        |  ,a.outstanding_title
        |  ,a.academician_selection_date
        |  ,a.birthplace
        |  ,a.brief_description
        |  ,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_person_academician\"","," ,"\"person_id\"",":","\"",a.person_id,"\"","}") as source
        |from ods.o_csai_person_academician a
        |left join ods.o_csai_person_all b
        |on a.person_id=b.person_id
      """.stripMargin)

    // 科协的人员（排除两院院士）
    println("-----------csai--------------")
    spark.sql(
      """
        |insert into dwd.wd_person_csai
        |select
        |   a.person_id
        |  ,a.chinese_name         as zh_name
        |  ,a.english_name         as en_name
        |  ,a.gender
        |  ,a.birthday
        |  ,a.ethnicity            as nation
        |  ,a.nationality
        |  ,a.current_organization as org_name
        |  ,a.title                as prof_title
        |  ,a.isacademician
        |  ,a.isoutstanding
        |  ,a.birthplace
        |  ,a.brief_description
        |  ,concat("{","\"source\"",":","\"csai\"",",""\"table\"",":","\"wd_person_csai\"","," ,"\"person_id\"",":","\"",a.person_id,"\"","}") as source
        |from ods.o_csai_person_all a
        | where not exists (select * from ods.o_csai_person_academician b where a.person_id=b.person_id)
      """.stripMargin)

    // 基金委的人员
    println("-----------nsfc--------------")
    spark.sql(
      """
        |insert into dwd.wd_person_nsfc
        |select
        |    person_id
        |   ,zh_name
        |   ,null  as en_name
        |   ,case when trim(gender)= 'M' then '男'  when trim(gender)= 'F' then '女' end
        |   ,c.zh_cn_caption    as nation
        |   ,split(birthday,' ')[0]
        |   ,null as birthplace
        |   ,org_id
        |   ,org_name
        |   ,b.dept_name
        |
        |   ,identity_card       as   idCard
        |   ,military_id         as   officerNo
        |   ,passport            as   passportNo
        |   ,home_return_permit  as   hkIdNo
        |   ,mainland_travel_permit_for_taiwan_residents  as  twIdNo
        |
        |   ,position
        |   ,g.prof_title
        |   ,a.prof_title_id
        |   ,null as researchArea
        |
        |   ,mobile
        |   ,tel
        |   ,email
        |   ,fax
        |   ,backupemail
        |
        |   ,address
        |   ,d.zh_cn_caption   as  nationality
        |   ,province
        |   ,city
        |   ,postcode
        |   ,null as avatar_url
        |
        |   ,f.zh_cn_caption as     degree
        |   ,degreeyear
        |   ,e.zh_cn_caption as degreecountry
        |   ,major
        |   ,null as brief_description
        |   ,concat("{","\"source\"",":","\"nsfc\"",",""\"table\"",":","\"wd_person_nsfc\"","," ,"\"person_id\"",":","\"",person_id,"\"","}") as source
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



  }




}
