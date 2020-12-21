package cn.sks.dm.person

import org.apache.spark.sql.SparkSession

object Person {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.master("local[12]")
      .appName("Person")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()
//
//    spark.sql(
//      """
//        |select count(*) from dm.dm_neo4j_person a join
//        |""".stripMargin)

    spark.sql(
      """
        |select
        |   a.person_id as id
        |  ,zh_name
        |  ,en_name
        |  ,gender
        |  ,nation
        |  ,replace(birthday,'-','')
        |  ,birthplace
        |  ,org_name
        |  ,prof_title
        |  ,nationality
        |  ,province
        |  ,city
        |  ,degree,
        |  if(d.person_id is not null,'guide',if(c.person_id is not null,'review experts',if(b.person_id is not null,'outstanding',source))),
        |flow_source,
        |source
        |from dwb.wb_person_nsfc_sts_academician_csai_ms a
        |left join dm.dm_neo4j_excel_person_outstanding b on a.person_id = b.person_id
        |left join dm.dm_neo4j_excel_person_special_project c on a.person_id = c.person_id
        |left join dm.dm_neo4j_excel_person_guide d on a.person_id = d.person_id
      """.stripMargin)
      .write.format("hive").mode("overwrite").insertInto("dm.dm_neo4j_person")


    spark.sql(
      """
        |select
        | person_id as id
        | ,name
        | ,null
        | ,null
        | ,null
        | ,null
        | ,null
        | ,org_name
        | ,null
        | ,null
        | ,null
        | ,null
        | ,null
        | ,null
        | ,null
        | ,flag
        | from dwb.wb_person_add
        |""".stripMargin).dropDuplicates("id")
      //.repartition(10).write.format("hive").mode("overwrite").insertInto("dm.dm_neo4j_person_add")

//    spark.sql(
//      """
//        |insert into table dm.dm_es_person
//        |select
//        | person_id as id
//        |,zh_name
//        |,en_name
//        |,gender
//        |,nation
//        |,birthday
//        |,birthplace
//        |,org_name
//        |,prof_title
//        |,nationality
//        |,null as                  province
//        |,null as                  city
//        |,null as                  degree
//        |from  dwb.wb_person_nsfc_sts_academician_artificial
//        |""".stripMargin)
//
//
//
//
//
//    val person_es = spark.sql(
//      """
//        |select  * from dwb.wb_person_nsfc_sts_academician_csai_ms
//      """.stripMargin).dropDuplicates("person_id")
//    person_es.createOrReplaceTempView("person_es")
//
//    spark.sql("insert into table dm.dm_es_person select * from person_es")
//
//    spark.sql(
//      """
//        |insert into table dm.dm_es_person
//        |select
//        | person_id as id
//        |,zh_name
//        |,en_name
//        |,gender
//        |,nation
//        |,birthday
//        |,birthplace
//        |,null as                  org_id
//        |,org_name
//        |,null as                  dept_name
//        |,null as                  idcard
//        |,null as                  officerno
//        |,null as                  passportno
//        |,null as                  hkidno
//        |,null as                  twidno
//        |,null as                  position
//        |,prof_title
//        |,null as                  prof_title_id
//        |,null as                  researcharea
//        |,null as                  mobile
//        |,null as                  tel
//        |,null as                  email
//        |,null as                  fax
//        |,null as                  backupemail
//        |,null as                  address
//        |,nationality
//        |,null as                  province
//        |,null as                  city
//        |,null as                  postcode
//        |,null as                  avatar_url
//        |,null as                  degree
//        |,null as                  degreeyear
//        |,null as                  degreecountry
//        |,null as                  major
//        |,brief_description
//        |,source
//        |from  dwb.wb_person_nsfc_sts_academician_artificial
//        |""".stripMargin)




  }

}
