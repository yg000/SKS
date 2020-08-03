package cn.sks.nsl

import org.apache.spark.sql.SparkSession

object dm_advisor_student_neo4j {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("product_keywords_nsfc_clean_translate")
      .config("spark.driver.memory", "16g")
      .config("spark.executor.memory", "32g")
      .config("spark.cores.max", "8")
      .config("spark.rpc.askTimeout", "300")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields", "200")
      .config("spark.driver.maxResultSize", "4G")
      .config("sethive.enforce.bucketing", "true")
      .enableHiveSupport()
      .getOrCreate()



    println("========人==========")
    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_person_nsl select
        |person_id as id,
        |name      as zh_name,
        |'null'    as en_name,
        |'null'    as gender,
        |'null'    as nation,
        |'null'    as birthday,
        |'null'    as birthplace,
        |org_name  as org_name,
        |'null'    as prof_title,
        |'null'    as nationality,
        |'null'    as province,
        |'null'    as city,
        |'null'    as degree
        |from dwb.wb_person_nsl group by
        |id,
        |zh_name,
        |en_name,
        |gender,
        |nation,
        |birthday,
        |birthplace,
        |org_name,
        |prof_title,
        |nationality,
        |province,
        |city,
        |degree
      """.stripMargin)


    println("========导师========")

    spark.sql("insert overwrite table dm.dm_neo4j_person_advisor select * from dwb.wb_person_advisor_rel ")

    spark.sql("insert overwrite table dm.dm_neo4j_person_advisor_nsl select * from dwb.wb_person_advisor_nsl_rel ")
    spark.sql("insert overwrite table dm.dm_neo4j_person_nsl_advisor_nsl select * from dwb.wb_person_advisor_nsl_rel ")

    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_advisor select
        |children_id as id,
        |children_name as zh_name,
        |'null' as en_name,
        |'null' as gender,
        |'null' as nation,
        |'null' as birthday,
        |'null' as birthplace,
        |children_org_name as org_name,
        |'null' as prof_title,
        |'null' as nationality,
        |'null' as province,
        |'null' as city,
        |'null' as degree
        |from dwb.wb_person_advisor_nsl
      """.stripMargin)


    println("========学生========")

    spark.sql("insert overwrite table dm.dm_neo4j_person_student select * from dwb.wb_person_children_rel")

    spark.sql("insert overwrite table dm.dm_neo4j_person_student_nsl select * from dwb.wb_person_children_nsl_rel")
    spark.sql("insert overwrite table dm.dm_neo4j_person_nsl_student_nsl select * from dwb.wb_person_children_nsl_rel")

    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_student select
        |children_id as id,
        |children_name as zh_name,
        |'null' as en_name,
        |'null' as gender,
        |'null' as nation,
        |'null' as birthday,
        |'null' as birthplace,
        |children_org_name as org_name,
        |'null' as prof_title,
        |'null' as nationality,
        |'null' as province,
        |'null' as city,
        |'null' as degree
        |from dwb.wb_person_children_nsl
      """.stripMargin)

  }
}
