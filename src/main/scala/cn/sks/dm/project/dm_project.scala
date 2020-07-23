package cn.sks.dm.project

import org.apache.spark.sql.SparkSession

object dm_project {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("dm_project")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("insert into table dm.dm_neo4j_project_product_conference  select * from dwb.wb_project_product  a" +
      " where exists (select * from dm.dm_neo4j_product_conference b where a.achievement_id=b.id)            ")

    spark.sql("insert into table dm.dm_neo4j_project_product_criterion   select * from dwb.wb_project_product  a" +
      " where exists (select * from dm.dm_neo4j_product_criterion b where a.achievement_id=b.id)              ")

    spark.sql("insert into table dm.dm_neo4j_project_product_journal     select * from dwb.wb_project_product  a" +
      " where exists (select * from dm.dm_neo4j_product_journal b where a.achievement_id=b.id)              ")

    spark.sql("insert into table dm.dm_neo4j_project_product_monograph   select * from dwb.wb_project_product  a" +
      "  where exists (select * from dm.dm_neo4j_product_monograph b where a.achievement_id=b.id)            ")

    spark.sql("insert into table dm.dm_neo4j_project_product_patent      select * from dwb.wb_project_product  a" +
      " where exists (select * from dm.dm_neo4j_product_patent b where a.achievement_id=b.id)            ")



    spark.sql("insert into table dm.dm_neo4j_project_keywords               select * from dwb.wb_project_keywords             ")
    spark.sql("insert into table dm.dm_neo4j_project_person_lead            select * from dwb.wb_project_person_lead          ")
    spark.sql("insert into table dm.dm_neo4j_project_person_participation   select * from dwb.wb_project_person_participation ")

    spark.sql("insert into table dm.dm_neo4j_project_reward                 select * from dwb.wb_project_reward               ")
    spark.sql("insert into table dm.dm_neo4j_project_special_project        select * from dwb.wb_project_special_project      ")
    spark.sql("insert into table dm.dm_neo4j_project_subject select project_id,two_rank_id from dwb.wb_project_subject where two_rank_id is not null")


    spark.sql(
      """
        |insert into table dm.dm_neo4j_project
        |select
        | project_id as id
        |,zh_title
        |,en_title
        |,prj_no
        |,person_id
        |,psn_name
        |,org_name
        |,grant_code
        |,grant_name
        |,subject_code1
        |,subject_code2
        |,start_date
        |,end_date
        |,approval_year
        |,duration
        |,status
        |,source
        |
        | from dwb.wb_project
      """.stripMargin)
    spark.sql("insert into table  dm.dm_es_project  select * from dwb.wb_project")









  }

}
