package cn.sks.dm.person

import org.apache.spark.sql.SparkSession

object RelPerson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("dm_Relation")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()


//    //relationship person_subject
//
//    spark.sql("""
//                |insert overwrite table dm.dm_neo4j_person_subject
//                |select
//                | a.*
//                |from (select * from dwb.wb_person_subject where two_rank_name  is not null) a
//                | join dm.dm_neo4j_person b on a.person_id = b.id
//                |""".stripMargin)
//
//
//    //dm_neo4j_person_keyword
//    spark.sql("""
//                |insert overwrite table dm.dm_neo4j_person_keyword
//                |select
//                |person_id,
//                |keyword_id
//                |from dwb.wb_relation_person_keyword a
//                | join dm.dm_neo4j_person b on a.person_id = b.id
//                |""".stripMargin)


    //society_person
    spark.sql(
      """
        |select society_id,
        |ifnull(person_id_to,person_id) as person_id
        |from ods.o_csai_society_person a left join dwb.wb_person_rel b on a.person_id = b.person_id_from
        |""".stripMargin).createOrReplaceTempView("society_person")

    spark.sql("insert overwrite table dm.dm_neo4j_society_person select * from society_person")






  }

}
