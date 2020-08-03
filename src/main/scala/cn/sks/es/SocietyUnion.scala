package cn.sks.es

import org.apache.spark.sql.SparkSession

object SocietyUnion {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .master("local[40]")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("neo4jcsv")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    val society_support_org = spark.sql("select id as society_id,org_id  from dm.dm_neo4j_society_org")
    val org = spark.sql("select id as org_id,org_name from dm.dm_es_organization")
    society_support_org.join(org, Seq("org_id")).createOrReplaceTempView("org")

    val org_2 = spark.sql(
      """
        |select
        |society_id,
        |concat("[",concat_ws(",",collect_set(to_json(struct(org_name as name)))),"]")  as org_2
        |from org group by society_id
      """.stripMargin)


    val society = spark.sql("select * from ods.o_csai_society")

    val society_id = spark.sql("select society_id as picture_id,id_society as society_id from ods.o_csai_society_id")

    society.join(society_id, Seq("society_id"), "left")
           .join(org_2, Seq("society_id"), "left")
      .createOrReplaceTempView("tem")

    spark.sql(
      """
        |select
        |society_id as id
        |,chinese_name
        |,null as english_name
        |,system
        |,subject_category
        |,address
        |,establish_date
        |,governing_body
        |,org_2 as support_unit
        |,appointed_time
        |,unit_member
        |,individual_member
        |,branch_num
        |,picture_id
        |from tem
      """.stripMargin).repartition(1).createOrReplaceTempView("result")

    spark.sql("insert overwrite table dm.dm_es_society  select * from result")





  }
}
