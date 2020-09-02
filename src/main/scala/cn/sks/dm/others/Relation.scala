package cn.sks.dm.others

import org.apache.spark.sql.SparkSession

object Relation {
  val spark: SparkSession = SparkSession.builder()
    .master("local[15]")
    .config("spark.deploy.mode", "clent")
    .config("executor-memory", "12g")
    .config("executor-cores", "6")
    .config("spark.local.dir","/data/tmp")
    //      .config("spark.drivermemory", "32g")
    //      .config("spark.cores.max", "16")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","20")
    .appName("Entity")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {


    //project_subject
    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_project_subject
        |select distinct project_id,two_rank_id
        |from (select * from dwd.wd_product_subject_nsfc_csai where two_rank_id is not null) a
        |join dm.dm_neo4j_project b on a.project_id = b.id
        |""".stripMargin)

    //project_keyword
    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_project_keyword
        |select
        | project_id
        |,keyword_id
        |from dwb.wb_relation_project_keyword a
        |join dm.dm_neo4j_project b on a.project_id = b.id
        |""".stripMargin)


    //keyword_subject
    spark.sql(
      """
        |insert overwrite table dm.`dm_neo4j_keyword_subject`
        |select
        | *
        |from dwb.wb_keyword_subject
        |""".stripMargin)

  }
}
