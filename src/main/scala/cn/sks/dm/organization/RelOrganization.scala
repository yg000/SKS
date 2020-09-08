package cn.sks.dm.organization

import org.apache.spark.sql.SparkSession

object RelOrganization {
  val spark = SparkSession.builder()
    .master("local[20]")
    .appName("org_rel")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","50")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {


    //relationship person_organization
    spark.sql("""
                |select
                |person_id,
                |organization_id
                |from dwb.wb_organization_person where organization_id is not null
                |""".stripMargin)
      .write.format("hive").mode("overwrite").insertInto("dm.dm_neo4j_person_organization")


//    //relationship society_org
//    spark.sql("""
//                |insert overwrite table dm.dm_neo4j_society_organization
//                |select
//                |society_id,
//                |organization_id
//                |from dwb.wb_organization_society where organization_id is not null
//                |""".stripMargin)
//
//    //relationship journal_organization
//    spark.sql("""
//                |insert overwrite table dm.dm_neo4j_journal_organization
//                |select
//                |journal_id,
//                |organization_id
//                |from dwb.wb_organization_journal where organization_id is not null
//                |""".stripMargin)

    //relationship prouduct_organization
    //    spark.sql(
    //      """
    //        |insert overwrite table dm.dm_neo4j_prouduct_org_criterion
    //        |select
    //        |achievement_id,
    //        |org_id
    //        |from dwb.wb_product_organization where type = 'criterion'
    //        |""".stripMargin)
    //    spark.sql(
    //      """
    //        |insert overwrite table dm.dm_neo4j_prouduct_org_journal
    //        |select
    //        |achievement_id,
    //        |org_id
    //        |from dwb.wb_product_organization where type = 'journal'
    //        |""".stripMargin)
    //    spark.sql(
    //      """
    //        |insert overwrite table dm.dm_neo4j_prouduct_org_monograph
    //        |select
    //        |achievement_id,
    //        |org_id
    //        |from dwb.wb_product_organization where type = 'monograph'
    //        |""".stripMargin)
    //    spark.sql(
    //      """
    //        |insert overwrite table dm.dm_neo4j_prouduct_org_patent
    //        |select
    //        |achievement_id,
    //        |org_id
    //        |from dwb.wb_product_organization where type = 'patent'
    //        |""".stripMargin)

  }
}
