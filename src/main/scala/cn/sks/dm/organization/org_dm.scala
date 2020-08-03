package cn.sks.dm.organization

import org.apache.spark.sql.SparkSession

object org_dm {
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("org_dm")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {

//    spark.sql("""
//                |insert overwrite table dm.dm_neo4j_organization
//                |   select org_id,
//                |   org_name,
//                |   en_name,
//                |   alias,
//                |   org_type,
//                |   country,
//                |   province,
//                |   city from dwb.wb_organization
//                |""".stripMargin)
    spark.sql(
      s"""
         |create table if not exists dm.dm_neo4j_organization_add like dm.dm_neo4j_organization
         |""".stripMargin)

    spark.sql("""
                |insert overwrite table dm.dm_neo4j_organization_add
                |   select org_id,
                |   org_name,
                |   en_name,
                |   alias,
                |   org_type,
                |   country,
                |   province,
                |   city from dwb.wb_organization_add
                |""".stripMargin)

//    spark.sql("""
//                |insert overwrite table dm.dm_es_organization
//                |select org_id,
//                |social_credit_code,
//                |org_name,
//                |en_name,
//                |alias,
//                |registration_date,
//                |org_type,
//                |nature,
//                |isLegalPersonInstitution,
//                |legal_person,
//                |belongtocode,
//                |address,
//                |country,
//                |province,
//                |city,
//                |district,
//                |zipCode,
//                |source
//                | from dwb.wb_organization
//                |""".stripMargin)


    //relationship person_organization
    spark.sql("""
                |insert overwrite table dm.dm_neo4j_person_organization
                |select
                |person_id,
                |organization_id
                |from dwb.wb_organization_person where organization_id is not null
                |""".stripMargin)

    //relationship person_organization

    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_prouduct_org_criterion
        |select
        |achievement_id,
        |org_id
        |from dwb.wb_product_organization where type = 'criterion'
        |""".stripMargin)
    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_prouduct_org_journal
        |select
        |achievement_id,
        |org_id
        |from dwb.wb_product_organization where type = 'journal'
        |""".stripMargin)
    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_prouduct_org_monograph
        |select
        |achievement_id,
        |org_id
        |from dwb.wb_product_organization where type = 'monograph'
        |""".stripMargin)
    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_prouduct_org_patent
        |select
        |achievement_id,
        |org_id
        |from dwb.wb_product_organization where type = 'patent'
        |""".stripMargin)

  }
}
