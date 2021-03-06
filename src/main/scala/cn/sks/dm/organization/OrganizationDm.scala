package cn.sks.dm.organization

import cn.sks.dm.achievement.Product.spark
import cn.sks.util.AchievementUtil
import org.apache.spark.sql.SparkSession

object OrganizationDm {
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



    spark.sql("""
                |insert overwrite table dm.dm_neo4j_organization
                |   select org_id,
                |   org_name,
                |   en_name,
                |   alias,
                |   org_type,
                |   country,
                |   province,
                |   city,
                |source from dwb.wb_organization
                |""".stripMargin)


    spark.sql("""
                |insert into table dm.dm_neo4j_organization
                |   select org_id,
                |   org_name,
                |   en_name,
                |   alias,
                |   org_type,
                |   country,
                |   province,
                |   city,
                |   source from dwb.wb_organization_add
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



  }
}
