package cn.sks.dwb.organization

import org.apache.spark.sql.SparkSession
import cn.sks.util.{DefineUDF,BuildOrgIDUtil}

object org_relation {
  val spark = SparkSession.builder()
    .master("local[12]")
    .appName("dddd")
    .config("spark.deploy.mode", "client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions", "10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")



  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })

  def main(args: Array[String]): Unit = {




//    val df = spark.read.table("dm.dm_neo4j_person ")
//    BuildOrgIDUtil.buildOrganizationID(spark,df,"org_name","relation_person_org").createOrReplaceTempView("relation_person_org")
//
////    spark.sql(
////      """
////        |insert overwrite table dwb.wb_organization_person
////        |select id,org_id
////        | from relation_person_org
////        |""".stripMargin)

    val df01 = spark.read.table("dwb.wb_product_organization_all")
    BuildOrgIDUtil.buildOrganizationID(spark,df01,"org_name","relation_product_org").createOrReplaceTempView("relation_product_org")




    spark.sql(
      """
        |insert overwrite table dwb.wb_product_organization_all_tmp
        |select achievement_id,org_id,new_org_name,type
        | from relation_product_org
        |""".stripMargin)





  }
}
