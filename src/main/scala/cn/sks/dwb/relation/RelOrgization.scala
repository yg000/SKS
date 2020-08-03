package cn.sks.dwb.relation

import cn.sks.util.{BuildOrgIDUtil, DefineUDF}
import org.apache.spark.sql.SparkSession

object RelOrgization {
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




    //org_person
//    val df = spark.read.table("dm.dm_neo4j_person ")
//    BuildOrgIDUtil.buildOrganizationID(spark,df,"org_name","relation_person_org").createOrReplaceTempView("relation_person_org")
//
////    spark.sql(
////      """
////        |insert overwrite table dwb.wb_organization_person
////        |select id,org_id
////        | from relation_person_org
////        |""".stripMargin)

    //org_product
    val criterion = spark.sql("select achievement_id,org_id,org_name,'criterion' as type from ods.o_csai_criterion_org").dropDuplicates("achievement_id","org_id")

    val journal = spark.sql("select achivement_id,org_id,org_name,'journal' as type from ods.o_csai_product_journal_author").dropDuplicates("achievement_id","org_id")

    val patent = spark.sql("select achievement_id,org_id,org_name,'patent' as type from ods.o_csai_product_patent_inventor").dropDuplicates("achievement_id","org_id")

    val monograph = spark.sql("select achievement_id,org_id,org_name,'monograph' as type from ods.o_csai_product_monograph_author").dropDuplicates("achievement_id","org_id")

    val product_id = spark.sql("select achievement_id as id,achievement_id_origin as achievement_id from dwb.wb_product_rel")

    val product_all = criterion.union(journal).union(patent).union(monograph).where("org_name is not null")

    product_all.join(product_id,Seq("achievement_id"),"left")
      .createOrReplaceTempView("org_product")
    //spark.read.table("org_product").printSchema()


    spark.sql(
      """
        |select
        |ifnull(id,achievement_id) as achievement_id,
        |id as original_achievement_id,
        |org_id,
        |org_name,
        |type
        |from keyword
      """.stripMargin)
      .repartition(100).createOrReplaceTempView("org_product")

    val df_org_product = spark.read.table("org_product")
    BuildOrgIDUtil.buildOrganizationID(spark,df_org_product,"org_name","relation_product_org").createOrReplaceTempView("relation_product_org")


    spark.sql(
      """
        |insert overwrite table dwb.wb_product_organization
        |select achievement_id,org_id,new_org_name,type
        | from relation_product_org
        |""".stripMargin)





  }
}
