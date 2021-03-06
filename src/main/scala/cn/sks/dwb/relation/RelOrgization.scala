package cn.sks.dwb.relation

import cn.sks.util.{BuildOrgIDUtil, DefineUDF}
import org.apache.spark.sql.SparkSession

object RelOrgization {
  val spark = SparkSession.builder()
    //.master("local[40]")
    .appName("RelOrgization")
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
    val df = spark.read.table("dm.dm_neo4j_person ")
    BuildOrgIDUtil.buildOrganizationID(spark,df,"org_name","relation_person_org").createOrReplaceTempView("relation_person_org")

    spark.sql(
      """
        |insert overwrite table dwb.wb_organization_person
        |select id,org_id
        | from relation_person_org
        |""".stripMargin)

//    //org_product
//    val criterion = spark.sql("select achievement_id,org_id,org_name,'criterion' as type from ods.o_csai_criterion_org").dropDuplicates("achievement_id","org_id")
//
//    val journal = spark.sql("select achivement_id,org_id,org_name,'journal' as type from ods.o_csai_product_journal_author")
//      .toDF("achievement_id","org_id","org_name","type").dropDuplicates("achievement_id","org_id")
//
//    val patent = spark.sql("select achievement_id,org_id,org_name,'patent' as type from ods.o_csai_product_patent_inventor").dropDuplicates("achievement_id","org_id")
//
//    val monograph = spark.sql("select achievement_id,org_id,org_name,'monograph' as type from ods.o_csai_product_monograph_author").dropDuplicates("achievement_id","org_id")
//
//    val product_id = spark.sql("select achievement_id as id,achievement_id_origin as achievement_id from dwb.wb_product_rel")
//
//    val product_all = criterion.union(journal).union(patent).union(monograph).where("org_name is not null")
//
//    product_all.join(product_id,Seq("achievement_id"),"left")
//      .createOrReplaceTempView("org_product")
//    //spark.read.table("org_product").printSchema()
//
//
//    spark.sql(
//      """
//        |select
//        |ifnull(id,achievement_id) as achievement_id,
//        |id as original_achievement_id,
//        |org_id,
//        |org_name,
//        |type
//        |from org_product
//      """.stripMargin)
//      .repartition(100).createOrReplaceTempView("org_product")
//
//    val df_org_product = spark.read.table("org_product")
//    BuildOrgIDUtil.buildOrganizationID(spark,df_org_product,"org_name","relation_product_org").createOrReplaceTempView("relation_product_org")
//
//
//    spark.sql(
//      """
//        |insert overwrite table dwb.wb_product_organization
//        |select achievement_id,org_id,new_org_name,type
//        | from relation_product_org
//        |""".stripMargin)
//
//    //org_society
//    val org_society_df = spark.read.table("ods.o_csai_society_support_org")
//    BuildOrgIDUtil.buildOrganizationID(spark,org_society_df,"org_name","relation_society_org").select("society_id","org_id").dropDuplicates().createOrReplaceTempView("relation_society_org")
//
//    spark.sql(
//      """
//        |insert overwrite table dwb.wb_organization_society
//        |select society_id,
//        |org_id
//        | from relation_society_org
//        |""".stripMargin)
//
//    //期刊和机构的关系
//
//    val constant_journal = spark.sql("select journal_id,chinese_name,english_name,language,organizer,fullimpact,compleximpact,publish_region from ods.o_csai_journal")
//
//    constant_journal.select("journal_id", "organizer").createOrReplaceTempView("organizer")
//
//    val journal_org_df = spark.sql(
//      """
//        |select
//        |journal_id,
//        |get_json_object(org,'$.orgName')  as org_name
//        |from organizer
//        |lateral view explode(split(regexp_replace(regexp_replace(organizer, '\\[|\\]',''),'\\}\\,\\{','\\}\\;\\{'),'\\;')) t1 as org
//      """.stripMargin)
//
//    BuildOrgIDUtil.buildOrganizationID(spark,journal_org_df,"org_name","relation_journal_org").select("journal_id","org_id").dropDuplicates().createOrReplaceTempView("relation_journal_org")
//
//    spark.sql(
//      """
//        |insert overwrite table dwb.wb_organization_journal
//        |select journal_id,
//        |org_id
//        | from relation_journal_org
//        |""".stripMargin)

  }
}
