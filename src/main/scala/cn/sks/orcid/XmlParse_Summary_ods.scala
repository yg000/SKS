package cn.sks.orcid

import org.apache.spark.sql.SparkSession

/*
  summary`,(基本信息locale为en和zh)
  educations`,(教育经历)
  employments`,(工作经历)
  works(成果)
*/
object XmlParse_Summary_ods {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("OrcidXmlParse")
      .config("spark.driver.memory", "24g")
      .config("spark.executor.memory", "48g")
      .config("spark.cores.max", "16")
      .config("spark.rpc.askTimeout","300")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields","200")
      .config("spark.driver.maxResultSize","12G")
      .config("dfs.namenode.fs-limits.max-directory-items","6400000")
      .config("sethive.enforce.bucketing","true")
      .enableHiveSupport()
      .getOrCreate()


  spark.sql(
    """
      |insert into table ods.o_orcid_person
      |select
      |path,
      |concat_ws(" ",per_name_given_names,per_name_family_name) as en_name,
      |concat_ws(",",email)
      |from orcid.summary_origin
    """.stripMargin)

//    spark.sql(
//      """
//        |insert into table ods.o_orcid_product_person
//        |select summary_id,
//        |works_id,
//        |title from test_orcid.works_new_0
//      """.stripMargin)

    println("=====================ending=====================")

  }
}
