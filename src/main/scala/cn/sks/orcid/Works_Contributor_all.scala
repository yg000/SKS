package cn.sks.orcid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  summary,(基本信息locale为en和zh)
  educations,(教育经历)
  employments,(工作经历)
  works(成果)
*/
object Works_Contributor_all {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("XmlParse_Works_Contributor")
      .config("spark.driver.memory", "24g")
      .config("spark.executor.memory", "48g")
      .config("spark.cores.max", "32")
      .config("spark.rpc.askTimeout","800")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields","100")
      .config("spark.driver.maxResultSize","24G")
      .config("dfs.namenode.fs-limits.max-directory-items","6400000")
      .enableHiveSupport()
      .getOrCreate()


    val sqldf=spark.sql(
      """
        |select
        |summary_id,
        |works_id,
        |contributor_orcid_host,
        |contributor_orcid_path,
        |contributor_orcid_uri,
        |contributor_role,
        |contributor_sequence,
        |credit_name,
        |md5(summary_id) as summary_uuid,
        |md5(works_id)  as works_uuid
        | from orcid.works_contributor
      """.stripMargin)

    sqldf.repartition(100).write.format("hive").mode("overwrite")
      .insertInto("ods.o_orcid_works_contributor")

    println("=====================ending=====================")

  }
}
