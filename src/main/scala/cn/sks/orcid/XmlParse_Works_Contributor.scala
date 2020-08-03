package cn.sks.orcid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  summary,(基本信息locale为en和zh)
  educations,(教育经历)
  employments,(工作经历)
  works(成果)
*/
object XmlParse_Works_Contributor {
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

    import spark.implicits._

    val folder="X"
    val xmlpath="/opt/mfs/sks/orcid/"+folder+"/*/*/works/*.xml"
    val rowTag="work:work"

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      .load(xmlpath)
//      .withColumn("common:external-id",explode($"`common:external-ids`.`common:external-id`"))
      .withColumn("work:contributor",explode($"`work:contributors`.`work:contributor`"))

        .selectExpr("_path",
          "`work:contributor`.`work:contributor-attributes`.`work:contributor-role` as contributor_role",
          "`work:contributor`.`work:contributor-attributes`.`work:contributor-sequence` as contributor_sequence",

          "`work:contributor`.`common:contributor-orcid`.`common:host` as contributor_orcid_host",
          "`work:contributor`.`common:contributor-orcid`.`common:path` as contributor_orcid_path",
          "`work:contributor`.`common:contributor-orcid`.`common:uri` as contributor_orcid_uri",
          "`work:contributor`.`work:credit-name` as credit_name")

//    xmlDF.printSchema()
    println("=====================start----"+folder+"-----=====================")

    xmlDF.createOrReplaceTempView("xml")

    val sqldf=spark.sql(
      """
        |select
        |split(_path,"/")[1] as summary_id,
        |concat_ws("_",split(_path,"/")[1],split(_path,"/")[3]) as works_id,
        |contributor_orcid_host,
        |contributor_orcid_path,
        |contributor_orcid_uri,
        |contributor_role,
        |contributor_sequence,
        |credit_name
        | from xml where contributor_role is not null
      """.stripMargin)

    sqldf.repartition(150).write.format("hive").mode("overwrite")
      .insertInto("test_orcid.works_contributor_x")

    println("=====================ending=====================")

  }
}
