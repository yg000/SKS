package cn.sks.orcid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  summary,(基本信息locale为en和zh)
  educations,(教育经历)
  employments,(工作经历)
  works(成果)
*/
object XmlParse_Works_External_Ids {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("XmlParse_Works_External_Ids")
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

    val folder="1"
    val xmlpath="/opt/mfs/sks/orcid/"+folder+"/*/*/works/*.xml"
    val rowTag="work:work"

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      .load(xmlpath)
      .withColumn("common:external-id",explode($"`common:external-ids`.`common:external-id`"))
        .selectExpr("_path",
          "`common:external-id`.`common:external-id-relationship` as  ext_id_relationship",
          "`common:external-id`.`common:external-id-type` as ext_id_type",
          "`common:external-id`.`common:external-id-url` as ext_id_url",
          "`common:external-id`.`common:external-id-value` as ext_id_value")

//    xmlDF.printSchema()
    println("=====================start----"+folder+"-----=====================")

    xmlDF.createOrReplaceTempView("xml")

    val sqldf=spark.sql(
      """
        |select
        |split(_path,"/")[1] as summary_id,
        |concat_ws("_",split(_path,"/")[1],split(_path,"/")[3]) as works_id,
        |ext_id_relationship,
        |ext_id_type,
        |ext_id_url,
        |ext_id_value
        | from xml where ext_id_type is not null
      """.stripMargin)

    sqldf.repartition(100).write.format("hive").mode("overwrite")
      .insertInto("test_orcid.works_external_"+folder)

    println("=====================ending=====================")

  }
}
