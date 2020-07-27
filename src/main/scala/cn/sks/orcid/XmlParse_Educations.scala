package cn.sks.orcid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
/*
  summary,(基本信息locale为en和zh)
  educations,(教育经历)
  employments,(工作经历)
  works(成果)  cn.sks.orcid.XmlParse_Educations
*/
object XmlParse_Educations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("XmlParse_Educations")
      .config("spark.driver.memory", "12g")
      .config("spark.executor.memory", "48g")
      .config("spark.cores.max", "32")
      .config("spark.rpc.askTimeout","800")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields","100")
      .enableHiveSupport()
      .getOrCreate()

//    val xmlpath=args(0)
//    val rowTag=args(1)

    import spark.implicits._

    val folder="9"
    val rowTag="education:education"
    val xmlpath="/data/ORCID/"+folder+"/*/*/educations/*.xml"

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      .load(xmlpath)
      .withColumnRenamed("department-name","department_name")
      .withColumnRenamed("role-title","role_title")
      .withColumnRenamed("start-date","start_date")
      .withColumnRenamed("end-date","end_date")
      .withColumnRenamed("created-date","created_date")
      .withColumnRenamed("last-modified-date","last_modified_date")
        .selectExpr("department_name",
          "_path",
          "created_date",
          "last_modified_date",
          "source",
          "role_title",
          "organization",
          "start_date",
          "end_date",
          "url")
//    xmlDF.show(10)
    xmlDF.printSchema()
    println("=================start"+folder+"================")

    xmlDF.createOrReplaceTempView("xml")
    val sqldf=spark.sql(
      """
        |select
        |split(_path,"/")[1] as summary_id,
        |concat_ws("_",split(_path,"/")[1],split(_path,"/")[3]) as education_id,
        |source.source_orcid.path as source_orcid_path,
        |source.source_orcid.host as source_orcid_host,
        |source.source_orcid.uri as source_orcid_uri,
        |created_date,
        |department_name,
        |last_modified_date,
        |organization.address.city as org_city,
        |organization.address.region as org_region,
        |organization.address.country as org_country,
        |organization.disambiguated_organization.disambiguated_organization_identifier as disambiguated_organization_identifier,
        |organization.disambiguated_organization.disambiguation_source as disambiguation_source,
        |organization.name as org_name,
        |role_title,
        |source.source_name as source_name,
        |start_date.year as start_year,
        |start_date.month as start_month,
        |start_date.day as start_day,
        |end_date.year as end_year,
        |end_date.month as end_month,
        |end_date.day as end_day,
        |url
        | from xml
      """.stripMargin)

//    sqldf.write.insertInto("test_orcid.education_"+folder)
    sqldf.repartition(10).write.format("hive")
      .mode("append").saveAsTable("test_orcid.education_new_"+folder)

    println("=================ending================")

  }
}
