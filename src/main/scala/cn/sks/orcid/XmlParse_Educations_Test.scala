package cn.sks.orcid

import java.io.File

import org.apache.spark.sql.SparkSession

/*
  summary,(基本信息locale为en和zh)
  educations,(教育经历)
  employments,(工作经历)
  works(成果)  cn.sks.orcid.XmlParse_Educations
*/
object XmlParse_Educations_Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("XmlParse_Educations_Test")
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

    val folder="X"
    val rowTag="education:education"
    val xmlpath="/data/ORCID/"+folder+"/*/*/educations/*.xml"

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      .load(xmlpath)
//        .selectExpr("`common:department-name`",
//          "`_path`",
//          "`common:created-date`",
//          "`common:last-modified-date`",
//          "`common:source`",
//          "`common:role-title`",
//          "`common:organization`",
//          "`common:start-date`",
//          "`common:end-date`",
//          "`common:url`")
//    xmlDF.show(10)
    xmlDF.printSchema()
    println("=================start================")

    xmlDF.createOrReplaceTempView("xml")
    val sqldf=spark.sql(
      """
        |select
        |split(_path,"/")[1] as summary_id,
        |concat_ws("_",split(_path,"/")[1],split(_path,"/")[3]) as education_id,
        |`common:source`.`common:source-orcid`.`common:path` as source_orcid_path,
        |`common:source`.`common:source-orcid`.`common:host` as source_orcid_host,
        |`common:source`.`common:source-orcid`.`common:uri` as source_orcid_uri,
        |`common:created-date` as created_date,
        |`common:department-name` as department_name,
        |`common:last-modified-date` as last_modified_date,
        |`common:organization`.`common:address`.`common:city` as org_city,
        |`common:organization`.`common:address`.`common:region` as org_region,
        |`common:organization`.`common:address`.`common:country` as org_country,
        |`common:organization`.`common:disambiguated-organization`.`common:disambiguated-organization-identifier` as disambiguated_organization_identifier,
        |`common:organization`.`common:disambiguated-organization`.`common:disambiguation-source` as disambiguation_source,
        |`common:organization`.`common:name` as org_name,
        |`common:role-title` as role_title,
        |`common:source`.`common:source-name` as source_name,
        |`common:start-date`.`common:year` as start_year,
        |`common:start-date`.`common:month` as start_month,
        |`common:start-date`.`common:day` as start_day,
        |`common:end-date`.`common:year` as end_year,
        |`common:end-date`.`common:month` as end_month,
        |`common:end-date`.`common:day` as end_day,
        |`common:url` as url
        | from xml
      """.stripMargin)

//    sqldf.write.insertInto("test_orcid.education_"+folder)
    sqldf.repartition(10).write.format("hive")
      .mode("append").saveAsTable("test_orcid.education_new_x")

    println("=================ending================")

  }
}
