package cn.sks.orcid

import org.apache.spark.sql.SparkSession

/*
  summary,(基本信息locale为en和zh)
  educations,(教育经历)
  employments,(工作经历)
  works(成果)  cn.sks.orcid.XmlParse_Educations
*/
object XmlParse_Employments {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("XmlParse_Employments")
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
    val rowTag="employment:employment"
    val xmlpath="/data/ORCID/"+folder+"/*/*/employments/*.xml"

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      .load(xmlpath)
//      .withColumnRenamed("department-name","department_name")
//      .withColumnRenamed("role-title","role_title")
//      .withColumnRenamed("start-date","start_date")
//      .withColumnRenamed("end-date","end_date")
//      .withColumnRenamed("created-date","created_date")
//      .withColumnRenamed("last-modified-date","last_modified_date")
//        .selectExpr("department_name",
//          "_path",
//          "created_date",
//          "last_modified_date",
//          "source",
//          "role_title",
//          "organization",
//          "start_date",
//          "end_date",
//          "url")
//    xmlDF.show(10)
    xmlDF.printSchema()
    println("=================start "+folder+"================")

    xmlDF.createOrReplaceTempView("xml")
//    val sqldf=spark.sql(
//      """
//        |select
//        |split(_path,"/")[1] as summary_id,
//        |concat_ws("_",split(_path,"/")[1],split(_path,"/")[3]) as employment_id,
//        |source.source_orcid.path as source_orcid_path,
//        |source.source_orcid.host as source_orcid_host,
//        |source.source_orcid.uri as source_orcid_uri,
//        |created_date,
//        |department_name,
//        |last_modified_date,
//        |organization.address.city              as org_city,
//        |organization.address.region            as org_region,
//        |organization.address.country           as org_country,
//        |organization.disambiguated_organization.disambiguated_organization_identifier as disambiguated_organization_identifier,
//        |organization.disambiguated_organization.disambiguation_source                 as disambiguation_source,
//        |organization.name                      as org_name,
//        |role_title,
//        |source.source_name                     as source_name,
//        |start_date.year                        as start_year,
//        |start_date.month                       as start_month,
//        |start_date.day                         as start_day,
//        |end_date.year                          as end_year,
//        |end_date.month                         as end_month,
//        |end_date.day                           as end_day,
//        |url
//        | from xml
//      """.stripMargin)
     val sqldf=spark.sql(
       """
         |select
         |split(_path,"/")[1] as summary_id,
         |concat_ws("_",split(_path,"/")[1],split(_path,"/")[3]) as employment_id,
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

    sqldf.repartition(10).write.format("hive")
      .mode("append").saveAsTable("test_orcid.employment_new_x")

//    source.assertion_origin_client_id.host as or_client_id_host,
//    source.assertion_origin_client_id.path as or_client_id_path,
//    source.assertion_origin_client_id.uri  as or_client_id_uri,
//    source.assertion_origin_name           as assertion_origin_name,

    println("=================ending================")

  }
}
