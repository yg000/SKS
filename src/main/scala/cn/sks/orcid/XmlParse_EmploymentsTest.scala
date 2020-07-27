package cn.sks.orcid

import org.apache.spark.sql.SparkSession

/*
  summary,(基本信息locale为en和zh)
  educations,(教育经历)
  employments,(工作经历)
  works(成果)  cn.sks.orcid.XmlParse_Educations
*/
object XmlParse_EmploymentsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("XmlParse_Employments")
      .config("spark.driver.memory", "12g")
      .config("spark.executor.memory", "48g")
      .config("spark.cores.max", "32")
      .config("spark.rpc.askTimeout","800")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

//    val xmlpath=args(0)
//    val rowTag=args(1)

//    val rowTag="work:work"
//    val xmlpath="/data/test.xml"
//
//
//    val xmlDF = spark.read.format("com.databricks.spark.xml")
//      .option("rowTag",rowTag)
//      .option("treatEmptyValuesAsNulls",true)
//      .load(xmlpath)
//        .select("common:url","common:external-ids")
//
//
//
////    xmlDF.show(10)
//    xmlDF.printSchema()
val rowTag="record:record"

//    xmlDF.createOrReplaceTempView("xml")
val xmlpath="/data/ORCID/summaries/400/0000-0002-1420-6400.xml"

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      .load(xmlpath)
      .selectExpr("`person:person`","`preferences:preferences`")

    xmlDF.createOrReplaceTempView("xml")
    val sql=spark.sql(
      """
        |select
        |`person:person`.`_path` as path,
        |`person:person`.`address:addresses`.`_path` as addresses_path,
        |`person:person`.`address:addresses`.`address:address`.`_display-index` as address_display_index,
        |`person:person`.`address:addresses`.`address:address`.`_path` as address_path,
        |`person:person`.`address:addresses`.`address:address`.`_put-code` as address_put_code,
        |`person:person`.`address:addresses`.`address:address`.`_visibility` as address_visibility,
        |`person:person`.`address:addresses`.`address:address`.`address:country` as address_country,
        |`person:person`.`address:addresses`.`address:address`.`common:created-date` as address_created_date,
        |`person:person`.`address:addresses`.`address:address`.`common:last-modified-date` as addresses_last_modified_date,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-name` as address_source_name,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-orcid`.`common:host` as address_source_orcid_host,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-orcid`.`common:path` as address_source_orcid_path,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-orcid`.`common:uri` as address_source_orcid_uri,
        |`person:person`.`address:addresses`.`common:last-modified-date` as address_last_modified_date,
        |`person:person`.`common:last-modified-date` as person_last_modified_date,
        |`person:person`.`email:emails`.`_VALUE` as emails_value,
        |`person:person`.`email:emails`.`_path` as emails_path,
        |`person:person`.`external-identifier:external-identifiers`.`_VALUE` as ext_identifiers_value,
        |`person:person`.`external-identifier:external-identifiers`.`_path` as ext_identifiers_path,
        |`person:person`.`keyword:keywords`.`_VALUE` as keywords_value,
        |`person:person`.`keyword:keywords`.`_path` as keywords_path,
        |`person:person`.`other-name:other-names`.`_VALUE` as other_names_value,
        |`person:person`.`other-name:other-names`.`_path` as other_names_path,
        |`person:person`.`person:name`.`_path` as per_name_path,
        |`person:person`.`person:name`.`_visibility` as per_name_visibility,
        |`person:person`.`person:name`.`common:created-date` as per_name_created_date,
        |`person:person`.`person:name`.`common:last-modified-date` as per_name_last_modified_date,
        |`person:person`.`person:name`.`personal-details:family-name` as per_name_family_name,
        |`person:person`.`person:name`.`personal-details:given-names` as per_name_given_names,
        |`person:person`.`researcher-url:researcher-urls`.`_path` as res_urls_path,
        |`person:person`.`researcher-url:researcher-urls`.`common:last-modified-date` as res_urls_last_modified_date,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`_display-index` as res_url_display_index,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`_path` as res_url_path,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`_put-code` as res_url_put_code,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`_visibility` as res_url_visibility,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:created-date` as res_url_created_date,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:last-modified-date` as res_url_last_modified_date,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-orcid`.`common:host` as res_url_source_orcid_host,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-orcid`.`common:path` as res_url_source_orcid_path,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-orcid`.`common:uri` as res_url_source_orcid_uri,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-name` as res_url_source_name,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`researcher-url:url` as res_url,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`researcher-url:url-name` as res_url_name,
        |`preferences:preferences`.`preferences:locale` as preferences_locale
        | from xml
      """.stripMargin)


    sql.write.format("hive")
      .mode("append").saveAsTable("test_orcid.summary_4_bak")


    //    xmlDF.foreach(row=>{
//      println(row.getAs("start-date"))
//      println(row.getStruct(37).getList(0))
//    })
//    xmlDF.createOrReplaceTempView("xml")
//    spark.sql(
//      """
//        |select
//        |source.source_orcid.path as source_orcid_path,
//        |source.source_orcid.host as source_orcid_host,
//        |source.source_orcid.uri as source_orcid_uri,
//        |created_date,
//        |department_name,
//        |last_modified_date,
//        |organization.address.city              as org_city,
//        |organization.address.region            as org_region,
//        |organization.address.country           as org_country,
//        |organization.disambiguated_organization.disambiguated_organization_identifier as dis_org_identifier,
//        |organization.disambiguated_organization.disambiguation_source                 as dis_source,
//        |organization.name                      as org_name,
//        |role_title,
//        |source.source_name                     as source_name,
//        |source.assertion_origin_client_id.host as or_client_id_host,
//        |source.assertion_origin_client_id.path as or_client_id_path,
//        |source.assertion_origin_client_id.uri  as or_client_id_uri,
//        |source.assertion_origin_name           as assertion_origin_name,
//        |source.source_client_id.host           as source_client_id_host,
//        |source.source_client_id.path           as source_client_id_path,
//        |source.source_client_id.uri            as source_client_id_uri,
//        |start_date.year                        as start_year,
//        |start_date.month                       as start_month,
//        |start_date.day                         as start_day,
//        |end_date.year                          as end_year,
//        |end_date.month                         as end_month,
//        |end_date.day                           as end_day,
//        |url
//        | from xml
//      """.stripMargin).write.insertInto("test_orcid.employment_0")

    println("=================ending================")

  }
}
