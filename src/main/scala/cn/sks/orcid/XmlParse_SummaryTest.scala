package cn.sks.orcid

import org.apache.spark.sql.SparkSession

/*
  summary`,(基本信息locale为en和zh)
  educations`,(教育经历)
  employments`,(工作经历)
  works(成果)
*/
object XmlParse_SummaryTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("XmlParse_SummaryTest")
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

    val rowTag="record:record"
//    val xmlpath="/data/ORCIDTEST/0/000/0000-0001-5881-6000/employments/0000-0001-5881-6000_employments_807958`.`xml"

    val xmlpath="/data/ORCIDTEST/summary/*/*.xml"

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      .load(xmlpath)
        .selectExpr("`person:person`","`preferences:preferences`")

    xmlDF.printSchema()

    xmlDF.createOrReplaceTempView("xml")

    val sql11=spark.sql(
      """
        |select
        |`person:person`.`_path` as path,
        |`person:person`.`address:addresses`.`_VALUE` as addresses_value,
        |`person:person`.`address:addresses`.`_path` as addresses_path,
        |`person:person`.`address:addresses`.`address:address` as address_arr,
        |`person:person`.`address:addresses`.`address:address`.`_display-index` as address_display_index,
        |`person:person`.`address:addresses`.`address:address`.`_path` as address_path,
        |`person:person`.`address:addresses`.`address:address`.`_put-code` as address_put_code,
        |`person:person`.`address:addresses`.`address:address`.`_visibility` as address_visibility,
        |`person:person`.`address:addresses`.`address:address`.`address:country` as address_country,
        |`person:person`.`address:addresses`.`address:address`.`common:created-date` as address_created_date,
        |`person:person`.`address:addresses`.`address:address`.`common:last-modified-date` as addresses_last_modified_date,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-client-id`.`common:host` as address_source_client_id_host,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-client-id`.`common:path` as address_source_client_id_path,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-client-id`.`common:uri` as address_source_client_id_uri,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-name` as address_source_name,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-orcid`.`common:host` as address_source_orcid_host,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-orcid`.`common:path` as address_source_orcid_path,
        |`person:person`.`address:addresses`.`address:address`.`common:source`.`common:source-orcid`.`common:uri` as address_source_orcid_uri,
        |`person:person`.`address:addresses`.`common:last-modified-date` as address_last_modified_date,
        |`person:person`.`common:last-modified-date` as person_last_modified_date,
        |`person:person`.`email:emails`.`_VALUE` as emails_value,
        |`person:person`.`email:emails`.`_path` as emails_path,
        |`person:person`.`email:emails`.`common:last-modified-date` as emails_last_modified_date,
        |`person:person`.`email:emails`.`email:email` as email_arr,
        |`person:person`.`email:emails`.`email:email`.`_primary` as email_primary,
        |`person:person`.`email:emails`.`email:email`.`_verified` as email_verified,
        |`person:person`.`email:emails`.`email:email`.`_visibility` as email_visibility,
        |`person:person`.`email:emails`.`email:email`.`common:created-date` as email_created_date,
        |`person:person`.`email:emails`.`email:email`.`common:last-modified-date` as email_last_modified_date,
        |`person:person`.`email:emails`.`email:email`.`common:source`.`common:source-client-id`.`common:host` as email_source_client_id_host,
        |`person:person`.`email:emails`.`email:email`.`common:source`.`common:source-client-id`.`common:path` as email_source_client_id_path,
        |`person:person`.`email:emails`.`email:email`.`common:source`.`common:source-client-id`.`common:uri` as email_source_client_id_uri,
        |`person:person`.`email:emails`.`email:email`.`common:source`.`common:source-name` as email_source_name,
        |`person:person`.`email:emails`.`email:email`.`common:source`.`common:source-orcid`.`common:host` as email_source_orcid_host,
        |`person:person`.`email:emails`.`email:email`.`common:source`.`common:source-orcid`.`common:path` as email_source_orcid_path,
        |`person:person`.`email:emails`.`email:email`.`common:source`.`common:source-orcid`.`common:uri` as email_source_orcid_uri,
        |`person:person`.`email:emails`.`email:email`.`email:email` as email,
        |`person:person`.`external-identifier:external-identifiers`.`_VALUE` as ext_identifiers_value,
        |`person:person`.`external-identifier:external-identifiers`.`_path` as ext_identifiers_path,
        |`person:person`.`external-identifier:external-identifiers`.`common:last-modified-date` as ext_identifier_last_modified_date,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier` as ext_identifier_arr,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`_display-index` as ext_identifier_display_index,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`_path` as ext_identifier_path,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`_put-code` as ext_identifier_put_code,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`_visibility` as ext_identifier_visibility,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:created-date` as ext_identifier_created_date,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:external-id-relationship` as ext_identifier_id_relationship,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:external-id-type` as ext_identifier_id_type,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:external-id-url` as ext_identifier_id_url,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:external-id-value` as ext_identifier_id_value,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:source`.`common:assertion-origin-name` as ext_identifier_ass_origin_name,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:source`.`common:assertion-origin-orcid`.`common:host` as ext_identifier_origin_orcid_host,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:source`.`common:assertion-origin-orcid`.`common:path` as ext_identifier_origin_orcid_path,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:source`.`common:assertion-origin-orcid`.`common:uri` as ext_identifier_origin_orcid_uri,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:source`.`common:source-client-id`.`common:host` as ext_identifier_source_client_id_host,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:source`.`common:source-client-id`.`common:path` as ext_identifier_source_client_id_path,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:source`.`common:source-client-id`.`common:uri` as ext_identifier_source_client_id_uri,
        |`person:person`.`external-identifier:external-identifiers`.`external-identifier:external-identifier`.`common:source`.`common:source-name` as ext_identifier_source_name,
        |`person:person`.`keyword:keywords`.`_VALUE` as keywords_value,
        |`person:person`.`keyword:keywords`.`_path` as keywords_path,
        |`person:person`.`keyword:keywords`.`common:last-modified-date` as keywords_last_modified_date,
        |`person:person`.`keyword:keywords`.`keyword:keyword` as keyword_arr,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`_display-index` as keyword_display_index,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`_path` as keyword_path,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`_put-code` as keyword_put_code,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`_visibility` as keyword_visibility,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:created-date` as keyword_created_date,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:last-modified-date` as keyword_last_modified_date,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:source`.`common:source-client-id`.`common:host` as keyword_source_client_id_host,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:source`.`common:source-client-id`.`common:path` as keyword_source_client_id_path,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:source`.`common:source-client-id`.`common:uri` as keyword_source_client_id_uri,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:source`.`common:source-orcid`.`common:host` as keyword_source_orcid_host,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:source`.`common:source-orcid`.`common:path` as keyword_source_orcid_path,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:source`.`common:source-orcid`.`common:uri` as keyword_source_orcid_uri,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`common:source`.`common:source-name` as keyword_source_name,
        |`person:person`.`keyword:keywords`.`keyword:keyword`.`keyword:content` as keyword,
        |`person:person`.`other-name:other-names`.`_VALUE` as other_names_value,
        |`person:person`.`other-name:other-names`.`_path` as other_names_path,
        |`person:person`.`other-name:other-names`.`common:last-modified-date` as other_names_last_modified_date,
        |`person:person`.`other-name:other-names`.`other-name:other-name` as other_name_arr,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`_display-index` as other_name_display_index,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`_path` as other_name_path,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`_put-code` as other_name_put_code,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`_visibility` as other_name_visibility,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:created-date` as other_name_created_date ,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:last-modified-date` as other_name_last_modified_date,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:source`.`common:source-client-id`.`common:host` as other_name_source_client_id_host,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:source`.`common:source-client-id`.`common:path` as other_name_source_client_id_path,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:source`.`common:source-client-id`.`common:uri` as other_name_source_client_id_uri,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:source`.`common:source-orcid`.`common:host` as other_name_source_orcid_host,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:source`.`common:source-orcid`.`common:path` as other_name_source_orcid_path,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:source`.`common:source-orcid`.`common:uri` as other_name_source_orcid_uri,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`common:source`.`common:source-name` as other_name_source_name,
        |`person:person`.`other-name:other-names`.`other-name:other-name`.`other-name:content` as other_name,
        |`person:person`.`person:name`.`_path` as per_name_path,
        |`person:person`.`person:name`.`_visibility` as per_name_visibility,
        |`person:person`.`person:name`.`common:created-date` as per_name_created_date,
        |`person:person`.`person:name`.`common:last-modified-date` as per_name_last_modified_date,
        |`person:person`.`person:name`.`personal-details:credit-name` as per_name_credit_name,
        |`person:person`.`person:name`.`personal-details:family-name` as per_name_family_name,
        |`person:person`.`person:name`.`personal-details:given-names` as per_name_given_names,
        |`person:person`.`researcher-url:researcher-urls`.`_VALUE` as res_urls_value,
        |`person:person`.`researcher-url:researcher-urls`.`_path` as res_urls_path,
        |`person:person`.`researcher-url:researcher-urls`.`common:last-modified-date` as res_urls_last_modified_date,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url` as res_url_arr,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`_display-index` as res_url_display_index,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`_path` as res_url_path,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`_put-code` as res_url_put_code,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`_visibility` as res_url_visibility,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:created-date` as res_url_created_date,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:last-modified-date` as res_url_last_modified_date,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-client-id`.`common:host` as res_url_source_client_id_host,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-client-id`.`common:path` as res_url_source_client_id_path,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-client-id`.`common:uri` as res_url_source_client_id_uri,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-orcid`.`common:host` as res_url_source_orcid_host,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-orcid`.`common:path` as res_url_source_orcid_path,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-orcid`.`common:uri` as res_url_source_orcid_uri,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`common:source`.`common:source-name` as res_url_source_name,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`researcher-url:url` as res_url,
        |`person:person`.`researcher-url:researcher-urls`.`researcher-url:researcher-url`.`researcher-url:url-name` as res_url_name,
        |`preferences:preferences`.`preferences:locale` as preferences_locale
        | from xml
      """.stripMargin)

    sql11.write.format("hive").mode("overwrite")
      .insertInto("test_orcid.summary_6_bak")

    println("=====================ending=====================")

  }
}
