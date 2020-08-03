package cn.sks.orcid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

/*
  summary,(基本信息locale为en和zh)
  educations,(教育经历)
  employments,(工作经历)
  works(成果)
*/
object XmlParse_WorksTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("XmlParse_WorksTest")
      .config("spark.driver.memory", "24g")
      .config("spark.executor.memory", "48g")
      .config("spark.cores.max", "32")
      .config("spark.rpc.askTimeout","300")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields","100")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val folder="1"
//    val xmlpath="/data/ORCIDTEST/"+folder+"/*/*/works/*.xml"
    val rowTag="work:work"
    val xmlpath="/data/ORCID/"+folder+"/481/0000-0002-3497-0481/works/*.xml"

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      .load(xmlpath)
      .withColumn("common:external-id",explode($"`common:external-ids`.`common:external-id`"))
      .withColumn("work:contributor",explode($"`work:contributors`.`work:contributor`"))

      .selectExpr("_path",
        "`common:source`",
//      "`common:country`",
      "`common:created-date`",
//      "`common:language-code`",
      "`common:last-modified-date`",
      "`common:publication-date`",
      "`common:url`",
      "`work:citation`",
//      "`work:contributors`",
      "`work:journal-title`",
//       "`work:contributors`.`work:contributor`.`common:contributor-orcid`.`common:host` as contributor_orcid_host",
//       "`work:contributors`.`work:contributor`.`common:contributor-orcid`.`common:path` as contributor_orcid_path",
//       "`work:contributors`.`work:contributor`.`common:contributor-orcid`.`common:uri` as contributor_orcid_uri",
       "`work:contributor`.`work:contributor-attributes`.`work:contributor-role` as contributor_role",
       "`work:contributor`.`work:contributor-attributes`.`work:contributor-sequence` as contributor_sequence",
        "`common:external-id`.`common:external-id-relationship` as  ext_id_relationship" ,
        "`common:external-id`.`common:external-id-type` as ext_id_type",
        "`common:external-id`.`common:external-id-url` as ext_id_url",
        "`common:external-id`.`common:external-id-value` as ext_id_value",
//        "`_path`",
//      "`work:short-description`",
      "`work:title`",
      "`work:type`")

    xmlDF.printSchema()

    xmlDF.createOrReplaceTempView("xml")

//    val sqldf=spark.sql(
//      """
//        |select
//        |`common:source`.`common:source-orcid`.`common:path`  as source_orcid_path,
//        |`common:source`.`common:source-orcid`.`common:host`  as source_orcid_host,
//        |`common:source`.`common:source-orcid`.`common:uri` as source_orcid_uri,
//        |`common:country` as country,
//        |`common:created-date` as created_date,
//        |`common:external-ids`.`common:external-id` as ext_id_arr,
//        |`common:external-ids`.`common:external-id`.`common:external-id-normalized`.`_VALUE` as ext_nor_value,
//        |`common:external-ids`.`common:external-id`.`common:external-id-normalized`.`_transient` as ext_nor_transient,
//        |`common:external-ids`.`common:external-id`.`common:external-id-relationship` as ext_id_relationship,
//        |`common:external-ids`.`common:external-id`.`common:external-id-type` as ext_id_type,
//        |`common:external-ids`.`common:external-id`.`common:external-id-url` as ext_id_url,
//        |`common:external-ids`.`common:external-id`.`common:external-id-value` as ext_id_value,
//        |`common:language-code` as language_code,
//        |`common:last-modified-date` last_modified_date,
//        |`common:publication-date`.`common:year` as pub_year,
//        |`common:publication-date`.`common:month` as pub_month,
//        |`common:publication-date`.`common:day` as pub_day,
//        |`common:source`.`common:assertion-origin-name` as ass_origin_name,
//        |`common:source`.`common:assertion-origin-orcid`.`common:host` as ass_origin_orcid_host,
//        |`common:source`.`common:assertion-origin-orcid`.`common:path` as ass_origin_orcid_path,
//        |`common:source`.`common:assertion-origin-orcid`.`common:uri` as ass_origin_orcid_uri,
//        |`common:source`.`common:source-client-id`.`common:host` as source_client_id_host,
//        |`common:source`.`common:source-client-id`.`common:path` as source_client_id_path,
//        |`common:source`.`common:source-client-id`.`common:uri` as source_client_id_uri,
//        |`common:source`.`common:source-name` as source_name,
//        |`common:url` as url,
//        |`work:citation`.`work:citation-type` as citation_type,
//        |`work:citation`.`work:citation-value` as citation_value,
//        |`work:contributors`.`work:contributor` as contributor_arr,
//        |`work:contributors`.`work:contributor`.`common:contributor-orcid`.`common:host` as contributor_orcid_host,
//        |`work:contributors`.`work:contributor`.`common:contributor-orcid`.`common:path` as contributor_orcid_path,
//        |`work:contributors`.`work:contributor`.`common:contributor-orcid`.`common:uri` as contributor_orcid_uri,
//        |`work:contributors`.`work:contributor`.`work:contributor-attributes`.`work:contributor-role` as contributor_role,
//        |`work:contributors`.`work:contributor`.`work:contributor-attributes`.`work:contributor-sequence` as contributor_sequence,
//        |`work:contributors`.`work:contributor`.`work:credit-name` as credit_name,
//        |`work:journal-title` as journal_title,
//        |`work:short-description` as short_description,
//        |`work:title`.`common:title` as title,
//        |`work:title`.`common:subtitle` as  subtitle,
//        |`work:title`.`common:translated-title`._VALUE as translated_title_value,
//        |`work:title`.`common:translated-title`.`_language-code` as translated_title_language_code,
//        |`work:type` as type
//        | from xml
//      """.stripMargin).show()
    val sqldf=spark.sql(
      """
        |select
        |split(_path,"/")[1] as summary_id,
        |concat_ws("_",split(_path,"/")[1],split(_path,"/")[3]) as works_id,
        |`common:created-date` as created_date,
        |ext_id_relationship,
        |ext_id_type,
        |ext_id_url,
        |ext_id_value,
        |`common:last-modified-date` last_modified_date,
        |`common:publication-date`.`common:year` as pub_year,
        |`common:publication-date`.`common:month` as pub_month,
        |`common:source`.`common:source-name` as source_name,
        |`common:url` as url,
        |`work:citation`.`work:citation-type` as citation_type,
        |`work:citation`.`work:citation-value` as citation_value,
        |`work:journal-title` as journal_title,
        |`work:title`.`common:title` as title,
        |`work:type` as type,
        |contributor_role,
        |contributor_sequence
        | from xml
      """.stripMargin)

    sqldf.write.format("hive").mode("overwrite").insertInto("test_orcid.works_ext_contri")

    println("=====================ending=====================")

  }
}
