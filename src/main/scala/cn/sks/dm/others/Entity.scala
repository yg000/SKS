package cn.sks.dm.others

import org.apache.spark.sql.SparkSession

object Entity {
  val spark: SparkSession = SparkSession.builder()
    .master("local[15]")
    .config("spark.deploy.mode", "clent")
    .config("executor-memory", "12g")
    .config("executor-cores", "6")
    .config("spark.local.dir","/data/tmp")
    //      .config("spark.drivermemory", "32g")
    //      .config("spark.cores.max", "16")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","20")
    .appName("Entity")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {



//    //学会的主体表
//    spark.sql("select society_id as id,chinese_name,null as english_name,subject_category,address,governing_body,support_unit,unit_member,individual_member from ods.o_csai_society")
//      .repartition(1).createOrReplaceTempView("society")
//    spark.sql("insert overwrite table dm.dm_neo4j_society select * from society")
//
//
//    //领域常量表
//    spark.sql("select * from ods.o_csai_subject_constant").createOrReplaceTempView("subject")
//    spark.sql("insert overwrite table dm.dm_neo4j_subject select * from subject")

    //关键词的常量表
    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_keyword
        |select
        |keyword_id,
        |zh_keyword,
        |en_keyword
        |from dwb.wb_keyword
        |""".stripMargin)
//
//    //期刊常量表
//    val constant_journal = spark.sql("select journal_id,chinese_name,english_name,language,organizer,fullimpact,compleximpact,publish_region from ods.o_csai_journal")
//
//    constant_journal.select("journal_id", "organizer").createOrReplaceTempView("organizer")
//
//    spark.sql(
//      """
//        |select
//        |journal_id,
//        |get_json_object(org,'$.orgName')  as org_name
//        |from organizer
//        |lateral view explode(split(regexp_replace(regexp_replace(organizer, '\\[|\\]',''),'\\}\\,\\{','\\}\\;\\{'),'\\;')) t1 as org
//      """.stripMargin).createOrReplaceTempView("get_org_name")
//
//    spark.sql(
//      """
//        |select
//        |journal_id,
//        |concat(";",concat_ws(";",collect_set(org_name)),";") as org_names
//        |from get_org_name group by journal_id
//      """.stripMargin).createOrReplaceTempView("get_org_names")
//
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_journal
//        |select a.journal_id,
//        |chinese_name,
//        |english_name,
//        |language,
//        |org_names,
//        |fullimpact,
//        |compleximpact,
//        |publish_region
//        |from  ods.o_csai_journal a left join get_org_names b on a.journal_id = b.journal_id
//        |""".stripMargin)
//
//
//
//    //会议的常量表
//    val constant_conference = spark.sql("select conference_id,conference,organization as organizer from ods.o_const_conference")
//
//    constant_conference.select("conference_id", "organizer").createOrReplaceTempView("organizer")
//
//    spark.sql(
//      """
//        |select
//        |conference_id,
//        |get_json_object(org,'$.orgName')  as org_name
//        |from organizer
//        |lateral view explode(split(regexp_replace(regexp_replace(organizer, '\\[|\\]',''),'\\}\\,\\{','\\}\\;\\{'),'\\;')) t1 as org
//        |""".stripMargin).createOrReplaceTempView("get_org_name")
//
//    spark.sql(
//      """
//        |select
//        |conference_id,
//        |concat(";",concat_ws(";",collect_set(org_name)),";") as org_names
//        |from get_org_name group by conference_id
//      """.stripMargin).createOrReplaceTempView("get_org_names")
//
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_conference
//        |select a.conference_id,
//        |conference,
//        |org_names
//        |from  ods.o_const_conference a left join get_org_names b on a.conference_id = b.conference_id
//        |""".stripMargin)

  }
}
