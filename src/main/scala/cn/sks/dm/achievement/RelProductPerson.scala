package cn.sks.dm.achievement

import org.apache.spark.sql.SparkSession

object RelProductPerson {
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
    .appName("RelProductPerson")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {

    println(8780265 + 220209 + 117291816 +498302 +49234471)
    //product_person
    val conference = spark.read.table("dwb.wb_product_conference_ms_nsfc_orcid").select("achievement_id")

    val criterion = spark.read.table("dwb.wb_product_criterion_csai_nsfc").select("achievement_id")

    val journal = spark.read.table("dwb.wb_product_journal_csai_nsfc_ms_orcid").select("achievement_id")

    val monograph = spark.read.table("dwb.wb_product_monograph_csai_nsfc_ms").select("achievement_id")

    val patent = spark.read.table("dwb.wb_product_patent_csai_nsfc_ms").select("achievement_id")

    val product_person = spark.read.table("dwb.wb_product_person_all_tmp")

    //8780265 220209 117291816 498302 49234471

//    product_person.join(conference, Seq("achievement_id")).createOrReplaceTempView("product_person")
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_product_person_conference
//        |select person_id
//        | ,achievement_id
//        | from product_person
//        |""".stripMargin)
//
//    product_person.join(criterion, Seq("achievement_id")).createOrReplaceTempView("product_person")
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_product_person_criterion
//        |select person_id
//        | ,achievement_id
//        | from product_person
//        |""".stripMargin)
//
//    product_person.join(journal, Seq("achievement_id")).createOrReplaceTempView("product_person")
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_product_person_journal
//        |select person_id
//        | ,achievement_id
//        | from product_person
//        |""".stripMargin)
//
//    product_person.join(monograph, Seq("achievement_id")).createOrReplaceTempView("product_person")
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_product_person_monograph
//        |select person_id
//        | ,achievement_id
//        | from product_person
//        |""".stripMargin)
//
//    product_person.join(patent, Seq("achievement_id")).createOrReplaceTempView("product_person")
//    spark.sql(
//      """
//        |insert overwrite table dm.dm_neo4j_product_person_patent
//        |select person_id
//        | ,achievement_id
//        | from product_person
//        |""".stripMargin)

    product_person.join(conference, Seq("achievement_id")).createOrReplaceTempView("product_person")
    spark.sql(
      """
        |select count(*) from product_person
        |""".stripMargin).show()

    product_person.join(criterion, Seq("achievement_id")).createOrReplaceTempView("product_person")
    spark.sql(
      """
        |select count(*) from product_person
        |""".stripMargin).show()

    product_person.join(journal, Seq("achievement_id")).createOrReplaceTempView("product_person")
    spark.sql(
      """
        |select count(*) from product_person
        |""".stripMargin).show()

    product_person.join(monograph, Seq("achievement_id")).createOrReplaceTempView("product_person")
    spark.sql(
      """
        |select count(*) from product_person
        |""".stripMargin).show()

    product_person.join(patent, Seq("achievement_id")).createOrReplaceTempView("product_person")
    spark.sql(
      """
        |select count(*) from product_person
        |""".stripMargin).show()


  }
}
