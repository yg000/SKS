package cn.sks.dm.achievement

import org.apache.spark.sql.SparkSession

object RelProductOthers {
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
    .appName("RelProductOthers")
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

    //关键词对应的成果
    val product_keyword = spark.sql("select achievement_id,keyword_id from dwb.wb_product_all_keyword")

    journal.join(product_keyword, Seq("achievement_id"))
      .repartition(100).createOrReplaceTempView("journal_keyword")
    // spark.sql("insert overwrite table dm.dm_neo4j_product_keyword_journal select * from journal_keyword")

    conference.join(product_keyword, Seq("achievement_id"))
      .repartition(10).createOrReplaceTempView("conference_keyword")
    //spark.sql("insert overwrite table dm.dm_neo4j_product_keyword_conference select * from conference_keyword")





  }
}
