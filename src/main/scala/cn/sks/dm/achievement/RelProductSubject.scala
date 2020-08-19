package cn.sks.dm.achievement

import org.apache.spark.sql.SparkSession

object RelProductSubject {
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
    .appName("RelProductSubject")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {

    //成果对应的领域
    
    val conference = spark.read.table("dwb.wb_product_conference_ms_nsfc_orcid").select("achievement_id")

    val criterion = spark.read.table("dwb.wb_product_criterion_csai_nsfc").select("achievement_id")

    val journal = spark.read.table("dwb.wb_product_journal_csai_nsfc_ms_orcid").select("achievement_id")

    val monograph = spark.read.table("dwb.wb_product_monograph_csai_nsfc_ms").select("achievement_id")

    val patent = spark.read.table("dwb.wb_product_patent_csai_nsfc_ms").select("achievement_id")
    

    val product_subject_csai = spark.read.table("dwb.wb_product_all_subject")
    val project_subject = spark.sql("select prj_code,one_rank_id,one_rank_no,one_rank_name,two_rank_id,two_rank_no,two_rank_name from dwd.wd_product_subject_nsfc_csai")
    val wb_product_project_rel = spark.sql("select prj_code,achievement_id from dwb.wb_product_project_rel")

    val project_product_subject = wb_product_project_rel.join(project_subject, Seq("prj_code"))
      .select("achievement_id", "one_rank_id", "one_rank_no", "one_rank_name", "two_rank_id", "two_rank_no", "two_rank_name")

    val product_subject_all = product_subject_csai.union(project_product_subject)
      .dropDuplicates()

    journal.join(product_subject_all, Seq("achievement_id"))
      .select("achievement_id", "two_rank_id")
      .repartition(20).createOrReplaceTempView("journal_subject")
    spark.sql("insert overwrite table  dm.dm_neo4j_product_subject_journal select * from journal_subject")

    conference.join(product_subject_all, Seq("achievement_id"))
      .select("achievement_id", "two_rank_id")
      .repartition(5).createOrReplaceTempView("conference_subject")
    spark.sql("insert overwrite table dm.dm_neo4j_product_subject_conference select * from conference_subject")

    patent.join(product_subject_all, Seq("achievement_id"))
      .select("achievement_id", "two_rank_id")
      .repartition(20).createOrReplaceTempView("patent_subject")
    spark.sql("insert overwrite table dm.dm_neo4j_product_subject_patent select * from patent_subject")


    criterion.join(product_subject_all, Seq("achievement_id"))
      .select("achievement_id", "two_rank_id")
      .repartition(5).createOrReplaceTempView("criterion_subject")
    spark.sql("insert overwrite table dm.dm_neo4j_product_subject_criterion select * from criterion_subject")

    monograph.join(product_subject_all, Seq("achievement_id"))
      .select("achievement_id", "two_rank_id")
      .repartition(5).createOrReplaceTempView("monograph_subject")
    spark.sql("insert overwrite table dm.dm_neo4j_product_subject_monograph select * from monograph_subject")


    spark.stop()
  }
}
