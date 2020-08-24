package cn.sks.dwb.relation

import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.{AchievementUtil, DefineUDF, NameToPinyinUtil}

/**
 * 邻域和成果 和项目和成果得关系 人和成果
 */

object RelProduct {
  val spark: SparkSession = SparkSession.builder()
    .master("local[20]")
    .config("spark.deploy.mode", "clent")
    .config("executor-memory", "12g")
    .config("executor-cores", "6")
    .config("spark.local.dir", "/data/tmp")
    //      .config("spark.drivermemory", "32g")
    //      .config("spark.cores.max", "16")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions", "120")
    .appName("journal")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  def main(args: Array[String]): Unit = {

    //人和成果得关系   188205546 187682988 188205546
    val csai_person = spark.sql("select person_id,achievement_id,product_type from dwd.wd_product_person_ext_csai where product_type not in ('8','3')")
    val nsfc_person = spark.sql("select person_id,achievement_id ,product_type from dwd.wd_product_person_ext_nsfc")
    val orcid_person = spark.sql("select person_id,achievement_id ,product_type from dwd.wd_product_person_ext_orcid")

    val person_productf_all = csai_person.unionAll(nsfc_person).unionAll(orcid_person).dropDuplicates()
    AchievementUtil.tranferAchievementID(spark,person_productf_all,"achievement_id").createOrReplaceTempView("person_product")
//    spark.sql(
//      """
//        |create table dwb.wb_product_person_all_tmp like dwb.wb_product_all_person
//        |""".stripMargin)
    spark.sql(
      """
        |insert overwrite table dwb.wb_product_person_all_tmp
        |select
        |person_id,
        |new_achievement_id,
        |product_type
        | from person_product
        |""".stripMargin)

//    //邻域和成果
//
//    val nsfc_product_subject = spark.sql("select achievement_id,one_rank_id,one_rank_no,one_rank_name,two_rank_id,two_rank_no,two_rank_name from dwd.wd_product_subject_nsfc_csai")
//    val csai_product_journal = spark.sql("select achievement_id,one_rank_id,one_rank_no,one_rank_name,two_rank_id,two_rank_no,two_rank_name from ods.o_csai_product_journal_subject")
//    val csai_product_patent = spark.sql("select achievement_id,one_rank_id,one_rank_no,one_rank_name,two_rank_id,two_rank_no,two_rank_name from ods.o_csai_product_patent_subject")
//
//    val product_subject = nsfc_product_subject.union(csai_product_journal).union(csai_product_patent)
//    val one_rank_no = spark.sql("select one_rank_no,one_rank_no as tem from ods.o_csai_subject_constant")
//    val two_rank_no = spark.sql("select two_rank_no  as one_rank_no,two_rank_no as tem from ods.o_csai_subject_constant")
//
//    val rank_all = one_rank_no.union(two_rank_no).dropDuplicates()
//
//
//    val product_id = spark.sql("select achievement_id as id,original_achievement_id as achievement_id from dwb.wb_product_all_rel")
//    product_subject.join(product_id, Seq("achievement_id"), "left").createOrReplaceTempView("subject")
//
//    val product_subject_2 = spark.sql(
//      """
//        |select
//        |if(id is null,achievement_id,id) as achievement_id,
//        |one_rank_id,
//        |one_rank_no,
//        |one_rank_name,
//        |two_rank_id,
//        |two_rank_no,
//        |two_rank_name
//        |from subject
//      """.stripMargin)
//    product_subject_2.join(rank_all,Seq("one_rank_no")).drop("tem")
//      .join(rank_all.toDF("two_rank_no","tem"),Seq("two_rank_no"),"left")
//      .createOrReplaceTempView("subject_2")
//    spark.sql(
//      """
//        |select
//        |achievement_id,
//        |one_rank_id,
//        |one_rank_no,
//        |one_rank_name,
//        |if(tem is null,null,two_rank_id) two_rank_id,
//        |if(tem is null,null,two_rank_no) two_rank_no,
//        |if(tem is null,null,two_rank_name) two_rank_name
//        |from subject_2
//      """.stripMargin)
//      .dropDuplicates().repartition(50)
//      .createOrReplaceTempView("result")
//
//    //  spark.sql("insert overwrite table dwb.wb_product_all_subject select * from result")
//
    //项目和成果

    val product_rel_subject = spark.sql("select  project_id, achievement_id from ods.o_nsfc_project_product")
    AchievementUtil.tranferAchievementID(spark,product_rel_subject,"achievement_id").createOrReplaceTempView("product_subject")

    spark.sql(
      """
        |select
        |project_id,
        |new_achievement_id
        |from product_subject
      """.stripMargin).repartition(10).createOrReplaceTempView("product_subject")

    //spark.sql("insert overwrite table dwb.wb_product_project_rel select * from product_subject  ")

  }
}
