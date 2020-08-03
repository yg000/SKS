package cn.sks.dwb.relation

import org.apache.spark.sql.{Column, SparkSession}

/**
  * 邻域和成果 和项目和成果得关系 人和成果
  */
object ProductSubjectAndProjectAndPerson {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .master("local[12]")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    val nsfc_product_subject = spark.sql("select achievement_id,one_rank_id,one_rank_no,one_rank_name,two_rank_id,two_rank_no,two_rank_name from dwd.wd_product_subject_nsfc_csai")
    val csai_product_journal = spark.sql("select achievement_id,one_rank_id,one_rank_no,one_rank_name,two_rank_id,two_rank_no,two_rank_name from ods.o_csai_product_journal_subject")
    val csai_product_patent = spark.sql("select achievement_id,one_rank_id,one_rank_no,one_rank_name,two_rank_id,two_rank_no,two_rank_name from ods.o_csai_product_patent_subject")

    val product_subject = nsfc_product_subject.union(csai_product_journal).union(csai_product_patent)
    val one_rank_no = spark.sql("select one_rank_no,one_rank_no as tem from ods.o_csai_subject_constant")
    val two_rank_no = spark.sql("select two_rank_no  as one_rank_no,two_rank_no as tem from ods.o_csai_subject_constant")

    val rank_all = one_rank_no.union(two_rank_no).dropDuplicates()


    val product_id = spark.sql("select achievement_id as id,original_achievement_id as achievement_id from dwb.wb_product_all_rel")
    product_subject.join(product_id, Seq("achievement_id"), "left").createOrReplaceTempView("subject")

    val product_subject_2 = spark.sql(
      """
        |select
        |if(id is null,achievement_id,id) as achievement_id,
        |one_rank_id,
        |one_rank_no,
        |one_rank_name,
        |two_rank_id,
        |two_rank_no,
        |two_rank_name
        |from subject
      """.stripMargin)
    product_subject_2.join(rank_all,Seq("one_rank_no")).drop("tem")
      .join(rank_all.toDF("two_rank_no","tem"),Seq("two_rank_no"),"left")
      .createOrReplaceTempView("subject_2")
    spark.sql(
      """
        |select
        |achievement_id,
        |one_rank_id,
        |one_rank_no,
        |one_rank_name,
        |if(tem is null,null,two_rank_id) two_rank_id,
        |if(tem is null,null,two_rank_no) two_rank_no,
        |if(tem is null,null,two_rank_name) two_rank_name
        |from subject_2
      """.stripMargin)
      .dropDuplicates().repartition(50)
      .createOrReplaceTempView("result")

  //  spark.sql("insert overwrite table dwb.wb_product_all_subject select * from result")


    //项目和成果得关系

    val product_rel_subject = spark.sql("select  project_id, achievement_id from ods.o_nsfc_project_product")

    product_rel_subject.join(product_id, Seq("achievement_id"), "left")
      .createOrReplaceTempView("project")
    spark.sql(
      """
        |select
        |project_id,
        |if(id is null,achievement_id,id) as achievement_id
        |from project
      """.stripMargin).repartition(1).createOrReplaceTempView("project_rel")

    spark.sql("insert overwrite table dwb.wb_product_project_rel select * from project_rel  ")


    //人和成果得关系
    val csai_person = spark.sql("select person_id,achievement_id,product_type from dwd.wd_product_person_ext_csai where product_type!='8'")
    val nsfc_person = spark.sql("select person_id,achievement_id ,product_type from dwd.wd_product_person_ext_nsfc")
    val orcid_person = spark.sql("select person_id,achievement_id ,product_type from dwd.wd_product_person_ext_orcid")

    val person_productf_all = csai_person.union(nsfc_person).union(orcid_person)

    person_productf_all.join(product_id, Seq("achievement_id"), "left")
      .createOrReplaceTempView("person")


    val col: Column = new Column("original_achievement_id")
    spark.sql(
      """
        |select
        |person_id,
        |if(id is null,achievement_id,id) as achievement_id,
        |id as original_achievement_id,
        |product_type
        |from person
      """.stripMargin).filter(col.isNull)
      .select("person_id", "achievement_id", "product_type")
      .repartition(100)
      .createOrReplaceTempView("person_re")

  //  spark.sql("insert overwrite table dwb.wb_product_all_person select * from person_re")


  }
}
