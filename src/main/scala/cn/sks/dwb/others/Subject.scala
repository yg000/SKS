package cn.sks.dwb.others

import org.apache.spark.sql.SparkSession

object Subject {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .master("local[12]")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    //抽取科协学科数据到
    //    val path = "D:\\worksource\\我的工作资料\\sks项目\\数据源\\subject.csv"
    //    spark.read.format("csv").option("header", true).load(path)
    //      .dropDuplicates("two_rank_name")
    //      .select("one_rank_id", "one_rank_no", "one_rank_name", "two_rank_id", "two_rank_no", "two_rank_name")
    //      .repartition(1).createOrReplaceTempView("subject")
    //
    //    spark.sql("insert overwrite table ods.o_csai_subject_constant  select * from subject")

    //将手动的采集的数据映射到科协的学科数据上边
//    val path_2 = "D:\\worksource\\我的工作资料\\sks项目\\数据源\\subject_code.csv"
//    val nsfc_subject_two = spark.read.format("csv").option("header", true).load(path_2).drop("负责人", "two_rank_name")
//      .where("length(two_rank_no)=5")
//
//    val nsfc_subject_one = spark.read.format("csv").option("header", true).load(path_2).drop("负责人", "two_rank_name")
//      .where("length(two_rank_no)=3").withColumnRenamed("two_rank_no", "one_rank_no")
//
//    val csai_subject_2 = spark.sql("select * from ods.o_csai_subject_constant")
//
//    val subject_2 = nsfc_subject_two.join(csai_subject_2, Seq("two_rank_no"))
//      .select("subject_code1", "subject_name_1", "subject_code2", "subject_name_2", "one_rank_id", "one_rank_no", "one_rank_name", "two_rank_id", "two_rank_no", "two_rank_name")
//
//    val csai_subject_one = csai_subject_2.select("one_rank_id", "one_rank_no", "one_rank_name").dropDuplicates("one_rank_no")
//
//    nsfc_subject_one.join(csai_subject_one, Seq("one_rank_no")).createOrReplaceTempView("subject_one")
//
//    val subject_1 = spark.sql(
//      """
//        |select
//        |subject_code1
//        |,subject_name_1
//        |,subject_code2
//        |,subject_name_2
//        |,one_rank_id
//        |,one_rank_no
//        |,one_rank_name
//        |,null as two_rank_id
//        |,null as two_rank_no
//        |,null as two_rank_name
//        |from subject_one
//      """.stripMargin)
//    subject_2.union(subject_1).repartition(1).createOrReplaceTempView("subject_union")
    // 科协和基金委合并后得数据
   //  spark.sql("insert overwrite table dwd.wd_subject_nsfc_csai  select * from subject_union")


    val nsfc_project = spark.sql("select project_id,substr(subject_code1,0,5) as subject_code2  from ods.o_nsfc_project ")
    val subject_csai_nsfc = spark.sql("select * from dwd.wd_subject_nsfc_csai")

    nsfc_project.join(subject_csai_nsfc, Seq("subject_code2"))
      .select("project_id", "subject_code1", "subject_name_1", "subject_code2", "subject_name_2", "one_rank_id", "one_rank_no", "one_rank_name",
        "two_rank_id", "two_rank_no", "two_rank_name").repartition(5).createOrReplaceTempView("project_subject")

     spark.sql("insert overwrite table dwd.wd_project_subject_nsfc_csai  select * from project_subject")

    val nsfc_product_project = spark.sql("select achievement_id,project_id  from ods.o_nsfc_project_product")
    val project_subject_nsfc_csai = spark.sql("select * from dwd.wd_project_subject_nsfc_csai")

    project_subject_nsfc_csai.join(nsfc_product_project, Seq("project_id"))
      .select("project_id", "achievement_id", "subject_code1", "subject_name_1", "subject_code2", "subject_name_2", "one_rank_id", "one_rank_no", "one_rank_name",
        "two_rank_id", "two_rank_no", "two_rank_name").repartition(5).createOrReplaceTempView("product_subject")

    spark.sql("insert overwrite  table dwd.wd_product_subject_nsfc_csai   select * from product_subject")


  }
}
