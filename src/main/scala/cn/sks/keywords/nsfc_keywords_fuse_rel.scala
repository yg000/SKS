package cn.sks.keywords

import java.util.regex.Pattern

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object nsfc_keywords_fuse_rel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("keywords_fuse")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "16g")
      .config("spark.cores.max", "8")
      .config("spark.rpc.askTimeout","300")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields","200")
      .config("spark.driver.maxResultSize","4G")
      .config("sethive.enforce.bucketing","true")
      .enableHiveSupport()
      .getOrCreate()


    spark.sqlContext.udf.register("CleanKeywords", (str: String) => {DefineUDF.clean_fusion(str)})


//    spark.sql(
//      """
//        |select
//        |zh_keywords,en_keywords
//        |from nsfc.o_product_keywords_nsfc_split
//        |union
//        |select
//        |zh_keywords,en_keywords
//        |from nsfc.o_project_person_keywords_nsfc_split
//        |""".stripMargin).createOrReplaceTempView("keywords_all")
//
//    spark.sql(
//      """
//        |select zh_keywords,en_keywords from keywords_all group by zh_keywords,en_keywords
//        |""".stripMargin).createOrReplaceTempView("distinct_keywords")
//
//    val nsfc_keyword_all=spark.sql(
//      """
//        |select md5(zh_keywords) as keywords_id,zh_keywords,en_keywords  from distinct_keywords
//        |""".stripMargin)
//
////    nsfc_keyword_all.write.format("hive").insertInto("nsfc.nsfc_keywords_all")
//
//    spark.sql("select keywords_id,zh_keywords,CleanKeywords(zh_keywords) as clean_keywords from nsfc.nsfc_keywords_all")
//        .createOrReplaceTempView("clean_keywords_all")
//
//    //product_keywords_rel
//    spark.sql(
//      """
//        |select
//        |product_id ,
//        |keywords_id,
//        |zh_keywords,
//        |en_keywords,
//        |CleanKeywords(zh_keywords) as clean_product_zh_keywords
//        |from nsfc.o_product_keywords_nsfc_split where zh_keywords != "null"
//      """.stripMargin).createOrReplaceTempView("product_keywords_clean")

    spark.sql("select keywords_id,zh_keywords from dwb.wb_keywords_nsfc_csai_all")
      .createOrReplaceTempView("nsfc_csai_keywords_all")

    val product_keywords_rel=spark.sql(
      """
        |select
        |a.product_id as product_id,
        |b.keywords_id as b_keywords_id
        |from dwd.wd_product_keywords_split_nsfc a
        | left join nsfc_csai_keywords_all b
        |  on a.zh_keywords = b.zh_keywords
        |  group by product_id,b_keywords_id
        |""".stripMargin)

    product_keywords_rel.write.format("hive").mode("overwrite").insertInto("nsfc.o_product_keywords_rel")


    //project_person_keywords_rel

    val project_keywords_rel = spark.sql(
      """
        |select
        |md5(a.code) as project_id,
        |a.code as code,
        |b.keywords_id as b_keywords_id
        |from dwd.wd_project_person_keywords_split_nsfc a
        | left join nsfc_csai_keywords_all b
        |  on a.zh_keywords=b.zh_keywords
        |   where a.type='1' or a.type='2' group by project_id,code,b_keywords_id
      """.stripMargin).createOrReplaceTempView("project_keywords_rel")

    val project_keywords=spark.sql(
      """
        |select * from project_keywords_rel a
        |where exists (select * from ods.o_nsfc_project b where a.code=b.prj_code)
      """.stripMargin)
    project_keywords.write.format("hive").mode("overwrite").insertInto("dwb.wb_keywords_project_nsfc")

    val person_keywords_rel = spark.sql(
      """
        |select
        |md5(a.code) as person_id,
        |a.code as code,
        |b.keywords_id as b_keywords_id
        |from nsfc.o_project_person_keywords_nsfc_split a
        | left join nsfc_csai_keywords_all b
        |  on a.zh_keywords=b.zh_keywords
        |    where a.type='3' or a.type='4' group by person_id,code,b_keywords_id
      """.stripMargin)

    person_keywords_rel.createOrReplaceTempView("person_keywords_rel")

    val person_keywords=spark.sql(
      """
        |select * from person_keywords_rel a
        |where exists (select * from ods.o_nsfc_person b where a.code=b.psn_code)
      """.stripMargin)


    person_keywords.write.format("hive").mode("overwrite").insertInto("dwb.wb_keywords_person_nsfc")

  }
}
