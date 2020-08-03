package cn.sks.keywords

import java.util.regex.Pattern

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object nsfc_csai_keywords_fuse {
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


    spark.sqlContext.udf.register("CleanAllString", (str: String) => {
      if (str == null) null
      else {
        val str1 = str.replaceAll("<strong>", "").replace("</strong>", "")
          .replaceAll("<b>", "").replaceAll("</b>", "")
          .replaceAll("#", "").replaceAll(";", "")
          .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\\*", "")
          .replaceAll(" ", "").replaceAll(",", "")
          .replaceAll("，", "").replaceAll("；", "")
          .replaceAll("-", "").replaceAll("\\.", "")
          .replaceAll("\t", "").replaceAll("\\s", "")
          .toLowerCase
        str1
      }
    })


//    spark.sql("select * from o_product_keywords_nsfc_split where keywords != 'null' ")
//      .createOrReplaceTempView("o_product_keywords_nsfc_split")
//
//    spark.sql("select * from o_project_person_keywords_nsfc_split where keywords !='null' ")
//        .createOrReplaceTempView("o_project_person_keywords_nsfc_split")
//    spark.sqlContext.udf.register("CleanKeywords", (str: String) => {DefineUDF.clean_fusion(str)})

    spark.sql(
      """
        |select
        |zh_keywords
        |from nsfc.o_product_keywords_nsfc_split where zh_keywords != 'null'
        |union all
        |select
        |zh_keywords
        |from nsfc.o_project_person_keywords_nsfc_split where zh_keywords !='null'
        |""".stripMargin).createOrReplaceTempView("nsfc_keywords_all")

//    spark.sql("select count(*) as nsfc_csai_count from nsfc_keywords_all").show()

    spark.sql(
      """
        |select
        |distinct(zh_keywords) as zh_keywords from nsfc_keywords_all
        |""".stripMargin).createOrReplaceTempView("nsfc_keywords_distinct")

     spark.sql("select count(*) as nsfc_all from  nsfc_keywords_all").show()       //25368452
     spark.sql("select count(*) as distinct from  nsfc_keywords_distinct").show()  //3365787


//    spark.sql("select zh_keywords,CleanKeywords(zh_keywords) as clean_keywords from nsfc_keywords_distinct")
//      .createOrReplaceTempView("nsfc_keywords_distinct_clean")
//
    spark.sql("select keyword as zh_keywords from ods.o_csai_keyword")
      .createOrReplaceTempView("csai_keywords")
//
//    spark.sql("select count(*) from (select distinct(clean_keywords) from csai_keywords_clean)a").show()

//    while(true){}

    spark.sql(
      """
        |select zh_keywords ,md5(zh_keywords) as keywords_id from nsfc_keywords_distinct b
        | where not exists(select * from csai_keywords a where a.zh_keywords=b.zh_keywords)
        |""".stripMargin).createOrReplaceTempView("nsfc_keywords_self")

    spark.sql(
      """
        |select zh_keywords,'nsfc_csai' as source from nsfc_keywords_distinct b
        | where exists(select * from csai_keywords a where a.zh_keywords=b.zh_keywords)
        |""".stripMargin).createOrReplaceTempView("nsfc_keywords_exists")

    spark.sql("select count(*) from nsfc_keywords_exists").show()


    spark.sql(
      """
        |select keywords_id,zh_keywords,'nsfc' as source from nsfc_keywords_self
        |union
        |select keyword_id as keywords_id,keyword as zh_keywords,source from ods.o_csai_keyword
        |""".stripMargin).createOrReplaceTempView("nsfc_csai_keywords_all")
//spark.sql("select count(*) as nsfc_csai_keywords_all from nsfc_csai_keywords_all").show()

//    spark.sql("select *,CleanKeywords(zh_keywords) as clean_keywords from nsfc_csai_keywords_all")
//      .createOrReplaceTempView("nsfc_csai_keywords_all_clean")

    val nsfc_csai=spark.sql(
      """
        |select a.keywords_id,a.zh_keywords,if(b.source is null,a.source,b.source) as source
        |from nsfc_csai_keywords_all a
        | left join nsfc_keywords_exists b
        |  on a.zh_keywords=b.zh_keywords
        |""".stripMargin)

    nsfc_csai.repartition(50).write.format("hive").mode("overwrite").insertInto("nsfc.nsfc_csai_keywords_all")

//    spark.sql("select count(*) from nsfc_csai_keywords").show()  14224380
//    spark.sql("select count(*) from ods.o_csai_keyword").show()  10858899

//    val keywords_distinct = spark.sql(
//      """
//        |select * from (
//        |select *, row_number() over (partition by zh_keywords order by keyword_id) rank from nsfc_csai_keywords
//        |)a where rank =1
//      """.stripMargin).drop("rank")
//    keywords_distinct.createOrReplaceTempView("nsfc_csai_keywords_distinct")
//
//    spark.sql(
//      """
//        |select keywords_id,zh_keywords,source from nsfc_csai_keywords group by
//        |""".stripMargin).createOrReplaceTempView("distinct_nsfc_csai_keywords")
//
//
//   spark.sql(
//      """
//        |select
//        |product_id,
//        |product_keywords_id,
//        |product_zh_keywords as nsfc_zh_keywords,
//        |project_person_id,
//        |type,
//        |project_person_keywords_id,
//        |CleanAllString(product_zh_keywords) as clean_keywords
//        |from nsfc.keywords_fuse
//      """.stripMargin).createOrReplaceTempView("nsfc_keywords")
//
//    spark.sql(
//      """
//        |select
//        |keywords_id,
//        |keywords,
//        |CleanAllString(keywords) as clean_keywords
//        |from ods.o_csai_keyword
//      """.stripMargin).createOrReplaceTempView("csai_keywords")
//
//    val nsfc_csai_fuse=spark.sql(
//      """
//        |select
//        |b.keywords_id                as csai_keywords_id,
//        |a.product_keywords_id        as nsfc_product_keywords_id,
//        |a.project_person_id          as nsfc_project_person_keywords_id,
//        |
//        |a.product_id,
//        |a.project_person_id,
//        |a.type,
//        |a.product_zh_keywords as zh_keywords,
//        |"nsfc_csai" as source
//        |from nsfc_keywords a join csai_keywords b on a.clean_keywords=b.clean_keywords
//        |group by
//        |         csai_keywords_id,
//        |         nsfc_product_keywords_id,
//        |         nsfc_project_person_keywords_id,
//        |         product_id,
//        |         project_person_id,
//        |         type,
//        |         zh_keywords,
//        |         source
//      """.stripMargin)
//
//    nsfc_csai_fuse.repartition(50).write.format("hive").insertInto("nsfc_csai_keywords_fuse")
//
//    val sql=spark.sql(
//      """
//        |select
//        |csai_keywords_id ,
//        |"null" as nsfc_product_keywords_id,
//        |"null" as project_person_keywords_id,
//        |"null" as product_id,
//        |"null" as project_person_id,
//        |"null" as type,
//        |zh_keywords,
//        |"csai" as source,
//        | from ods.o_csai_keyword a
//        |where not exists (select * from nsfc.nsfc_csai_keywords_fuse b where a.keywords_id=b.keywords_id)
//        |union select
//        |      csai_keywords_id ,
//        |      nsfc_product_keywords_id,
//        |      nsfc_project_person_keywords_id,
//        |      product_id,
//        |      project_person_id
//        |      type,
//        |      zh_keywords,
//        |      source
//        |                   from nsfc_csai_keywords_fuse
//      """.stripMargin)
//
//    sql.repartition(100).write.format("hive").insertInto("nsfc.nsfc_csai_keywords_all")



  }
}
