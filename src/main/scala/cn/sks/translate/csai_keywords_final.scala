package cn.sks.translate

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object csai_keywords_final {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("product_keywords_nsfc_clean_translate")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "16g")
      .config("spark.cores.max", "8")
      .config("spark.rpc.askTimeout", "300")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields", "200")
      .config("spark.driver.maxResultSize", "4G")
      .config("sethive.enforce.bucketing", "true")
      .enableHiveSupport()
      .getOrCreate()


    import spark.implicits._

//    val zh_keywords_50=spark.sql(
//      """
//        |select * from nsfc.o_csai_keyword_translate where size(split(en_keywords,';')) =50
//      """.stripMargin)
//
//
//    val zh_keywords_50_2=spark.sql(
//      """
//        |select * from nsfc.o_csai_keyword_translate_2 where  size(split(en_keywords,';')) =50
//      """.stripMargin)
//
//
//    val zh_keywords_20=spark.sql(
//      """
//        |select * from nsfc.o_csai_keyword_translate_3 where  size(split(en_keywords,';')) =20
//      """.stripMargin)
//
//
//    val zh_keywords_10=spark.sql(
//      """
//        |select * from nsfc.o_csai_keyword_translate_4 where  size(split(en_keywords,';')) =50
//      """.stripMargin)
//
//    zh_keywords_50.union(zh_keywords_50_2).union(zh_keywords_20).union(zh_keywords_10).createOrReplaceTempView("zh_keywords")

//    spark.sql("select count(*) from (select explode(split(zh_keyword,';')) from zh_keywords)a").show()
//    spark.sql("insert overwrite table nsfc.o_csai_keyword_translate_8 select * from zh_keywords")
//    spark.sql(
//      """
//        |select * from nsfc.o_csai_keyword_translate_0726
//      """.stripMargin).createOrReplaceTempView("o_csai_keyword_translate_9_split")
//
//    spark.sql("select * from nsfc.o_csai_keywords_split_1 where en_keywords is null").createOrReplaceTempView("o_csai_keyword_split_1")
//
//    spark.sql(
//      """
//        |insert overwrite table nsfc.o_csai_keywords_final
//        |select md5(a.zh_keywords) as rank,a.zh_keywords from o_csai_keyword_split_1 a where not exists
//        |(select * from o_csai_keyword_translate_9_split b where a.zh_keywords =b.zh_keywords)
//      """.stripMargin)

    spark.sql(
      """
        |select * from nsfc.o_csai_keyword_translate_final union
        |select * from nsfc.o_csai_keyword_translate_0726
      """.stripMargin).createOrReplaceTempView("keywords_all")
//    spark.sql("select count(*) from keywords_all").show()

    val keywords_all=spark.sql(
      """
        |select a.keywords_id,a.zh_keywords,if(a.en_keywords is null,b.en_keywords,a.en_keywords),'csai' as source
        | from nsfc.o_csai_keywords_split_1 a left join keywords_all b
        |  on a.zh_keywords=b.zh_keywords
      """.stripMargin)

    keywords_all.repartition(20).write.format("hive").mode("overwrite")
      .insertInto("ods.o_csai_keyword_translate")
//    spark.sql("select keywords as zh_keyword from nsfc.o_csai_keywords_concat_01 union all select explode(split(zh_keyword,';')) as zh_keyword from zh_keywords").createOrReplaceTempView("zh_keywords_all")



//    spark.sql("insert into table nsfc.o_csai_keywords_non_translate select md5(zh_keyword) as rank,zh_keyword from zh_keywords_all")

  }
}
