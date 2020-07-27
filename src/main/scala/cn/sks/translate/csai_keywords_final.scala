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

    val zh_keywords_50=spark.sql(
      """
        |select zh_keyword from nsfc.o_csai_keyword_translate where size(split(en_keywords,';')) =50
      """.stripMargin)


    val zh_keywords_50_2=spark.sql(
      """
        |select zh_keyword from nsfc.o_csai_keyword_translate_2 where size(split(en_keywords,';')) =50
      """.stripMargin)


    val zh_keywords_20=spark.sql(
      """
        |select zh_keyword from nsfc.o_csai_keyword_translate_3 where size(split(en_keywords,';')) =20
      """.stripMargin)


    val zh_keywords_10=spark.sql(
      """
        |select zh_keyword from nsfc.o_csai_keyword_translate_4 where size(split(en_keywords,';')) =10
      """.stripMargin)

    zh_keywords_50.union(zh_keywords_50_2).union(zh_keywords_20).union(zh_keywords_10).createOrReplaceTempView("zh_keywords")

    spark.sql("select count(*) from (select explode(split(zh_keyword,';')) as zh_keyword from zh_keywords)a").show()

    spark.sql("select keywords as zh_keyword from nsfc.o_csai_keywords_concat_01 union all select explode(split(zh_keyword,';')) as zh_keyword from zh_keywords").createOrReplaceTempView("zh_keywords_all")



//    spark.sql("insert into table nsfc.o_csai_keywords_non_translate select md5(zh_keyword) as rank,zh_keyword from zh_keywords_all")

  }
}
