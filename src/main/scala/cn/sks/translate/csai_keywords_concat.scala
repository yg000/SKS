package cn.sks.translate

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object csai_keywords_concat {
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

//    val rdd=spark.sql(
//      """
//        |select zh_keywords,row_number() over (order by keywords_id ) rank from nsfc.csai_keywords_split_1 where en_keywords is null
//      """.stripMargin).rdd

    spark.sql(
      """
        |select keyword_id,explode(split(zh_keyword,';')) as zh_keywords from nsfc.o_csai_keyword_translate_6
        | where size(split(en_keywords,';')) <10 or size(split(en_keywords,';'))>10
      """.stripMargin).createOrReplaceTempView("zh_keywords_20_split")

    spark.sql("select count(*) from zh_keywords_20_split").show()  //76385
//    while(true){}

    spark.sql(
      """
        |select zh_keywords,md5(zh_keywords) as rank from zh_keywords_20_split
          """.stripMargin).createOrReplaceTempView("zh_keywords_20_split_rank")

    spark.sql("insert overwrite table nsfc.o_csai_keywords_concat_01 select rank,zh_keywords from zh_keywords_20_split_rank")
    println("==========================")
    while(true){}

    val rdd=spark.sql(
      """
        |select zh_keywords,row_number() over (order by rank) rank from zh_keywords_20_split_rank
      """.stripMargin).rdd

//    while(true){}

    val value: RDD[String] = rdd.map((s => {

      val key = s.getAs[String]("zh_keywords").replace(";", "")
      val rank=s.getAs[Int]("rank")

      key + ";" + rank / 10
    }))


    value.toDF("key").createOrReplaceTempView("o_csai_split_10")

    spark.udf.register("clean",(str:String)=>{
      str.replaceAll("(.*)\\;","$1")
    })

    spark.sql(
      """
        |select
        |split(key,";")[0] as keywords ,
        |split(key,";")[1] as rank from o_csai_split_10
      """.stripMargin).createOrReplaceTempView("keywords_rank")

    spark.sql(
      """
        |select rank,concat_ws(';',collect_list(keywords)) as keywords
        | from keywords_rank group by rank
      """.stripMargin).createOrReplaceTempView("keywords_collect")


   spark.sql("insert overwrite table nsfc.o_csai_keywords_concat_50_10 select md5(rank) as rank,keywords from  keywords_collect")

    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
  }
}
