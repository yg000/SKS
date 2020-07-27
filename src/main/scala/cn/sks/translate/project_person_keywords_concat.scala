package cn.sks.translate

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object project_person_keywords_concat {
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

    spark.sql("select zh_keywords,en_keywords from dwd.wd_project_person_keywords_split_nsfc where zh_keywords='null' " +
      "or en_keywords='null' group by zh_keywords,en_keywords").createOrReplaceTempView("sss")
    spark.sql("select count(*) from  sss").show()

    while(true){}

    spark.sql("select * from dwd.wd_project_person_keywords_split_nsfc where en_keywords ='null'")
      .createOrReplaceTempView("project_person_keywords_en_null")

    val rdd=spark.sql(
      """
        |select zh_keywords,row_number() over (order by keywords_id ) rank from nsfc.csai_keywords_split_1 where en_keywords is null
      """.stripMargin).rdd

//    while(true){}

    val value: RDD[String] = rdd.map(s => {

      val key = s.getAs[String]("zh_keywords").replace(";", "")
      val rank=s.getAs[Int]("rank")

      key + ";" + rank / 50
    })


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


   spark.sql("insert overwrite table nsfc.o_csai_keywords_concat_2 select md5(rank) as rank,keywords from  keywords_collect")

    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
  }
}
