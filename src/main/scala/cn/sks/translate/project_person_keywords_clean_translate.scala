package cn.sks.translate

import java.text.SimpleDateFormat
import java.util.Date

import cn.sks.BaiduTranslate.baidu.Baidu_Translate
import org.apache.spark.sql.SparkSession

object project_person_keywords_clean_translate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("project_person_keywords_split")
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

    //    spark.sqlContext.udf.register("TranslateToZh", (str: String) => DefinedUDF.translate(str, "zh"))
    //    spark.sqlContext.udf.register("TranslateToEn", (str: String) => DefinedUDF.translate(str, "en"))


    val sql = spark.sql(
      """
        |select
        |id,
        |code,
        |source,
        |type,
        |zh_research,
        |en_research
        | from nsfc.project_person_translate_zh_null limit 5035
      """.stripMargin)


    val rdd = sql.toDF().rdd

    rdd.map(s => {
      var row = ""
      val id: String = s.getAs[String]("id")
      val code: String = s.getAs[String]("code")
      val type1: String = s.getAs[String]("type")

      val zh_research: String = s.getAs[String]("zh_research")
      val en_research: String = s.getAs[String]("en_research")


      var zh_research_tran = ""
      val translate = "translate"
      try {
        if (en_research != "") {
          Thread.sleep(1200)

          zh_research_tran = Baidu_Translate.translate(en_research, "zh")
        }
      } catch {
        case e: InterruptedException =>
      }

      (id, code, type1, zh_research_tran, en_research, translate)
    }).toDF("id", "code", "type1", "zh_research_tran", "en_research", "translate")
      .repartition(1).write.format("hive").mode("overwrite").insertInto("nsfc.project_person_translate")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
  }
}
