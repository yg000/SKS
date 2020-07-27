package cn.sks.translate

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object csai_keywords_split {
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



    val sql = spark.sql(
      """
        |select keyword_id,explode(zh_keyword) as zh_keyword from nsfc.o_csai_keyword_translate_4 where size(split(en_keywords,';')) <50
      """.stripMargin)

    val rdd: RDD[Row] = sql.toDF().rdd


    val value: RDD[String] = rdd.map(s => {
      var row = ""

      val id: String = s.getAs[String]("keyword_id")

      var zh_keywords: String = s.getAs[String]("zh_keyword").replaceAll("☼","").replaceAll("☿","☿")
      var en_keywords: String = s.getAs[String]("en_keywords").replaceAll("☼","").replaceAll("☿","☿")
      var source: String = s.getAs[String]("source")


      try {
        var zh_split = zh_keywords.toString.split(";")
        var en_split = en_keywords.toString.split(";")


        if (en_keywords != "" && zh_keywords != "" && zh_split.size == en_split.size) {
          if (zh_split.size == 1) {
            row += id
            row += "☼"
            row += zh_keywords
            row += "☿"
            row += en_keywords
            row += "☼"
            row += source


          } else
            for (i <- 0 until zh_split.size) {
              row += id
              row += "☼"
              row += zh_split(i)
              row += "☿"
              row += en_split(i)
              row += "☼"
              row += source
              row += "¤"
              if (i == zh_split.size - 1) {
                row += id
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += source

              }
            }

        }
        //unequal
        if (en_keywords != "" && zh_keywords != "" && zh_split.size != en_split.size) {

          if (zh_split.size > en_split.size) {
              for (i <- 0 until en_split.size) {
                row += id
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += source
                row += "¤"
                if (i == en_split.size - 1) {
                  row += id
                  row += "☼"
                  row += zh_split(i)
                  row += "☿"
                  row += en_split(i)
                  row += "☼"
                  row += source
                }
              }
          }
        }

      } catch {
        case ex: NullPointerException => "sss"
      }
      row

    })

    value.toDF("key").createOrReplaceTempView("csai_keyword")

    spark.sql(
      """
        |select explode(split(key,"¤")) as key from csai_keyword
      """.stripMargin).createOrReplaceTempView("csai_keyword_explode")

    spark.sql(
      """
        |select
        |split(key,"☼")[0] as id,
        |split(key,"☼")[1] as keywords,
        |split(key,"☼")[2] as source
        | from csai_keyword_explode
      """.stripMargin).createOrReplaceTempView("split_explode_csai_keyword")

    spark.udf.register("generateUUID",()=>UUID.randomUUID().toString.replace("-",""))

    spark.sql(
      """
        |select
        |id,
        |split(keywords,"☿")[0] as zh_keywords,
        |split(keywords,"☿")[1] as en_keywords,
        |source
        | from split_explode_csai_keyword
        |   group by id,zh_keywords,en_keywords,source
      """.stripMargin).createOrReplaceTempView("o_csai_keyword_split")


    spark.sql(
      """
        |insert overwrite table nsfc.csai_keywords_split_2
        |select a.keyword_id,a.keyword,b.en_keywords,b.source from ods.o_csai_keyword a
        | left join o_csai_keyword_split b on b.zh_keywords=a.keyword
        |""".stripMargin)


  }
}


