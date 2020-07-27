package cn.sks.keywords

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object product_keywords_nsfc_split {
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
        |select * from nsfc.o_product_keywords_clean
      """.stripMargin)

    val rdd: RDD[Row] = sql.toDF().rdd


    val value: RDD[String] = rdd.map(s => {
      var row = ""

      val id: String = s.getAs[String]("product_id")

      var zh_keywords: String = s.getAs[String]("zh_keywords")
      var en_keywords: String = s.getAs[String]("en_keywords")
      var standard_or_not: String = s.getAs[String]("standard_or_not")

      //      var zh_translate: String = s.getAs[String]("zh_translate")
      //      var en_translate: String = s.getAs[String]("en_translate")

      try {
        if (zh_keywords != "") {
          val zh: Array[String] = zh_keywords.split("")
          if (zh(zh.size - 1) == ";") {
            zh_keywords += "null"
          }
          if (zh(0) == ";") {
            zh_keywords = "null" + zh_keywords
          }
        }
      } catch {
        case ex: NullPointerException =>
      }

      try {
        var zh_split = zh_keywords.toString.split(";")
        var en_split = en_keywords.toString.split(";")

        //        var zh_translate_split = zh_translate.toString.split(";")
        //        var en_translate_split = en_translate.toString.split(";")


        if (en_keywords != "" && zh_keywords != "" && zh_split.size == en_split.size) {
          if (zh_split.size == 1) {
            row += id
            row += "☼"
            row += zh_keywords
            row += "☿"
            row += en_keywords
            row += "☼"
            row += standard_or_not


          } else
            for (i <- 0 until zh_split.size) {
              row += id
              row += "☼"
              row += zh_split(i)
              row += "☿"
              row += en_split(i)
              row += "☼"
              row += standard_or_not
              row += "¤"
              if (i == zh_split.size - 1) {
                row += id
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += standard_or_not

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
                row += standard_or_not
                row += "¤"
                if (i == en_split.size - 1) {
                  row += id
                  row += "☼"
                  row += zh_split(i)
                  row += "☿"
                  row += en_split(i)
                  row += "☼"
                  row += standard_or_not
                }
              }
          }
          if (en_split.size > zh_split.size) {
              for (i <- 0 until zh_split.size) {
                row += id
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += standard_or_not
                row += "¤"
                if (i == zh_split.size - 1) {
                  row += id
                  row += "☼"
                  row += zh_split(i)
                  row += "☿"
                  row += en_split(i)
                  row += "☼"
                  row += standard_or_not
                }
              }
          }
        }
        if (zh_keywords == "" && en_keywords != "") {
          if (en_split.size == 1) {
            row += id
            row += "☼"
            row += "null"
            row += "☿"
            row += en_keywords
            row += "☼"
            row += standard_or_not
          } else
            for (i <- 0 until en_split.size) {
              row += id
              row += "☼"
              row += "null"
              row += "☿"
              row += en_split(i)
              row += "☼"
              row += standard_or_not
              row += "¤"

              if (i == en_split.size - 1) {
                row += id
                row += "☼"
                row += "null"
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += standard_or_not
              }
            }

        }
        if (en_keywords == "" && zh_keywords != "") {
          if (zh_split.size == 1) {
            row += id
            row += "☼"
            row += zh_keywords
            row += "☿"
            row += "null"
            row += "☼"
            row += standard_or_not
          } else
            for (i <- 0 until zh_split.size) {
              row += id
              row += "☼"
              row += zh_split(i)
              row += "☿"
              row += "null"
              row += "☼"
              row += standard_or_not
              row += "¤"
              if (i == zh_split.size - 1) {
                row += id
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += "null"
                row += "☼"
                row += standard_or_not
              }

            }
        }
        if (en_keywords == "" && zh_keywords == "") {}

      } catch {
        case ex: NullPointerException => "sss"
      }
      row

    })

    value.toDF("key").createOrReplaceTempView("project_person")

    spark.sql(
      """
        |select explode(split(key,"¤")) as key from project_person
      """.stripMargin).createOrReplaceTempView("explode_project_person")

    spark.sql(
      """
        |select
        |split(key,"☼")[0] as id,
        |split(key,"☼")[1] as keywords,
        |split(key,"☼")[2] as standard_or_not
        | from explode_project_person
      """.stripMargin).createOrReplaceTempView("split_explode_project_person")

    spark.udf.register("generateUUID",()=>UUID.randomUUID().toString.replace("-",""))

    spark.sql(
      """
        |select
        |id,
        |split(keywords,"☿")[0] as zh_keywords,
        |split(keywords,"☿")[1] as en_keywords,
        |standard_or_not,
        |"nsfc" as source
        | from split_explode_project_person
        |   group by id,zh_keywords,en_keywords,source,standard_or_not
      """.stripMargin).createOrReplaceTempView("o_product_keywords_nsfc_split")

    spark.sql(
      """
        |select
        |id,
        |zh_keywords,
        |en_keywords,
        |standard_or_not,
        |source
        | from o_product_keywords_nsfc_split
      """.stripMargin).createOrReplaceTempView("o_product_keywords_nsfc_split")

    spark.sql(
      """
        |insert overwrite table nsfc.o_product_keywords_nsfc_split
        |select * from o_product_keywords_nsfc_split where zh_keywords  ='null' and en_keywords !='null' union all
        |select * from o_product_keywords_nsfc_split where zh_keywords !='null' and en_keywords  ='null' union all
        |select * from o_product_keywords_nsfc_split where zh_keywords !='null' and en_keywords !='null'
        |""".stripMargin)


  }
}


