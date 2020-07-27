package cn.sks.keywords

import java.lang.NullPointerException
import java.util.UUID


import util.control.Breaks._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object project_person_keywords_split {
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

//    spark.sqlContext.udf.register("TranslateToZh",(str:String)=> DefinedUDF.translate(str,"zh"))
//    spark.sqlContext.udf.register("TranslateToEn",(str:String)=> DefinedUDF.translate(str,"en"))

    val sql = spark.sql(
      """
        |select
        |id,
        |code,
        |source,
        |type,
        |zh_research,
        |en_research
        | from nsfc.o_project_person_keywords_nsfc_clean
      """.stripMargin)

    val rdd: RDD[Row] = sql.toDF().rdd


    val value: RDD[String] = rdd.map(s => {
      var row = ""

      val id: String = s.getAs[String]("id")
      val code: String = s.getAs[String]("code")
      val source: String = s.getAs[String]("source")
      val type1: String = s.getAs[String]("type")

      var zh_research: String = s.getAs[String]("zh_research")
      var en_research: String = s.getAs[String]("en_research")

//      var zh_translate: String = s.getAs[String]("zh_translate")
//      var en_translate: String = s.getAs[String]("en_translate")

      try {
        if (zh_research != "") {
          val zh: Array[String] = zh_research.split("")
          if (zh(zh.size - 1) == ";") {
            zh_research += "null"
          }
          if (zh(0) == ";") {
            zh_research = "null" + zh_research
          }
        }
      } catch {
        case ex: NullPointerException =>
      }

      try {
        var zh_split = zh_research.toString.split(";")
        var en_split = en_research.toString.split(";")

//        var zh_translate_split = zh_translate.toString.split(";")
//        var en_translate_split = en_translate.toString.split(";")


        if (en_research != "" && zh_research != "" && zh_split.size == en_split.size ) {
          if (zh_split.size == 1) {
            row += id
            row += "☼"
            row += type1
            row += "☼"
            row += code
            row += "☼"
            row += zh_research
            row += "☿"
            row += en_research
            row += "☼"
            row += "standard"
          } else
            for (i <- 0 until zh_split.size) {
              row += id
              row += "☼"
              row += type1
              row += "☼"
              row += code
              row += "☼"
              row += zh_split(i)
              row += "☿"
              row += en_split(i)
              row += "☼"
              row += "standard"
              row += "¤"
              if (i == zh_split.size - 1) {
                row += id
                row += "☼"
                row += type1
                row += "☼"
                row += code
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += "standard"
              }
            }

        }
        //unequal
        else if (en_research != "" && zh_research != "" && zh_split.size != en_split.size ) {

          if(zh_split.size > en_split.size){
            if (en_split.size == 1) {
              row += id
              row += "☼"
              row += type1
              row += "☼"
              row += code
              row += "☼"
              row += zh_research
              row += "☿"
              row += en_research
              row += "☼"
              row += "nonstandard"
            } else
            for (i <- 0 until en_split.size) {
              row += id
              row += "☼"
              row += type1
              row += "☼"
              row += code
              row += "☼"
              row += zh_split(i)
              row += "☿"
              row += en_split(i)
              row += "☼"
              row += "nonstandard"
              row += "¤"
              if (i == en_split.size - 1) {
                row += id
                row += "☼"
                row += type1
                row += "☼"
                row += code
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += "nonstandard"
              }
            }
          }
          if( en_split.size > zh_split.size ){
            if (zh_split.size == 1) {
              row += id
              row += "☼"
              row += type1
              row += "☼"
              row += code
              row += "☼"
              row += zh_research
              row += "☿"
              row += en_research
              row += "☼"
              row += "nonstandard"
            } else
            for (i <- 0 until zh_split.size) {
              row += id
              row += "☼"
              row += type1
              row += "☼"
              row += code
              row += "☼"
              row += zh_split(i)
              row += "☿"
              row += en_split(i)
              row += "☼"
              row += "nonstandard"
              row += "¤"
              if (i == zh_split.size - 1) {
                row += id
                row += "☼"
                row += type1
                row += "☼"
                row += code
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += "nonstandard"
              }
            }
          }
        } else if (zh_research == "" && en_research != "") {
          if (en_split.size == 1) {
            row += id
            row += "☼"
            row += type1
            row += "☼"
            row += code
            row += "☼"
            row += "null"
            row += "☿"
            row += en_research
            row += "☼"
            row += "nonstandard"
          } else
            for (i <- 0 until en_split.size) {
              row += id
              row += "☼"
              row += type1
              row += "☼"
              row += code
              row += "☼"
              row += "null"
              row += "☿"
              row += en_split(i)
              row += "☼"
              row += "nonstandard"
              row += "¤"

              if (i == zh_split.size - 1) {
                row += id
                row += "☼"
                row += type1
                row += "☼"
                row += code
                row += "☼"
                row += "null"
                row += "☿"
                row += en_split(i)
                row += "☼"
                row += "nonstandard"
              }
            }

        } else if (en_research == "" && zh_research != "") {
          if (zh_split.size == 1) {
            row += id
            row += "☼"
            row += type1
            row += "☼"
            row += code
            row += "☼"
            row += zh_research
            row += "☿"
            row += "null"
            row += "☼"
            row += "nonstandard"
          } else
            for (i <- 0 until zh_split.size) {
              row += id
              row += "☼"
              row += type1
              row += "☼"
              row += code
              row += "☼"
              row += zh_split(i)
              row += "☿"
              row += "null"
              row += "☼"
              row += "nonstandard"
              row += "¤"
              if (i == zh_split.size - 1) {
                row += id
                row += "☼"
                row += type1
                row += "☼"
                row += code
                row += "☼"
                row += zh_split(i)
                row += "☿"
                row += "null"
                row += "☼"
                row += "nonstandard"
              }

            }
        }
        if (en_research == "" && zh_research == "") {}

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
        |split(key,"☼")[1] as type,
        |split(key,"☼")[2] as code,
        |split(key,"☼")[3] as keywords,
        |split(key,"☼")[4] as standard_or_not
        | from explode_project_person
      """.stripMargin).createOrReplaceTempView("split_explode_project_person")

    spark.sql(
      """
        |select
        |id,
        |type,
        |code,
        |split(keywords,"☿")[0] as zh_keywords,
        |split(keywords,"☿")[1] as en_keywords,
        |standard_or_not,
        |"nsfc" as source
        | from split_explode_project_person
        |   group by id,code,type,zh_keywords,en_keywords,standard_or_not,source
      """.stripMargin).createOrReplaceTempView("split_explode_project_person_1")


    spark.sql(
      """
        |select
        |id,
        |type,
        |code,
        |if(zh_keywords="","null",zh_keywords) as zh_keywords,
        |if(en_keywords="","null",en_keywords) as en_keywords,
        |standard_or_not,
        |source
        | from split_explode_project_person_1 group by
        |id,type,code,zh_keywords,en_keywords,standard_or_not,source
      """.stripMargin).createOrReplaceTempView("o_project_person_keywords_nsfc_split")



   spark.sql(
     """
       |insert overwrite table dwd.wd_project_person_keywords_split_nsfc
       |select * from o_project_person_keywords_nsfc_split where zh_keywords  ='null' and en_keywords !='null' union all
       |select * from o_project_person_keywords_nsfc_split where zh_keywords !='null' and en_keywords ='null' union all
       |select * from o_project_person_keywords_nsfc_split where zh_keywords !='null' and en_keywords !='null'
       |""".stripMargin)

    //      .createOrReplaceTempView("split_explode_project_person_2")
//
//    spark.sql(
//      """
//        |insert overwrite table nsfc.o_project_person_keywords_nsfc_split
//        |select
//        |id,
//        |type,
//        |code,
//        |zh_keywords,
//        |en_keywords,
//        |standard_or_not,
//        |source
//        | from split_explode_project_person_2
//        |  where zh_keywords !='null' and en_keywords !='null'
//      """.stripMargin)
  }
}


