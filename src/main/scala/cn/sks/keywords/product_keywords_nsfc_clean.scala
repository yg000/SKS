package cn.sks.keywords

import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.{break, breakable}

object product_keywords_nsfc_clean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("keywords_product_nsfc")
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

    //CleanSpecificSymbol
    spark.sqlContext.udf.register("Clean", (str: String) => CleanSpecificSymbol(str))

    //Cleanbracket
    spark.sqlContext.udf.register("Cleanbracket", (str: String) => CleanBracket(str)) //(;)
    spark.sqlContext.udf.register("Cleanbracket2", (str: String) => CleanBracket(str)) //[;]

    spark.sql("select * from nsfc.o_product_person_extend where keywords is not null").createOrReplaceTempView("o_product_person_extend")

    spark.sql(
      """
        |select
        |product_id,
        |Clean(keywords) as keywords
        |from o_product_person_extend
        |where keywords !="" and keywords !='***'
      """.stripMargin)
      .createOrReplaceTempView("product_clean")

    spark.sql(
      """
        |select product_id,
        |trim(Cleanbracket2(keywords)) as keywords
        |from product_clean
      """.stripMargin).createOrReplaceTempView("product_clean2")

    spark.sql(
      """
        |select product_id,
        |trim(Cleanbracket(keywords)) as keywords
        |from product_clean2
      """.stripMargin).createOrReplaceTempView("product_keywords_clean")


    //全角转半角
    spark.sqlContext.udf.register("regexPro", (str: String) => processTitle(str))
    //SplitZhEn
    spark.sqlContext.udf.register("SplitZhEn", (str: String) => SplitZhEn(str))

    val clean_rdd = spark.sql(
      """
        |select
        |product_id,
        |keywords,
        |splitZhEn(keywords) as en_keywords
        | from product_keywords_clean
      """.stripMargin)

    import spark.implicits._

    clean_rdd.toDF().rdd.map(s => {

      val id = s.getAs[String]("product_id")
      val en_keywords = s.getAs[String]("en_keywords").trim

      val zh_keywords = s.getAs[String]("keywords").replace(en_keywords, "").trim
      (id, zh_keywords, en_keywords)

    }).toDF("id", "zh_keywords", "en_keywords").createOrReplaceTempView("keywords_zh_en")

//    spark.sql("insert overwrite table nsfc.product_keywords_clean_zh_en from keywords_zh_en")

    spark.sql(
      """
        |select id,
        | trim(regexp_replace(zh_keywords,'\\s+'," ")) as zh_keywords,
        | trim(regexp_replace(en_keywords,'\\s+'," ")) as en_keywords
        | from keywords_zh_en
      """.stripMargin).createOrReplaceTempView("product_keywords_clean_zh_en")

    spark.sql(
      """
        |select id,
        | zh_keywords,
        | if(en_keywords="\;" or en_keywords="\.","null",en_keywords) as en_keywords
        | from product_keywords_clean_zh_en
      """.stripMargin).createOrReplaceTempView("product_keywords_clean_zh_en2")


    val rdd = spark.sql("select * from product_keywords_clean_zh_en2").toDF().rdd

    val value: RDD[String] = rdd.map(s => {
      var row = ""
      val id: String = s.getAs[String]("id")

      var zh_keywords: String = s.getAs[String]("zh_keywords")
      var en_keywords: String = s.getAs[String]("en_keywords")


      try {

        //        var en_row=""
        //        if(en_keywords!="") {
        //          var en_s = en_keywords.toString.split("")
        //          for (i <- 0 until en_s.size) {
        //            if (en_s(0) == ";") {
        //                en_s(0)=""
        //            }
        //            en_row+=en_s(i)
        //
        //          }
        //        }
        var zh_split = zh_keywords.toString.split(";")
        var en_split = zh_keywords.toString.split(";")

        if (zh_keywords != "" && en_keywords != "" && zh_split.size == en_split.size) {
          row += id
          row += "☼"
          row += zh_keywords
          row += "☼"
          row += en_keywords
          row += "☼"
          row += "standard"
        } else if (zh_keywords != "" && en_keywords != "" && zh_split.size + 1 == en_split.size) {
          row += id
          row += "☼"
          row += zh_keywords + en_split(0)
          row += "☼"
          for (i <- 1 until en_split.size) {
            row += en_split(i)
          }
          row += "☼"
          row += "nonstandard"
        } else if (zh_keywords != "" && en_keywords != "" && en_split.size == 1 && zh_split != 1) {
          row += id
          row += "☼"
          row += zh_keywords + en_keywords
          row += "☼"
          row += "null"
          row += "☼"
          row += "nonstandard"

        } else if (zh_keywords == "" && en_keywords != "") {
          row += id
          row += "☼"
          row += "null"
          row += "☼"
          row += en_keywords
          row += "☼"
          row += "nonstandard"
        } else if (zh_keywords != "" && en_keywords == "") {
          row += id
          row += "☼"
          row += zh_keywords
          row += "☼"
          row += "null"
          row += "☼"
          row += "nonstandard"
        } else {
          row += id
          row += "☼"
          row += zh_keywords
          row += "☼"
          row += en_keywords
          row += "☼"
          row += "nonstandard"
        }

        if (zh_keywords == "" && en_keywords == "") {}
      } catch {
        case ex: NullPointerException => "sss"
      }
      row
    })
    val product_key = value.toDF("key").createOrReplaceTempView("product_key")

    spark.sql(
      """
        |select
        |split(key,"☼")[0] as id,
        |split(key,"☼")[1] as zh_keywords,
        |split(key,"☼")[2] as en_keywords,
        |split(key,"☼")[3] as standard_or_not
        | from product_key
      """.stripMargin).createOrReplaceTempView("product_key_split")

    spark.sql(
      """
        |select
        |id,
        |if(zh_keywords="null","",zh_keywords) as zh_keywords,
        |if(en_keywords="null","",en_keywords) as en_keywords,
        |standard_or_not
        | from product_key_split
      """.stripMargin).createOrReplaceTempView("o_product_keywords_clean")

//CleanSeparator
    spark.sqlContext.udf.register("CleanSeparator",(str:String)=>{
      if (str == null || str == "" || str.trim == "..") ""
      else {
        val str_new=str
          .replaceAll("•",";")
          .replaceAll("·",";")
          .replaceAll("–",";")
          .replaceAll("/",";")
          .replaceAll("%",";")
          .replaceAll("\\.",";")
          .replaceAll("∙",";")
          .replaceAll("·",";")
        str_new
      }

    })

    spark.sql(
      """
        |select
        |id,
        |zh_keywords,
        |en_keywords,
        |standard_or_not
        | from o_product_keywords_clean where en_keywords rlike '[,\;]+' union all
        |select
        |id,
        |zh_keywords,
        |CleanSeparator(en_keywords) as en_keywords,
        |standard_or_not
        | from o_product_keywords_clean where en_keywords not rlike '[,\;]+'
      """.stripMargin).createOrReplaceTempView("o_product_keywords_clean_en")

    spark.sql(
      """
        |select
        |id,
        |zh_keywords,
        |en_keywords,
        |standard_or_not
        | from o_product_keywords_clean_en where zh_keywords rlike '[,\;]+' union all
        |select
        |id,
        |CleanSeparator(zh_keywords) as zh_keywords,
        |en_keywords,
        |standard_or_not
        | from o_product_keywords_clean_en where zh_keywords not rlike '[,\;]+'
      """.stripMargin).createOrReplaceTempView("o_product_keywords_clean_all")

    spark.sql(
      """
        |insert overwrite table dwd.wd_product_keywords_nsfc
        |select * from o_product_keywords_clean_all where zh_keywords  ='' and en_keywords !='' union all
        |select * from o_product_keywords_clean_all where zh_keywords !='' and en_keywords  ='' union all
        |select * from o_product_keywords_clean_all where zh_keywords !='' and en_keywords !=''
        |""".stripMargin)


//    spark.sql(
//      """
//        |select
//        |id,
//        |zh_keywords,
//        |if(en_keywords=".","null",en_keywords) as en_keywords,
//        |standard_or_not
//        | from product_key_split_1
//      """.stripMargin).createOrReplaceTempView("product_key_split1")
    //    spark.sql(
    //      """
    //        |insert overwrite table nsfc.product_keywords_clean_not_exists
    //        |select * from nsfc.product_keywords_clean_zh_en a
    //        |where not exists (select * from nsfc.product_keywords_clean_zh_en_2 b
    //        | where a.product_id = b.id)
    //      """.stripMargin)
  }


  def SplitZhEn(str: String): String = {
    if (str == null || str == "") ""
    else {
      try {
        val strings = str.reverse.split("")

        var sb = ""
        var zh = "[\u4e00-\u9fa5]"
        var flag = true

        breakable {
          for (i <- 0 until strings.size) {
            if (strings(i).matches(zh))
              break
            sb += strings(i)
          }

        }
        sb.reverse.trim

      } catch {
        case ex: NullPointerException => "sss"
      }
    }
  }

  def CleanBracket(str: String): String = {
    if (str == null || str == "") ""
    else {

      val strings: Array[String] = str.split("")
      var flag = 0
      var sb = ""
      for (i <- 0 until strings.size) {

        if (strings(i) == "(") {
          flag += 1
        } else if (strings(i) == ")") {
          flag -= 1
        }

        if (strings(i) == ";" && flag == 1) {
          strings(i) = "；"
        }
        sb += strings(i)
      }
      sb
    }
  }

  def CleanBracket2(str: String): String = {
    if (str == null || str == "") ""
    else {

      val strings: Array[String] = str.split("")
      var flag = 0
      var sb = ""
      for (i <- 0 until strings.size) {

        if (strings(i) == "[") {
          flag += 1
        } else if (strings(i) == "]") {
          flag -= 1
        }

        if (strings(i) == ";" && flag == 1) {
          strings(i) = "；"
        }
        sb += strings(i)
      }
      sb
    }
  }

  def CleanSpecificSymbol(str: String): String = {
    if (str == null || str == "") ""
    else {
      val str1 = str
        .replaceAll("，", ";").replaceAll("；", ";")
        .replaceAll(";;", ";").replaceAll(",", ";")
        .replaceAll("、", ";").replaceAll(";  ", ";")
        .replaceAll("  ;", ";").replaceAll("; ", ";")
        .replaceAll(" ;", ";").replaceAll("。", "")
        .toLowerCase
      str1
    }
  }

  def processTitle(oriTitle: String): String = { //全角转半角
    val res: String = someTwoBytesCharToOneByte(oriTitle)
    //若不包含空格则输出NULL
    //if (!res.contains(" ")) return null  regexp_replace(keywords,splitZhEn(keywords),"") as zh_keywords ,
    res
  }

  def someTwoBytesCharToOneByte(oriString: String): String = { //TODO 汉字和全角字符混合是否正常
    if (oriString == null) return null
    var c: Array[Char] = oriString.toCharArray
    for (index <- 0 to c.length - 1) {
      if (c(index) == '\u3000') {
        c(index) = ' ';
      } else if (c(index) > '\uFF00' && c(index) < '\uFF5F') {
        c(index) = (c(index) - 65248).toChar;

      }
    }
    return new String(c);

  }
}

//    nsfc.product_keywords_zh
//    spark.sql(
//      """
//        |insert into table nsfc.product_keywords_zh
//        |select
//        |product_id,
//        |Clean(keywords) from nsfc.o_product_person_extend
//        |where keywords rlike '[\\u4e00-\\u9fa5A-Za-z0-9\\u2000-\\u206F\\u0370\\u03FF\\uFF00-\uFFEF]+ +\\w+[\\u4e00-\\u9fa5]+$'
//        |or keywords rlike '[\\u4e00-\\u9fa5A-Za-z0-9\\u2000-\\u206F\\u0370\\u03FF\\uFF00-\uFFEF]+[\\u4e00-\\u9fa50-9]+$'
//        |or keywords rlike '[\\u4e00-\\u9fa5]+[\\u4e00-\\u9fa5]+$'
//      """.stripMargin)


//    spark.sql(
//      """
//        |select
//        |product_id,
//        |regexPro(Clean(keywords)) as keywords
//        |from nsfc.o_product_person_extend
//        |where keywords !="" and keywords !='***'
//        """.stripMargin)
//      .createOrReplaceTempView("product_clean")
////
//    val sql=spark.sql(
//      """
//        |insert into table nsfc.product_keywords_clean
//        |select product_id,
//        |trim(clean2(keywords)) as keywords
//        |from product_clean
//      """.stripMargin)

//    spark.sql(
//      """
//        |select product_id,
//        | regexp_extract(keywords,'[\\u4e00-\\u9fa5A-Za-z0-9\\u2000-\\u206F\\u0370\\u03FF\\uFF00-\uFFEF]+[\\u4e00-\\u9fa50-9]+$',0) as keywords
//        | from nsfc.product_keywords_clean
//      """.stripMargin).createOrReplaceTempView("zh")
