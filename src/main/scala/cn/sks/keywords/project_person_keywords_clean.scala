package cn.sks.keywords

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object project_person_keywords_clean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("project_person_keywords_clean")
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


    spark.sqlContext.udf.register("CleanZh", (str: String) => {
      if (str == null || str == "") ""
      else {
        val str1 = str
          .replace(",", ";").replace("；.；", ";")
          .replace(":；", ";").replace(";；", ";")
          .replace("）",")").replaceAll("；",";")
          .replace("（","(").replaceAll("\\.;",";")
          .replace("，",";")
          .replaceAll("(.*)\\.\\.","$1").trim
          .replaceAll("\\]","")
          .replaceAll("\\*","")
          .toLowerCase
        str1
      }
    })

//    spark.sql(
//      """
//        |insert overwrite table nsfc.project_person_keywords_nsfc_clean
//        |select
//        |id,
//        |type,
//        |code,
//        |discipline,
//        |direction,
//        |CleanZh(zh_research) as zh_research,
//        |if(en_research=" ","",en_research) as en_research,
//        |source
//        |from ods.o_nsfc_keywords
//      """.stripMargin)

    //Cleanbracket
    spark.sqlContext.udf.register("Cleanbracket", (str: String) => {
      if (str == null || str == "") ""
      else {

        val strings: Array[String] = str.split("")
        var flag=0
        var sb=""
        for(i <- 0 until strings.size){

          if(strings(i)=="("){
            flag += 1
          }else if(strings(i)==")"){
            flag -= 1
          }

          if(strings(i)==";" && flag==1){
            strings(i)="；"
          }
          sb+=strings(i)
        }
        sb
      }
    })

    // Cleanbracket2
    spark.sqlContext.udf.register("Cleanbracket2", (str: String) => {
      if (str == null || str == "") ""
      else {

        val strings: Array[String] = str.split("")
        var flag=0
        var sb=""
        for(i <- 0 until strings.size){

          if(strings(i)=="("){
            flag += 1
          }else if(strings(i)==")"){
            flag -= 1
          }

          if(strings(i)=="," && flag==1){
            strings(i)="，"
          }
          sb+=strings(i)
        }
        //        val str1 = str.replaceAll("(\\(.*);;(.*\\))","$1；；$2")
        //          .replaceAll("(\\(.*);(.*\\))","$1；$2")
        //          .toLowerCase
        sb
      }
    })

    spark.sqlContext.udf.register("CleanEn",(str:String)=>{
      if (str == null || str == "" || str.trim == "..") ""
      else {
        str.replaceAll("(.*)\\.\\.","$1").trim
      }
    })
    spark.sql(
      """
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |zh_research,
        |en_research,
        |source
        |from ods.o_nsfc_project_person_keywords where en_research rlike '[,\;]+' union
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |zh_research,
        |regexp_replace(en_research,"\.",";") as en_research,
        |source
        |from ods.o_nsfc_project_person_keywords where en_research  not rlike '[,\;]+'
      """.stripMargin).createOrReplaceTempView("zh_0")

//    spark.sql("select count(*) from zh_5").show()
//    while(true){}
    spark.sql("select * from zh_0 where zh_research ='' and en_research ='' ")
        .createOrReplaceTempView("zh_null")


    spark.sql("select * from zh_0 a" +
      " where not exists (select * from zh_null b where a.id=b.id)").createOrReplaceTempView("zh_1_not_null")

    spark.sql(
      """
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |CleanZh(zh_research) as zh_research,
        |if(en_research=" ","",en_research) as en_research,
        |source
        |from zh_1_not_null
      """.stripMargin).createOrReplaceTempView("zh_1")

    spark.sql(
      """
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |Cleanbracket(zh_research) as zh_research,
        |if(en_research='..',"",en_research) as en_research,
        |source
        |from zh_1
      """.stripMargin).createOrReplaceTempView("zh_2")



    spark.sql(
      """
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |zh_research,
        |CleanEn(en_research) as en_research,
        |source
        |from zh_2
      """.stripMargin).createOrReplaceTempView("zh_3")

    spark.sql(
      """
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |zh_research,
        |en_research,
        |source
        |from zh_3 where zh_research rlike '[,\;]+' union
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |regexp_replace(zh_research,"\.",";") as zh_research,
        |en_research,
        |source
        |from zh_3 where zh_research  not rlike '[,\;]+'
      """.stripMargin).createOrReplaceTempView("zh_4")

spark.sql("select count(*) from zh_4").show()
    while (true){}

    spark.sqlContext.udf.register("CleanComma",(str:String)=>{
      if (str == null || str == "" || str.trim == "..") ""
      else {
        var row=""
        val str1=str.split(";")
        for(i <- 0 until str1.size){
          row += str1(i)
        }
        if(row==""){
          row
        }else{
          str
        }
      }
    })

    spark.sql(
      """
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |CleanComma(zh_research) as zh_research,
        |CleanComma(en_research) as en_research,
        |source
        |from zh_4
      """.stripMargin).createOrReplaceTempView("zh_6")

    spark.sql("select * from zh_6 where zh_research='' and en_research='' ").createOrReplaceTempView("zh_6_null")

    spark.sql("select count(*) from zh_6_null").show()

    spark.sql("select * from zh_6 a" +
      " where not exists (select * from zh_6_null b where a.id=b.id)").createOrReplaceTempView("zh_6_not_null")


    //    spark.sql("select count(*) from zh_6_not_null").show()
//    while (true){}
//    spark.sql("create table nsfc.test as select * from zh_6")

    spark.sql(
      """
        |insert overwrite table nsfc.project_person_keywords_nsfc_clean
        |select
        |id,
        |type,
        |code,
        |discipline,
        |direction,
        |zh_research,
        |en_research,
        |source
        |from zh_6_not_null
      """.stripMargin)
  }
}
