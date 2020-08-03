package cn.sks.nsl

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JsonParseTeacher {
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

    val bw:BufferedWriter=new BufferedWriter(
          new OutputStreamWriter(
            new FileOutputStream("src/main/resources/person.csv",true)))
    val fName: String = "/data/relationship/"

    val tempfile = new File(fName.trim())

    val files: Array[File] = tempfile.listFiles()

    var filelist: String = ""

    for (i <- 0 until files.length) {

      filelist += files(i).getName
      filelist += "\n"
    }

    val value: RDD[String] = spark.sparkContext.parallelize(filelist.split("\n"))

    value.toDF("person_code").createOrReplaceTempView("person")

    spark.sql(
      """
        |select regexp_replace(person_code,'\.json','') as person_code from person
      """.stripMargin)
      .createOrReplaceTempView("person1")

    //    spark.sql("select distinct(size(split(person_code,'-'))) from person1").show()

    spark.sql(
      """
        |select
        |if(size(split(person_code,'-'))=3,split(person_code,'-')[2],split(person_code,'-')[3]) as psn_code,
        |if(size(split(person_code,'-'))=3,split(person_code,'-')[1],split(person_code,'-')[2]) as zh_name,
        |if(size(split(person_code,'-'))=3,split(person_code,'-')[0],split(person_code,'-')[1]) as org_name from person1
        |
      """.stripMargin).createOrReplaceTempView("person2")

    spark.sql("insert overwrite table ods.o_json_person select * from person2")


    val jsonPath = "/data/relationship/*.json"

    var FinalDF = spark.read
      .option("multiline", "true")
      .json(jsonPath)

    val df = FinalDF.select($"upTree.name",
      $"upTree.year",
      $"upTree.attr.Ins",
      explode($"upTree.children"),
      $"upTree.attr.Ins"
    ).toDF("name", "year", "person_ins", "children", "ins")


    //
    df.select($"name",
      $"year",
      $"children.name" as "name",
      $"children.year" as "children_year",
      $"children.attr.id" as "children_id",
      $"person_ins" as "org_name",
      $"children.attr.Ins" as "children_org_name")
      .createOrReplaceTempView("person")

    spark.sql("insert overwrite table ods.o_json_person_teacher select * from person")

  }
}
