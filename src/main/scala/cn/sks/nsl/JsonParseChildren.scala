package cn.sks.nsl

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.util.parsing.json.JSON
import org.apache.spark.sql.functions._

object JsonParseChildren {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("JsonParseChildern")
      .config("spark.driver.memory", "16g")
      .config("spark.executor.memory", "32g")
      .config("spark.cores.max", "8")
      .config("spark.executor.core","11")
      .config("spark.rpc.askTimeout", "300")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields", "200")
      .config("spark.driver.maxResultSize", "4G")
      .config("sethive.enforce.bucketing", "true")
      .enableHiveSupport()
      .getOrCreate()

    //schema

//    |-- downTree: struct (nullable = true)
//    |    |-- attr: struct (nullable = true)
//    |    |    |-- Ins: string (nullable = true)
//    |    |    |-- id: long (nullable = true)
//    |    |    |-- type: string (nullable = true)
//    |    |-- children: array (nullable = true)
//    |    |    |-- element: struct (containsNull = true)
//    |    |    |    |-- attr: struct (nullable = true)
//    |    |    |    |    |-- Ins: string (nullable = true)
//    |    |    |    |    |-- id: long (nullable = true)
//    |    |    |    |    |-- type: string (nullable = true)
//    |    |    |    |-- children: array (nullable = true)
//    |    |    |    |    |-- element: string (containsNull = true)
//    |    |    |    |-- name: string (nullable = true)
//    |    |    |    |-- size: double (nullable = true)
//    |    |    |    |-- year: long (nullable = true)
//    |    |-- name: string (nullable = true)
//    |    |-- size: double (nullable = true)
//    |    |-- year: long (nullable = true)
//    |-- upTree: struct (nullable = true)
//    |    |-- attr: struct (nullable = true)
//    |    |    |-- Ins: string (nullable = true)
//    |    |    |-- id: long (nullable = true)
//    |    |    |-- type: string (nullable = true)
//    |    |-- children: array (nullable = true)
//    |    |    |-- element: string (containsNull = true)
//    |    |-- name: string (nullable = true)
//    |    |-- size: double (nullable = true)
//    |    |-- year: long (nullable = true)


    val jsonPath="/data/relationship/*.json"

    var jsonDF = spark.read
      .option("multiline","true")
      .json(jsonPath)

    jsonDF.printSchema()

    import spark.implicits._

    val df=jsonDF.select($"downTree.name",
                         $"downTree.year",
                         $"upTree.attr.Ins",
                 explode($"downTree.children"),
                         $"downTree.attr.Ins"
                 ).toDF("name","year","teacher_ins","children","ins")


    df.select($"name",
                    $"year",
                    $"children.name" as "name",
                    $"children.year" as "children_year",
                    $"children.attr.id" as "children_id",
                    $"teacher_ins" as "org_name",
                    $"children.attr.Ins" as "children_org_name")
      .createOrReplaceTempView("person")

    spark.sql("insert overwrite table ods.o_json_person_children select * from person")


    spark.sql(
      """
        |select
        |teacher_name,teacher_year,children_name,children_year,children_attr_id,org_name,children_org_name
        |from ods.o_json_person_children
        |group by
        |teacher_name,teacher_year,children_name,children_year,children_attr_id,org_name,children_org_name
      """.stripMargin).createOrReplaceTempView("person_group")

    spark.sql("insert overwrite table ods.o_json_person_children_new select * from person_group")
    println("============children group by count============")
    spark.sql("select count(*) from ods.o_json_person_children_new").show()

//    spark.sql(
//      """
//        |select * from (select *,
//        |row_number() over(partition by children_name,teacher_name,org_name order by children_name desc) as rank
//        |from  ods.o_json_person_children_new a)  where rank = 1
//      """.stripMargin).drop("rank").createOrReplaceTempView("children_group")
//
//
//    spark.sql(
//      """
//        |select
//        |teacher_name,
//        |teacher_year,
//        |children_name,
//        |children_year,
//        |children_attr_id,
//        |org_name,
//        |children_org_name
//        |md5(concat_ws('_',teacher_name,children_name,children_attr_id,children_org_name)) as children_id
//        |from ods.o_json_person_children_new
//      """.stripMargin).createOrReplaceTempView("o_json_person_children_id")
//
//    spark.sql("select children_id,count(*) c from children_id group by children_id having c >1").show()
//
//    spark.sql(
//      """
//        |select a.psn_code,b.children_id
//        |from ods.o_json_person_advisor a left join o_json_person_children_id b
//        |on a.teacher_name = b.teacher_name and a.org_name=b.org_name group by a.psn_code,b.children_id
//      """.stripMargin)
//
//    spark.sql("select count(*) from children_group").show()

//    spark.sql(
//      """
//        |select
//        |downTree.name as teacher_name,
//        |downTree.size as teacher_size,
//        |downTree.year as teacher_year,
//        | explode(children) as children
//        | from person
//      """.stripMargin)











  }

//  def getDf(Path: String,ss:SQLContext): DataFrame ={
//    val frame: DataFrame = ss.read.json(Path)
//    frame
//  }
//
//  def getFinalDF(arrPath: ArrayBuffer[String],ss:SQLContext): DataFrame = {
//    var index: Int = 0
//    breakable {
//      for (i <- 0 until arrPath.length) {
//        if (getDf(arrPath(i), ss).count() != 0) {
//          index = i
//          break
//        }
//      }
//    }
//
//    val df01 = ss.read.option("multiline","true").json(arrPath(index))
//
//    var aaa:String="name"
//    var df: DataFrame = df01
//    for(d <- index+1 until(arrPath.length)){
//      if(getDf(arrPath(d),ss).count()!=0){
//        val df1: DataFrame = ss.read.option("multiline","true").json(arrPath(d))
//        val df2: DataFrame = df.union(df1).toDF()
//        df=df2
//      }
//    }
//    df
//  }
//
//
//  def getFileName(path:String):ArrayBuffer[String]={
//    val conf: Configuration = new Configuration()
//    val hdfs: FileSystem = FileSystem.get(URI.create(path),conf)
//    val fs: Array[FileStatus] = hdfs.listStatus(new Path(path))
//    val arrPath: Array[Path] = FileUtil.stat2Paths(fs)
//    var arrBuffer:ArrayBuffer[String]=ArrayBuffer()
//    for(eachPath<-arrPath){
//      arrBuffer+=(folderPath+eachPath.getName)
//    }
//    arrBuffer
//  }
}
//    val frame=spark.read.format("json").load("/data/relationship/*.json")


//    val sql: SQLContext = spark.sqlContext
//
//    val arrPath: ArrayBuffer[String] = getFileName(folderPath)
//    var FinalDF: DataFrame = getFinalDF(arrPath,sql)
//    FinalDF.printSchema()
//    if(tag.length>0){
//      val writeDF: DataFrame = JsonUtil.ParserJsonDF(FinalDF,tag)
//      FinalDF=writeDF
//    }
//
//    FinalDF.show()