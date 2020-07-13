package cn.sks.common

import cn.sks.util.ExcelParseUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExcelToHive {
  def main(args: Array[String]): Unit = {
    val target_table= "ods.o_manual_guide_expert"
    val excelPath= "C:\\Users\\yanggang\\Desktop\\excel\\toyg\\toyg\\指南编制专家\\指南编制专家_数据1的副本.xlsx"
    val id = "person_id"
    toHive(target_table,excelPath,id)
  }

  def toHive(target_table:String,excelPath:String,id:String)={
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("ExcelToHive")
//      .config("spark.deploy.mode","4g")
//      .config("spark.drivermemory","16g")
//      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    var origin: DataFrame = ExcelParseUtil.parseExcel(spark, excelPath)

//    var origin = spark.sql("select zh_name as name  from ods.o_arp_person limit 10")
    val origin_list = origin.schema.fieldNames.toList

    val target = spark.sql(s"select * from   ${target_table}")
    val target_list: List[String] = target.schema.fieldNames.toList

    val diff_list = target_list diff origin_list
    import org.apache.spark.sql._
    val udf_null = functions.udf(()=> null )
    diff_list.foreach(x=>{
      origin=origin.withColumn(x,udf_null())
    })

    val build = new StringBuilder
    build.append("select  ")
    target.schema.fieldNames.foreach(x=>{
      build.append(x+",")
    })

    origin.createOrReplaceTempView("temp")
    val sql_str =s"insert into ${target_table}  "+ build.toString().stripSuffix(",") + s"  from temp where ${id} is not null "
//    spark.sql(sql_str)

    println(s"恭喜你 ====》 insert into ${target_table} success ！")

  }


}
