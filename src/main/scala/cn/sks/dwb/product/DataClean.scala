package cn.sks.dwb.product

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object DataClean {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("test")
      .config("spark.deploy.mode","4g")
      .config("spark.drivermemory","16g")
      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()


    spark.sqlContext.udf.register("fusion_clean",(str:String)=>{
      if (str == null) null
      else str.replaceAll("[^a-zA-Z0-9\u4E00-\u9FFF]","")
    })

    spark.sqlContext.udf.register("clean_completely",(str:String)=>{
      DefineUDF.clean_div(str)
    })
    spark.sqlContext.udf.register("clean_separator",(authors:String)=>{
     DefineUDF.clean_separator(authors)
    })



    val df = spark.sql("select fusion_clean(authors)) as authors1  from dwd.wd_product_person_ext_nsfc limit 10000 ")
    df.createOrReplaceTempView("df")

    df.collect().foreach(println)











  }



}
