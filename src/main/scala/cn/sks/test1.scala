package cn.sks

import cn.sks.util.NameToPinyinUtil
import org.apache.spark.sql.SparkSession

object test1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("test")
      .config("spark.deploy.mode","2g")
      .config("spark.drivermemory","16g")
      .config("spark.cores.max","4")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.sql("select trim(zh_name) as zh_name from ods.o_arp_person ")

    val frame = NameToPinyinUtil.nameToPinyin(spark,df,"zh_name")
    frame.show()

    println(frame.count())





  }

}
