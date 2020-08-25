package cn.sks.util

import java.sql.DriverManager

import org.apache.spark.sql.{DataFrame, SparkSession}

object CommonUtil {

  private var server = null
  private val port = "9080"
  private val dbDir = "/root/test;CACHE_SIZE=32384;MAX_LOG_SIZE=32384;mv_store=false" // ./h2db/
  private val user = "sa"
  private val password = ""

  def getDataTrace(spark:SparkSession,source:String,target:String)={
    val source_cc = spark.read.table(source).count()
    val target_cc = spark.read.table(target).count()
    try {
      Class.forName("org.h2.Driver")
      val conn = DriverManager.getConnection("jdbc:h2:tcp://10.0.80.191:" + port + "/" + dbDir, user, password)
      val stat = conn.createStatement
      // insert data
      //stat.execute("DROP TABLE IF EXISTS data_trace");
      stat.execute("CREATE TABLE if not exists data_trace(source VARCHAR,target varchar,source_cc varchar,target_cc varchar, UNIQUE KEY `uk_source_target` (`source`,`target`))")
      stat.execute(s"INSERT INTO data_trace VALUES('$source','$target','$source_cc','$target_cc')")
      // use data
      val result = stat.executeQuery("select source,target,source_cc,target_cc from data_trace ")
      var i = 1
      while ( {
        result.next
      }) System.out.println({
        i += 1; i - 1
      } + ":" + result.getString("source") + ":" + result.getString("source_cc") + ":" + result.getString("target_cc"))
      result.close()
      stat.close()
      conn.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }


  // 根据目标表自动校准 schema ，缺少字段，自动补空
  def completionSchemaFields(spark:SparkSession,originDF:DataFrame,targetTable:DataFrame):DataFrame = {
    var origin =originDF
    val origin_list = origin.schema.fieldNames.toList
    val target_list: List[String] = targetTable.schema.fieldNames.toList

    val diff_list = target_list diff origin_list
    import org.apache.spark.sql._
    val udf_null = functions.udf(()=> null )
    diff_list.foreach(x=>{
      origin=origin.withColumn(x,udf_null())
    })

    val build = new StringBuilder
    build.append("select  ")
    targetTable.schema.fieldNames.foreach(x=>{
      build.append(x+",")
    })

    origin.createOrReplaceTempView("temp")
    spark.sql(build.toString().stripSuffix(",") +  "  from temp")
  }


  def splicFlowSource(spark:SparkSession,df:DataFrame,originFlowSource:String,targetID:String,targetFlowSource:String,rule:String):DataFrame={

    import org.apache.spark.sql.functions._
    val originDf= df.groupBy(targetID,targetFlowSource).agg(concat_ws(",",collect_set(originFlowSource))).toDF(targetID,targetFlowSource,originFlowSource)
    originDf.createOrReplaceTempView("temp")

    val str1= "{\"from\":["
    val str2= "],\"to\":"
    val str3= ",\"rule\":\""
    val str4= "\"}"

    val frame= spark.sql(
      s"""
         |
        |select targetID
         | ,concat('${str1}',originFlowSource,'${str2}',targetFlowSource,'${str3}','${rule}','${str4}') as flow_source
         | from temp
         |
      """.stripMargin)
    frame
  }


}
