package cn.sks.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object CommonUtil {

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
