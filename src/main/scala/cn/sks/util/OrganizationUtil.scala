package cn.sks.util

import java.util
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import me.xdrop.fuzzywuzzy.FuzzySearch
import me.xdrop.fuzzywuzzy.ratios.PartialRatio

object OrganizationUtil {

  def dropDuplicates(spark:SparkSession,df:DataFrame,col:String,order_by_col:String):DataFrame= {
    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    df.createOrReplaceTempView("mid_tb")
    spark.sql(s"""
                |select  * from (select * ,row_number()over(partition by clean_fusion($col) order by $order_by_col desc) as tid from mid_tb )a where tid = 1
                |""".stripMargin)
  }



  def getStandardOrganization(spark:SparkSession):DataFrame= {


      spark.sql(
        """
          |select * from dwb.wb_organization
          |union all
          |select * from dwb.wb_organization_add
          |""".stripMargin)
  }

  def getOrgComparisonTable(spark:SparkSession):DataFrame= {
    spark.sql(
      s"""
         |select * from dwb.wb_organization_comparison_table
         |union
         |select * from dwb.wb_organization_comparison_table_exp
         |""".stripMargin)

  }
  def getStandardOrgComparisonTable(spark:SparkSession):DataFrame= {
    getStandardOrganization(spark).createOrReplaceTempView("standard_org_tb")
    spark.sql(
      """
        |select * from standard_org_tb where length(org_name)>=3 and org_name not in ('中国科学','Institut','Center','卫生部','中科院','加拿大','意大利','俄罗斯','农业部','教育部','信息学院','科技大学','The','理工学院','澳大利亚','Asia','化学研究所','物理学院','University of','Institute','Japan','博士后流动站','研究院','江苏省','新加坡','工程学院','工学院','中国科学院','农业大学')
        |""".stripMargin)
  }

  def getFuzzyMatchOrgName(spark:SparkSession,df:DataFrame)= {

    spark.sqlContext.udf.register("transform_name",(str:String) =>{
      str.replace("中科院","中国科学院").replace("社科院","社会科学院").replace("国网","国家电网")
        .replace("市","").replace("省","").replace("自治区","").replace("县","")
        .replace("有限","").replace("责任","").replace("股份","").replace("集团","")
    })

    df.createOrReplaceTempView("match_tb")
    val broadData =
      spark.sql(
        """
          |select transform_name(org_name) from dwb.wb_organization where length(org_name)>=3 and org_name not in ('中国科学','Institut','Center','卫生部','中科院','加拿大','意大利','俄罗斯','农业部','教育部','信息学院','科技大学','The','理工学院','澳大利亚','Asia','化学研究所','物理学院','University of','Institute','Japan','博士后流动站','研究院','江苏省','新加坡','工程学院','工学院','中国科学院','农业大学') order by org_name desc
          |""".stripMargin).rdd.collect()

    val list = new util.ArrayList[String]()
    broadData.foreach(x=>{
      list.add(x(0).toString)
    })
    import spark.implicits._
    spark.sql(
      """
        |select org_name,transform_name(org_name) as new_name from match_tb
        |""".stripMargin).rdd.repartition(400).mapPartitions(it=>{
      val lst = new ArrayBuffer[(String,String,String,String)]()
      it.foreach(x=>{
        val result = FuzzySearch.extractOne(x(1).toString, list, new PartialRatio)
        lst.append((x(0).toString,result.getString,result.getScore.toString,result.getIndex.toString))
      })
      lst.toIterator
    }).toDF("name","new_name","score","id").createOrReplaceTempView("new_name_tb")

    spark.sql(
      """
        |select org_name,row_number()over(order by org_name desc) as tid from dwb.wb_organization where length(org_name)>=3 and org_name not in ('中国科学','Institut','Center','卫生部','中科院','加拿大','意大利','俄罗斯','农业部','教育部','信息学院','科技大学','The','理工学院','澳大利亚','Asia','化学研究所','物理学院','University of','Institute','Japan','博士后流动站','研究院','江苏省','新加坡','工程学院','工学院','中国科学院','农业大学')
        |""".stripMargin).createOrReplaceTempView("wb_organization")

    spark.sql(
      """
        |select  name,new_name,org_name,score,id from new_name_tb a left join  wb_organization b on a.id+1 = b.tid
        |""".stripMargin).rdd.map(x=>{
      x(0).toString + "\t" + x(1).toString + "\t" + x(2).toString + "\t" + x(3).toString + "\t" + x(4)
    }).repartition(1).saveAsTextFile("match_name")

  }
}
