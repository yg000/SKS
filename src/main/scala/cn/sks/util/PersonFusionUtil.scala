package cn.sks.util

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.reflect.internal.util.TableDef.Column

object PersonFusionUtil {

 /* 规则：人名和单位名字融合
     人名默认为 zh_name,单位名 默认为 org_name
    参数：
           spark
           originPerson    :原始表
           originID       :原始表唯一值字段（person_id）
           targetPerson  :目标表
           targetID     :目标表表唯一值字段（person_id）

    返回值（元组）  :（rel_person_one,rel_person_more,person_not_exists）
          rel_person_one      : 一对一 （原始表的一个人对应 目标表中的一个人）
          rel_person_more    : 一对多 （原始表的一个人对应 目标表中的多个人）
          person_not_exists : 未融合的人（原始表 中的人 在目标表中找不到对应关系）
   */
 def fusionPerson_Org(spark:SparkSession,originPerson:DataFrame,originID:String,targetPerson:DataFrame,targetID:String):(DataFrame,DataFrame,DataFrame) = {
   spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
     DefineUDF.clean_fusion(str)
    })
   originPerson.createOrReplaceTempView("temp_origin")
   targetPerson.createOrReplaceTempView("temp_target")
   //  原始表
   val person_origin = spark.sql(
      """
        |select CleanFusion(zh_name) as clean_zh_name,CleanFusion(org_name) as clean_org_name,*  from temp_origin
        | where zh_name is not null and org_name is not null
        |""".stripMargin)
    person_origin.createOrReplaceTempView("person_origin")
   // 目标表
    val person_target =spark.sql(
      s"""
        |select CleanFusion(zh_name) as clean_zh_name, CleanFusion(org_name) as clean_org_name,${targetID} from temp_target
        |""".stripMargin)
    person_target.createOrReplaceTempView("person_target")

    // 两部分人中的交集（人员对应关系）
    val rel_person= spark.sql(
      s"""
        |    select * from (
        |        select  a.${originID} as originID ,b.${targetID} as targetID
        |        from  person_origin a join person_target b
        |        on a.clean_zh_name=b.clean_zh_name and a.clean_org_name = b.clean_org_name
        |       )a  group by originID,targetID
      """.stripMargin).cache()
    rel_person.createOrReplaceTempView("rel_person")

    // 一对一 （原始的一个人对应 目标表中的一个人）
    val rel_person_one = spark.sql("select originID,targetID from rel_person where originID in (select originID from rel_person group by originID having count(originID)<2)")
    // 一对多 （原始的一个人对应 目标表中的多个人）
    val rel_person_more = spark.sql("select originID,targetID from rel_person where originID in (select originID from rel_person group by originID having count(originID)>1)")
    // 未融合的人（原始表 中的人 在目标表中找不到对应关系）
    val person_not_exists = spark.sql(s"select * from  temp_origin a  where not exists (select * from rel_person b where a.${originID}=b.originID)")
        .drop("clean_zh_name","clean_org_name")

    (rel_person_one,rel_person_more,person_not_exists)
  }

}
