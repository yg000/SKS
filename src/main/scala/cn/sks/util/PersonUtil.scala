package cn.sks.util

import java.sql.DriverManager

import org.apache.spark.sql.{DataFrame, SparkSession}


object PersonUtil {

  def getDeliverOwnerRelation(spark:SparkSession,df:DataFrame):DataFrame = {
    //PersonUtil.getDeliverRelation (spark,PersonUtil.getDeliverRelation (spark,df,df),df).dropDuplicates("person_id_from","person_id_to")
    PersonUtil.getDeliverRelation (spark,df,df).dropDuplicates("person_id_from","person_id_to")
      //df
  }


  def dropDuplicates(spark:SparkSession,df:DataFrame,col1:String,col2:String):DataFrame = {

    df.createOrReplaceTempView("mid_tb")
    spark.sql(s"""
                 |select  * from (select * ,row_number()over(partition by $col1,$col2 order by person_level,person_id) as tid from mid_tb )a where tid = 1
                 |""".stripMargin).drop("tid")
  }

  def getAddTitle(spark:SparkSession,tb_name:DataFrame,title_tb:String):DataFrame = {
    tb_name.createOrReplaceTempView("tb_name")
    spark.sql(
      s"""
        |select a.*,clean_fusion(b.zh_title) as clean_title from  tb_name a join  $title_tb b on a.person_id=b.person_id
        |""".stripMargin)
  }




  def getWithOut(spark:SparkSession,df:DataFrame,withoutDf:DataFrame):DataFrame = {
    df.createOrReplaceTempView("tmp_tb")
    withoutDf.createOrReplaceTempView("not_in_tb")
    spark.sql(
      """
        |select * from tmp_tb a where not exists (select * from not_in_tb b where a.person_id=b.person_id_from)
        |""".stripMargin)
  }

  def getSource(spark:SparkSession,table_name:String):DataFrame={
    spark.sql(
      s"""
         |select person_id_to,concat_ws(',',collect_set(rule))as rule,concat_ws(',',collect_set(flow_source_from)) as  source  from $table_name group by person_id_to
         |""".stripMargin)

  }

  def getDistinctRelation(spark:SparkSession,originDf:DataFrame,distinctDf:DataFrame,col_1:String,col_2:String,col_3:String,rule:String):DataFrame = {
    originDf.createOrReplaceTempView("origin_tb")
    distinctDf.createOrReplaceTempView("distinct_tb")

    if ("" == col_3 ){
      spark.sql(
        s"""
           |select a.person_id as `person_id_from`,b.person_id as `person_id_to`
           |    ,a.flow_source as flow_source_from
           |    ,b.flow_source as flow_source_to
           |    ,'$rule' as rule
           |     from origin_tb a join distinct_tb b
           |    on a.$col_1 = b.$col_1 and  a.$col_2 = b.$col_2 where a.person_id != b.person_id
          """.stripMargin).dropDuplicates("person_id_from","person_id_to")
    }else {
      spark.sql(
        s"""
           |select a.person_id as `person_id_from`
           |,b.person_id as `person_id_to`
           |    ,a.flow_source as flow_source_from
           |    ,b.flow_source as flow_source_to
           |    ,'$rule' as rule
           |     from origin_tb a join distinct_tb b
           |    on a.$col_1 = b.$col_1 and  a.$col_2 = b.$col_2 and a.$col_3 = b.$col_3 where a.person_id != b.person_id
          """.stripMargin).dropDuplicates("person_id_from","person_id_to")
    }

  }


  def getDistinctRelationSecond(spark:SparkSession,df:DataFrame,col_1:String,col_2:String,col_3:String,rule:String):DataFrame = {
    df.createOrReplaceTempView("mid_tb")

    spark.sql(
      s"""
         |select a.person_id as person_id_from,b.person_id as person_id_to
         |,a.flow_source as flow_source_from
         |,b.flow_source as flow_source_to
         |,'$rule' as rule
         |from mid_tb a join mid_tb b on a.$col_1 = b.$col_1 and  a.$col_2 = b.$col_3  where a.person_id > b.person_id
         |""".stripMargin).dropDuplicates("person_id_from","person_id_to")

  }



  def getDeliverRelation(spark:SparkSession,fromDf:DataFrame,toDf:DataFrame):DataFrame = {
    fromDf.createOrReplaceTempView("from_tb")
    toDf.createOrReplaceTempView("to_tb")
    spark.sql(
      """
        |select
        | a.person_id_from,
        | ifnull(b.person_id_to,a.person_id_to) as person_id_to,
        | a.flow_source_from,
        | ifnull(b.flow_source_to,a.flow_source_to) as flow_source_to,
        | a.rule
        | from from_tb a left join to_tb b on a.person_id_to = b.person_id_from
        |""".stripMargin)

  }


  def getUnionRelation(spark:SparkSession,originDf:DataFrame,newDf:DataFrame):DataFrame = {
    originDf.createOrReplaceTempView("origin_tb")
    newDf.createOrReplaceTempView("new_tb")
    spark.sql(
      """
        |select * from origin_tb
        |union
        |select * from new_tb a where not exists (select * from origin_tb b where a.person_id_from=b.person_id_from)
        |""".stripMargin)

  }
  def getComparisonTableTst(spark:SparkSession,toDf:DataFrame,fromDf:DataFrame,col_1:String,col_2:String,col_3:String,rule:String):DataFrame={

    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    toDf.createOrReplaceTempView("to_tb")
    fromDf.createOrReplaceTempView("from_tb")
    if ("" == col_3 ){
      spark.sql(
        s"""
           |select
           |a.person_id as person_id_from
           |,b.person_id as person_id_to
           |from from_tb a join to_tb b on a.$col_1 = b.$col_1 and  a.$col_2 = b.$col_2
      """.stripMargin)
    }else {
      spark.sql(
        s"""
           |select
           |a.person_id as person_id_from
           |,b.person_id as person_id_to
           |from from_tb a join to_tb b on a.$col_1 = b.$col_1 and  a.$col_2 = b.$col_2 and a.$col_3 = b.$col_3
      """.stripMargin)
    }


  }

  def getComparisonTable(spark:SparkSession,toDf:DataFrame,fromDf:DataFrame,col_1:String,col_2:String,col_3:String,rule:String):DataFrame={

    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    toDf.createOrReplaceTempView("to_tb")
    fromDf.createOrReplaceTempView("from_tb")
    if ("" == col_3 ){
      spark.sql(
        s"""
           |select
           |a.person_id as person_id_from
           |,b.person_id as person_id_to
           |,a.flow_source as flow_source_from
           |,b.flow_source as flow_source_to
           |,'$rule' as rule
           |from from_tb a join to_tb b on a.$col_1 = b.$col_1 and  a.$col_2 = b.$col_2
      """.stripMargin)
    }else {
      spark.sql(
        s"""
           |select
           |a.person_id as person_id_from
           |,b.person_id as person_id_to
           |,a.flow_source as flow_source_from
           |,b.flow_source as flow_source_to
           |,'$rule' as rule
           |from from_tb a join to_tb b on a.$col_1 = b.$col_1 and  a.$col_2 = b.$col_2 and a.$col_3 = b.$col_3
      """.stripMargin)
    }


  }


  def getComparisonTableSecond(spark:SparkSession,toDf:DataFrame,fromDf:DataFrame,col_1:String,col_2:String,col_3:String,col_4:String,rule:String):DataFrame={

    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    toDf.createOrReplaceTempView("to_tb")
    fromDf.createOrReplaceTempView("from_tb")
    if ("" == col_4 ){
      spark.sql(
        s"""
           |select
           |a.person_id as person_id_from
           |,b.person_id as person_id_to
           |,a.flow_source as flow_source_from
           |,b.flow_source as flow_source_to
           |,'$rule' as rule
           |from from_tb a join to_tb b on a.$col_1 = b.$col_2 and  a.$col_3 = b.$col_3
      """.stripMargin)
    }else {
      spark.sql(
        s"""
           |select
           |a.person_id as person_id_from
           |,b.person_id as person_id_to
           |,a.flow_source as flow_source_from
           |,b.flow_source as flow_source_to
           |,'$rule' as rule
           |from from_tb a join to_tb b on a.$col_1 = b.$col_2 and  a.$col_3 = b.$col_3 and a.$col_4 = b.$col_4
      """.stripMargin)
    }


  }





  def DistinctArp(spark:SparkSession,originDf:DataFrame,distinctFields:String):(DataFrame,DataFrame) = {

    originDf.createOrReplaceTempView("temp")

    val distinctDf = spark.sql(
      s"""
         |select * from (
         |select *, row_number() over (partition by ${distinctFields} order by person_id ) rank from temp
         |)a where rank =1
      """.stripMargin).drop("rank")
    distinctDf.createOrReplaceTempView("distinctDf")


    var rel_df:DataFrame=null
    if (distinctFields.contains("birth_year")) {

      rel_df = spark.sql(
        """
          |select * from (
          |    select a.person_id as `old`,a.flow_source as old_flow_source,b.person_id as `new`,b.flow_source as new_flow_source from temp a left join distinctDf b
          |    on a.zh_name=b.zh_name and  a.clean_org=b.clean_org and a.birth_year=b.birth_year
          |)a where `new` <> old
        """.stripMargin)
    } else {
      rel_df= spark.sql(
        """
          |select * from (
          |    select a.person_id as `old`,a.flow_source as old_flow_source,b.person_id as `new`,b.flow_source as new_flow_source from temp a left join distinctDf b
          |    on a.zh_name=b.zh_name and  a.clean_email=b.clean_email
          |)a where `new` <> old
        """.stripMargin)
    }


    (distinctDf,rel_df)
  }

}
