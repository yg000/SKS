package cn.sks.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object AchievementUtil {


  def explodeAuthors(spark:SparkSession,oldDf:DataFrame,col_name:String):DataFrame={

    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    oldDf.createOrReplaceTempView("old_tb")
    spark.sql(
      s"""
        |select
        |achievement_id
        |,chinese_title as title
        |,explode(split($col_name,";")) as person_name
        |,flow_source
        |from old_tb
      """.stripMargin)


  }
  //from  yao zhuan hua de    to: zhuan hua dao
  def getComparisonTable(spark:SparkSession,old_table_from:String,old_table_to:String):DataFrame={

    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    val nsfc_en_name_1 = spark.sql(
      s"""
        |select
        |a.achievement_id as achievement_id_from
        |,b.achievement_id as achievement_id_to
        |,"6" as product_type
        |,a.flow_source as flow_source_from
        |,b.flow_source as flow_source_to
        |from $old_table_from a join $old_table_to b on clean_fusion(a.title)=clean_fusion(b.title) and clean_fusion(a.en_name_normal)=clean_fusion(b.en_name_normal)
      """.stripMargin)



    val nsfc_en_name_2 = spark.sql(
      s"""
        |select
        |a.achievement_id as achievement_id_from
        |,b.achievement_id as achievement_id_to
        |,"6" as product_type
        |,a.flow_source as flow_source_from
        |,b.flow_source as flow_source_to
        |from $old_table_from a join $old_table_to b on clean_fusion(a.title)=clean_fusion(b.title) and clean_fusion(a.en_name_inverted)=clean_fusion(b.en_name_inverted)
      """.stripMargin)

    nsfc_en_name_1.union(nsfc_en_name_2).dropDuplicates("achievement_id_from").createOrReplaceTempView("comparison_table")
    spark.sql(
      """
        |select
        |achievement_id_to
        |,achievement_id_from
        |,product_type
        |,concat("{","\"from\"",":",flow_source_from,",","\"to\"",":",flow_source_to,",","\"rule\"",":","\"name+title\"","}") as source
        |from comparison_table
      """.stripMargin)
      .repartition(1)

  }

  def getSource(spark:SparkSession,table_name:String):DataFrame={
    spark.sql(
      s"""
        |select achievement_id_to as achievement_id, concat_ws(',',collect_set(achievement_id_from)) as  source  from $table_name group by achievement_id_to
        |""".stripMargin)

  }

}
