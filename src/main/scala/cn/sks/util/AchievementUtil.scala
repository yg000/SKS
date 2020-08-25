package cn.sks.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.h2.tools.Server


object AchievementUtil {
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

  def tranferAchievementID(spark:SparkSession,df:DataFrame,col_name:String):DataFrame={

    df.createOrReplaceTempView("tb_name")

    spark.sql(
      s"""
        |select
        |a.*,
        |ifnull(b.achievement_id, a.$col_name)as new_achievement_id
        | from tb_name a left join dwb.wb_product_rel b on a.$col_name = b.achievement_id_origin
      """.stripMargin)


  }


  def getAuthors(spark:SparkSession,table_name:String,authers_tb:String,col_name:String):DataFrame={
    spark.sql(
      s"""
        |select achievement_id,concat_ws(';',collect_set(person_name)) as authors from $authers_tb group by achievement_id
        |""".stripMargin).createOrReplaceTempView("get_authors")

    val get_table = spark.read.table(s"$table_name")
    get_table.drop(col_name)

  }

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
        |,flow_source_to
        |,flow_source_from
        |,concat("{","\"from\"",":",flow_source_from,",","\"to\"",":",flow_source_to,",","\"rule\"",":","\"name+title\"","}") as source
        |from comparison_table
      """.stripMargin)
      .repartition(1)

  }

  def getSource(spark:SparkSession,table_name:String):DataFrame={
    spark.sql(
      s"""
        |select achievement_id_to as achievement_id, concat_ws(',',collect_set(flow_source_from)) as  source  from $table_name group by achievement_id_to
        |""".stripMargin)

  }



}
