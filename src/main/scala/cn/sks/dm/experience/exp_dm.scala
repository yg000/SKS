package cn.sks.dm.experience

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession
import java.util


object exp_dm {
  val spark = SparkSession.builder()
    //.master("local[12]")
    .appName("exp_dm")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  spark.sqlContext.udf.register("trans_advisor",(str:String) =>{
    try {
      if(DefineUDF.isAllEnglish(str.replace(" ","").replace(".","").replace(",","").replace("-","")))
        str
      else
        str.replace(" 教授","").replace("教授","").replace("Prof.","").replace("(","").replace(")","")
          .replace("/",";").replace("；",";").replace(",",";").replace(" ",";").replace("，",";").replace("、",";")

    } catch {
      case ex: Exception =>{
        println("Missing file exception")
      }
        str+" null"
    }

  })



  def main(args: Array[String]): Unit = {


    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_experience_study
        |select
        |person_id,
        |org_id ,
        |start_date  ,
        |end_date ,
        |profession,
        |degree,
        |country,
        |award_year,
        |if(advisor is null,advisor,trans_advisor(advisor)),
        |source  from dwd.wd_nsfc_experience_study
        |""".stripMargin)


    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_experience_work
        |select
        |person_id,
        |org_id ,
        |start_date  ,
        |end_date ,
        |deparment ,
        |prof_title ,
        |source  from dwd.wd_nsfc_experience_work
        |""".stripMargin)


    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_experience_postdoctor
        |select
        |person_id,
        |org_id ,
        |start_date  ,
        |end_date ,
        |if(advisor is null,advisor,trans_advisor(advisor)),
        |onjob,
        |source  from dwd.wd_nsfc_experience_postdoctor
        |""".stripMargin)

  }
}
