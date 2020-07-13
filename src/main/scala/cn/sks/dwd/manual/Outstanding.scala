package cn.sks.dwd.manual

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object Outstanding {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("wd_manual_excel_outstanding")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    spark.sql("select * from ods.o_manual_excel_outstanding").dropDuplicates("id")
        .createOrReplaceTempView("temp")
    spark.sql(
      """
        |insert into table dwd.wd_manual_excel_outstanding
        |select
        | md5(id) as id
        |,outstanding_tittle
        |,outstanding_level
        |,include_outstanding
        |,outstanding_type
        |,publish_date
        |,batches
        |,trim(team_name) as team_name
        |,CleanFusion(zh_name) as zh_name
        |,gender
        |,trim(person_organization)  as org_name
        |,prof_title
        |,position
        |,project_name
        |,classification
        |,research_area
        |,bonus_sourc
        |,bonus_amount
        |,trim(issued_by) as issued_by
        |,source_title
        |,source
        |,last_update
        |from temp  where  outstanding_type = "个人"
      """.stripMargin)






  }

}
