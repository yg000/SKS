package cn.sks.dwd.manual

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object Project {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("wd_manual_excel_project")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    spark.sql("select * from ods.o_manual_excel_project").dropDuplicates("id")
        .createOrReplaceTempView("temp")

    spark.sql(
      """
        |insert into table dwd.wd_manual_excel_project
        |select
        | md5(id) as id
        |,special_name
        |,special_level
        |,special_type
        |,project_year
        |,project_type
        |,project_no
        |,project_name
        |,project_leader_organization
        |,project_person
        |,person_organization
        |,prof_title
        |,classification
        |,bonus_amount
        |,start_date
        |,end_date
        |,publish_date
        |,publish_department
        |,source_title
        |,source
        |,last_update
        |from temp
      """.stripMargin)









  }

}
