package cn.sks.dwd.manual

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object ReviewExpert {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("wd_manual_excel_review_expert")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    spark.sql("select * from ods.o_manual_excel_review_expert").dropDuplicates("id")
        .createOrReplaceTempView("temp")
    spark.sql(
      """
        |insert into table dwd.wd_manual_excel_review_expert
        |select
        | md5(id) as id
        |,CleanFusion(zh_name) as zh_name
        |,prof_title
        |,trim(organization) as org_name
        |,review_type
        |,review_group
        |,review_rounds
        |,review_year
        |,special_name
        |,special_type
        |,publish_department
        |,publish_date
        |,source_title
        |,source
        |,last_update
        |from temp
      """.stripMargin)








  }

}
