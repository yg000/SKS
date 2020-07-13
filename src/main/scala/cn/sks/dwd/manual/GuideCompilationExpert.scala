package cn.sks.dwd.manual

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object GuideCompilationExpert {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("wd_manual_excel_guide_compilation_expert")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    spark.sql("select * from ods.o_manual_excel_guide_compilation_expert").dropDuplicates("id")
        .createOrReplaceTempView("temp")

    spark.sql(
      """
        |insert into table dwd.wd_manual_excel_guide_compilation_expert
        |select
        | md5(id) as id
        |,CleanFusion(zh_name) as zh_name
        |,en_name
        |,trim(organization) as org_name
        |,trim(prof_title)   as prof_title
        |,guide_name
        |,publish_department
        |,guide_year
        |,special_name
        |,special_type
        |,source_title
        |,source
        |,last_update
        |from temp
      """.stripMargin)







  }

}
