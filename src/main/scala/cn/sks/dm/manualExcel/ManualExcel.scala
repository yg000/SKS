package cn.sks.dm.manualExcel

import org.apache.spark.sql.SparkSession

object ManualExcel {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("ManualExcel To DM")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("insert into table dm.dm_neo4j_excel_team                     select * from dwb.wb_excel_team                  ")
    spark.sql("insert into table dm.dm_neo4j_excel_reward                   select * from dwb.wb_excel_reward                ")
    spark.sql("insert into table dm.dm_neo4j_excel_team_reward              select * from dwb.wb_excel_team_reward           ")
    spark.sql("insert into table dm.dm_neo4j_excel_person_reward            select * from dwb.wb_excel_person_reward         ")
    spark.sql("insert into table dm.dm_neo4j_excel_special_project          select * from dwb.wb_excel_special_project       ")
    spark.sql("insert into table dm.dm_neo4j_excel_person_special_project   select * from dwb.wb_excel_person_special_project")
    spark.sql("insert into table dm.dm_neo4j_excel_outstanding              select * from dwb.wb_excel_outstanding           ")
    spark.sql("insert into table dm.dm_neo4j_excel_person_outstanding       select * from dwb.wb_excel_person_outstanding    ")
    spark.sql("insert into table dm.dm_neo4j_excel_guide                    select * from dwb.wb_excel_guide                 ")
    spark.sql("insert into table dm.dm_neo4j_excel_guide_special_project    select * from dwb.wb_excel_guide_special_project ")
    spark.sql("insert into table dm.dm_neo4j_excel_person_guide             select * from dwb.wb_excel_person_guide          ")


  }

}
