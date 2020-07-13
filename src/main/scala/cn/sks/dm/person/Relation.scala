package cn.sks.dm.person

import org.apache.spark.sql.SparkSession

object Relation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("dm_Relation")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val person_subject =spark.sql("select person_id,two_rank_id from dwb.wb_person_subject")
      .dropDuplicates("person_id","two_rank_id")
    val person_keywords= spark.sql("select person_id,keywords_id from dwb.wb_keywords_person_nsfc")
      .dropDuplicates("person_id","keywords_id")


    val project_keywords= spark.sql("select project_id,keywords_id from dwb.wb_keywords_project_nsfc")
      .dropDuplicates("project_id","keywords_id")


    person_subject.createOrReplaceTempView("person_subject")
    person_keywords.createOrReplaceTempView("person_keywords")
    project_keywords.createOrReplaceTempView("project_keywords")




    spark.sql("insert into table dm.dm_neo4j_person_subject   select * from person_subject where two_rank_id is not null")
    spark.sql("insert into table dm.dm_neo4j_person_keywords   select * from person_keywords")
    spark.sql("insert into table dm.dm_neo4j_project_keywords  select * from project_keywords")








  }

}
