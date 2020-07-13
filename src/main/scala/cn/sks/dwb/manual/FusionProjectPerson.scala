package cn.sks.dwb.manual

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object FusionProjectPerson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("FusionProjectPerson")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()
    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    // 原始库中的人
    val person_origin =spark.sql(
      """
        |select CleanFusion(zh_name) as clean_zh_name,
        | CleanFusion(org_name) as clean_org_name,id as person_id
        | from dm.dm_neo4j_person
        |""".stripMargin)
    person_origin.createOrReplaceTempView("person_origin")
    //  excel 中的人
    val manual_person = spark.sql(
      """
        |select CleanFusion(project_person) as clean_zh_name,
        | CleanFusion(person_organization) as clean_org_name,*
        | from dwd.wd_manual_excel_project
        |""".stripMargin)
    manual_person.createOrReplaceTempView("manual_person")
    // 两部分人中的交集（人员对应关系）
    val rel_person= spark.sql(
      """
        | select a.person_id,a.id,person_organization as org_name,prof_title from (
        |    select * from (
        |        select  a.id ,b.person_id
        |        from  manual_person a join person_origin b
        |        on a.clean_zh_name=b.clean_zh_name and a.clean_org_name = b.clean_org_name
        |       )a  group by id,person_id
        |)a  left join manual_person b on a.id =b.id
      """.stripMargin)

    rel_person.createOrReplaceTempView("rel_person")
    // 一对一 （excel的一个人对应 原始库中的一个人）
    val rel_person_one = spark.sql(
      """
        |select * from rel_person where id in (
        |select id from rel_person group by id having count(id)<2)
      """.stripMargin)
    rel_person_one.createOrReplaceTempView("rel_person_one")
    // 一对多 （excel的一个人对应 原始库中的多个人）
    val rel_person_more = spark.sql(
      """
        |select * from rel_person where id in (
        |select id from rel_person group by id having count(id)>1)
      """.stripMargin)
    rel_person_more.createOrReplaceTempView("rel_person_more")

    //一对一 写入 hive库
    spark.sql("insert into table dwb.wb_person_manual_excel_project_person_rel_one   select * from rel_person_one")
    // 一对多 写入hive库 （为后续人工做准备）
    spark.sql("insert into table dwb.wb_person_manual_excel_project_person_rel_more  select * from rel_person_more")

    // 未融合的人（excel 中的人 在原始库中找不到对应关系）
    val person_not_exists = spark.sql("select id,project_person,person_organization,prof_title,classification from  manual_person a  where not exists (select * from rel_person b where a.id=b.id)")
        .toDF("id","zh_name","org_name","prof_title","classification")

    person_not_exists.createOrReplaceTempView("person_not_exists")
    spark.sql("insert into table dwb.wb_person_manual_excel_project_person_not_fusion   select * from person_not_exists")














  }

}
