package cn.sks.dwb.manual

import cn.sks.util.DefineUDF
import org.apache.spark.sql.{DataFrame, SparkSession}

object FusionProject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("FusionProject")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()
    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })

    spark.sqlContext.udf.register("CleanDate",(str:String) =>{
      if (str==null)  null
      else {
        val builder = new StringBuffer(str)
        if(str.length==8) builder.insert(4,"-").insert(7,"-").toString
        if(str.length==4) builder.append("-01-01").toString
        else  str
      }
    })

    // 项目融合（人工excel中的项目：wd_manual_excel_project  以及 奖励中的项目：wd_manual_excel_reward_project ）
    // 1、人工excel 项目
    val manual_project = spark.sql(
        """
          |select id,
          |project_name as zh_title,
          |project_no as prj_no ,
          |zh_name as psn_name,
          | org_name,
          |project_year as approval_year,
          |CleanDate(start_date) as start_date,
          |CleanDate(end_date) as end_date,
          |CleanFusion(project_name) as clean_zh_title
          | from dwd.wd_manual_excel_project
        """.stripMargin)
    manual_project.createOrReplaceTempView("manual_project")

    // 基金委项目
    val project_nsfc = spark.sql("select * from dwd.wd_project_nsfc")
    project_nsfc.createOrReplaceTempView("project_nsfc")
    val origin_project = spark.sql("select project_id,zh_title,CleanFusion(zh_title) as clean_zh_title from project_nsfc")
    origin_project.createOrReplaceTempView("origin_project")

    // 项目交集（对应关系）
    val rel_person_project= spark.sql(
      """
        | select * from (
        |     select  a.id ,b.project_id
        |     from  manual_project a join origin_project b
        |     on a.clean_zh_title=b.clean_zh_title
        |    )a  group by id,project_id
      """.stripMargin)
    rel_person_project.createOrReplaceTempView("rel_person_project")
    // excel 项目不存在于基金委中的项目
    val person_project_not_exists = spark.sql("select md5(clean_zh_title) as project_id,* from manual_project a  where not exists (select * from rel_person_project b where a.id=b.id)")
        .drop("clean_zh_title").dropDuplicates("project_id")
    person_project_not_exists.createOrReplaceTempView("person_project_not_exists")

    // 给不能融合的项目 添加 person_id （关联项目中的人员融合表）
    val person_project_not_exists_replace_person_id = spark.sql(
      """
        |select if(person_id is null ,a.id,person_id) as person_id,a.*
        | from  person_project_not_exists a
        |  left join dwb.wb_person_excel_project_person_rel_one b
        |  on a.id =b.id
      """.stripMargin).drop("id")

    // 合并 基金委项目，与不能融合的项目
    val project_temp = completionFields(spark,person_project_not_exists_replace_person_id,project_nsfc).union(project_nsfc)
    project_temp.createOrReplaceTempView("project_temp")
    spark.sql("select *,CleanFusion(zh_title) as clean_zh_title from project_temp").createOrReplaceTempView("origin_project_temp")


    // 2、融合来源于奖励中的项目  （来源于 wd_manual_excel_reward_project）
    val manual_reward_project = spark.sql(
      """
        |select
        |a.id,
        |project_name as zh_title,
        |project_no as prj_no,
        |if(person_id is null ,a.id,person_id) as person_id,
        |zh_name as psn_name,
        |org_name,
        |CleanFusion(project_name) as clean_zh_title
        |from dwd.wd_manual_excel_reward_project  a
        |left join dwb.wb_person_excel_reward_person_rel_one b
        |on a.id=b.id
      """.stripMargin)
    manual_reward_project.createOrReplaceTempView("manual_reward_project")
    // 以 origin_project_temp 为基准，根据zh_title 融合奖励中的项目
    val rel_reward_project= spark.sql(
      """
        | select * from (
        |     select  a.id ,b.project_id
        |     from  manual_reward_project a join origin_project_temp b
        |     on a.clean_zh_title=b.clean_zh_title
        |    )a  group by id,project_id
      """.stripMargin)
    rel_reward_project.createOrReplaceTempView("rel_reward_project")

    val reward_project_not_exists = spark.sql("select md5(clean_zh_title) as project_id,* from manual_reward_project a  where not exists (select * from rel_reward_project b where a.id=b.id)")
      .drop("id").drop("clean_zh_title").dropDuplicates("project_id")

    val wb_project = completionFields(spark,reward_project_not_exists,project_nsfc).union(project_temp)

    // excel 与基金委项目之间的对应 的关系
    val wb_manual_excel_project_nsfc_rel = rel_person_project.union(rel_reward_project).toDF("id","project_id_nsfc")
    wb_manual_excel_project_nsfc_rel.createOrReplaceTempView("wb_manual_excel_project_nsfc_rel")

    // 项目与奖励 的关系
    val wb_project_reward = spark.sql(
      """
        |select
        | if(b.project_id_nsfc is null,md5(CleanFusion(project_name)),project_id_nsfc) as project_id,
        | md5(zh_title) as reward_id ,
        | publish_date ,session,reward_level,include_award,reward_rank
        |from dwd.wd_manual_excel_reward_project a
        |left join wb_manual_excel_project_nsfc_rel b
        |on a.id=b.id
      """.stripMargin)
    // 项目与专项 的关系
    val wb_project_special_project =spark.sql(
      """
        |select
        | if(b.project_id_nsfc is null,md5(CleanFusion(project_name)),project_id_nsfc) as project_id,
        | md5(special_name) as special_project_id
        | from dwd.wd_manual_excel_project a
        | left join wb_manual_excel_project_nsfc_rel b
        | on a.id=b.id
      """.stripMargin)

    // 项目与人 的关系 (participation)
    val project_person_participation = spark.sql(
      """
        |select md5(a.prj_code) as project_id,a.person_id,a.org_name
        |from ods.o_nsfc_project_person a
        | where  exists (select * from project_nsfc b where md5(a.prj_code)=b.project_id)
      """.stripMargin)
    // 项目与单位 的关系(lead)
    val project_org_lead= BuildOrgIDUtil.buildOrganizationID(spark,wb_project,"org_name","dwb.wb_organization_project")
      .select("project_id","org_id").show()
    // 项目与单位 的关系（participation）
    val project_org_participation= BuildOrgIDUtil.buildOrganizationID(spark,project_person_participation,"org_name","dwb.wb_organization_project")
      .select("project_id","org_id").show()

    wb_project.createOrReplaceTempView("wb_project")
    project_person_participation.createOrReplaceTempView("project_person_participation")
    wb_project_reward.createOrReplaceTempView("wb_project_reward")
    wb_project_special_project.createOrReplaceTempView("wb_project_special_project")

    wb_project.show(5)
    wb_project_reward.show(5)
    wb_project_special_project.show(5)
    project_person_participation.show(5)


//    spark.sql("insert into table dwb.wb_manual_excel_project_nsfc_rel select * from wb_manual_excel_project_nsfc_rel")
//
//    spark.sql("insert into table dwb.wb_project select * from wb_project")
//    spark.sql("insert into table dwb.wb_project_reward select * from wb_project_reward")
//
//    spark.sql("insert into table dwb.wb_project_special_project select * from wb_project_special_project")
//
//    spark.sql("insert into table dwb.wb_project_person_lead            select project_id,person_id from wb_project")
//    spark.sql("insert into table dwb.wb_project_person_participation   select  project_id,person_id from project_person_participation")





  }

   // 根据目标表自动校准 schema ，缺少字段，自动补空
  def completionFields(spark:SparkSession,originDF:DataFrame,targetTable:DataFrame):DataFrame = {
    var origin =originDF
    val origin_list = origin.schema.fieldNames.toList
    val target_list: List[String] = targetTable.schema.fieldNames.toList

    val diff_list = target_list diff origin_list
    import org.apache.spark.sql._
    val udf_null = functions.udf(()=> null )
    diff_list.foreach(x=>{
      origin=origin.withColumn(x,udf_null())
    })

    val build = new StringBuilder
    build.append("select  ")
    targetTable.schema.fieldNames.foreach(x=>{
      build.append(x+",")
    })

    origin.createOrReplaceTempView("temp")
    spark.sql(build.toString().stripSuffix(",") +  "  from temp")
  }

}







