package cn.sks.dwd.manual

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object Reward {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("wd_manual_excel_reward")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    spark.sql("select * from ods.o_manual_excel_reward where zh_title is not null").dropDuplicates("id")
      .createOrReplaceTempView("temp")

    val manual_excel_reward_person = spark.sql(
      """
        |
        |insert into dwd.wd_manual_excel_reward_person
        |select
        | md5(id)  as id
        |,trim(zh_title) as  zh_title
        |,trim(en_title) as  en_title
        |,publish_date
        |,session
        |,reward_level
        |,include_award
        |,reward_type
        |,reward_rank
        |,bonus_amount
        |,CleanFusion(zh_name)  as zh_name
        |,en_name
        |,gender
        |,avatar_url
        |,prof_title
        |,position
        |,CleanFusion(person_organization) as org_name
        |,research_area
        |,person_brief_description
        |,birthday
        |,is_dead
        |,dead_date
        |,reward_classification
        |,reward_reason
        |,person_achievements
        |,nominating_expert
        |,nominating_organization
        |,recommended_organization
        |,recommended_person
        |,issued_country
        |,issued_by
        |,source_title
        |,source
        |,last_update
        |from temp  where reward_type ='人'
        |""".stripMargin)

    val manual_excel_reward_team = spark.sql(
          """
            |insert into table dwd.wd_manual_excel_reward_team
            |select
            | md5(id)  as id
            |,trim(zh_title) as  zh_title
            |,trim(en_title) as  en_title
            |,publish_date
            |,session
            |,reward_level
            |,include_award
            |,reward_type
            |,reward_rank
            |,bonus_amount
            |,zh_name as team_name
            |,nominating_expert
            |,nominating_organization
            |,recommended_organization
            |,recommended_person
            |,issued_country
            |,issued_by
            |,source_title
            |,source
            |,last_update
            |from temp  where reward_type ='团队'
            |""".stripMargin)


    val manual_excel_reward_project = spark.sql(
      """
        | insert into table dwd.wd_manual_excel_reward_project
        |select
        | md5(id)  as id
        |,trim(zh_title) as  zh_title
        |,trim(en_title) as  en_title
        |,publish_date
        |,session
        |,reward_level
        |,include_award
        |,reward_type
        |,reward_rank
        |,bonus_amount
        |,trim(project_name) as project_name
        |,project_no
        |,project_introduction
        |,project_owner_type
        |,split(split(project_person,'；')[0],':')[1] as psn_name
        |,split(split(project_organization,',')[0],':')[1] as org_name
        |,nominating_expert
        |,nominating_organization
        |,recommended_organization
        |,recommended_person
        |,issued_country
        |,issued_by
        |,source_title
        |,source
        |,last_update
        |from temp  where reward_type ='项目'
        |""".stripMargin)







  }

}
