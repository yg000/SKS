package cn.sks.dwd.manual

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object ManualExcelDate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("ManualExcelDate")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })

    // 1-- o_manual_excel_outstanding
    spark.sql("select * from ods.o_manual_excel_outstanding").dropDuplicates("id")
        .createOrReplaceTempView("temp_outstanding")
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
        |from temp_outstanding where outstanding_type = "个人"
      """.stripMargin)


    // 2-- o_manual_excel_guide_compilation_expert
    spark.sql("select * from ods.o_manual_excel_guide_compilation_expert").dropDuplicates("id")
      .createOrReplaceTempView("temp_guide")
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
        |from temp_guide
      """.stripMargin)

    // 3-- o_manual_excel_review_expert
    spark.sql("select * from ods.o_manual_excel_review_expert").dropDuplicates("id")
      .createOrReplaceTempView("temp_review")
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
        |from temp_review
      """.stripMargin)

    //4 --o_manual_excel_project
    spark.sql("select * from ods.o_manual_excel_project").dropDuplicates("id")
      .createOrReplaceTempView("temp_project")

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
        |,project_person  as     zh_name
        |,person_organization as org_name
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
        |from temp_project
      """.stripMargin)

    // 5 --  o_manual_excel_reward
    spark.sql("select * from ods.o_manual_excel_reward where zh_title is not null").dropDuplicates("id")
      .createOrReplaceTempView("temp_reward")

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
        |from temp_reward  where reward_type ='人'
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
        |from temp_reward  where reward_type ='团队'
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
        |,split(split(project_person,'；')[0],':')[1]      as zh_name
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
        |from temp_reward  where reward_type ='项目'
        |""".stripMargin)









  }

}
