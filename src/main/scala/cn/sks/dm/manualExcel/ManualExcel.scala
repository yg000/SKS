package cn.sks.dm.manualExcel

import org.apache.spark.sql.SparkSession

object ManualExcel {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("ManualExcel")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()
    // 1---- 奖励
    val excel_reward = spark.sql("select md5(zh_title) as id,zh_title,en_title,issued_by from ods.o_manual_excel_reward").dropDuplicates("zh_title")
    // 人与奖励
    val excel_person_reward=spark.sql(
      """
        |select person_id,md5(zh_title) as reward_id,publish_date,session,reward_level,include_award,reward_rank
        |from dwb.wb_person_manual_excel_reward_person_rel_one
        |""".stripMargin)
    //  团队与奖励
    val excel_team_reward=spark.sql(
      """
        |select md5(team_name) as team_id,md5(zh_title) as reward_id,publish_date,session,reward_level,include_award,reward_rank
        |from dwd.wd_manual_excel_reward_team
        |""".stripMargin)



    println("----------")
    while (true){

    }



    //  项目与奖励

    //2---- 专项
    // 评审专家中的专项  -- -指南编制专家中的专项
    val excel_special_project=spark.sql("select md5(special_name) as id, special_name,special_type,'国家级' as special_level,publish_department from ods.o_manual_excel_review_expert").union(
    spark.sql("select md5(special_name) as id,special_name,special_type,'国家级' as special_level,publish_department from ods.o_manual_excel_guide_compilation_expert")).union(
      spark.sql("select md5(special_name) as id,special_name,special_type,special_level,publish_department from dwd.wd_manual_excel_project")
    ).dropDuplicates("special_name")
    // 人评审专项
    val excel_person_special_project = spark.sql(
      """
        |select  person_id,md5(special_name) as special_project_id,review_type,review_group,review_rounds,review_year
        |from dwb.wb_person_manual_excel_review_expert_person_rel_one
        |""".stripMargin)
    //3---- 人才
    val excel_outstanding = spark.sql("select md5(outstanding_tittle) as id,outstanding_tittle,outstanding_level,issued_by from ods.o_manual_excel_outstanding").dropDuplicates("outstanding_tittle")
    // 人与杰出人才
    val excel_person_outstanding= spark.sql(
      """
        |select person_id ,md5(outstanding_tittle) as outstanding_id,outstanding_level,include_outstanding,publish_date,batches
        |from dwb.wb_person_manual_excel_outstanding_person_rel_one
        |""".stripMargin)

    //4---- 指南编制专家
    val excel_guide= spark.sql("select md5(guide_name) as id,guide_name,publish_department from ods.o_manual_excel_guide_compilation_expert").dropDuplicates("guide_name")
    // 指南与专项
    val excel_guide_special_project= spark.sql("select md5(guide_name) as guide_id,md5(special_name) as special_project_id  from ods.o_manual_excel_guide_compilation_expert")
      .dropDuplicates("guide_id","special_project_id")

    // 人与指南
    val excel_person_guide = spark.sql(
      """
        |select  person_id,md5(guide_name) as guide_id,guide_year
        |from dwb.wb_person_manual_excel_guide_compilation_expert_person_rel_one
        |""".stripMargin)



    println(excel_team_reward           .count())
    println(excel_reward                .count())
    println(excel_person_reward         .count())
    println(excel_special_project       .count())
    println(excel_person_special_project.count())
    println(excel_outstanding           .count())
    println(excel_person_outstanding    .count())
    println(excel_guide                 .count())
    println(excel_guide_special_project .count())
    println(excel_person_guide          .count())



    excel_reward.createOrReplaceTempView("excel_reward")
    excel_person_reward.createOrReplaceTempView("excel_person_reward")
    excel_special_project.createOrReplaceTempView("excel_special_project")
    excel_person_special_project.createOrReplaceTempView("excel_person_special_project")
    excel_outstanding.createOrReplaceTempView("excel_outstanding")
    excel_person_outstanding.createOrReplaceTempView("excel_person_outstanding")
    excel_guide.createOrReplaceTempView("excel_guide")
    excel_guide_special_project.createOrReplaceTempView("excel_guide_special_project")
    excel_person_guide.createOrReplaceTempView("excel_person_guide")
    excel_team_reward.createOrReplaceTempView("excel_team_reward")





    spark.sql("insert into table dm.dm_neo4j_manual_excel_team_reward            select * from excel_team_reward         ")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_reward                   select * from excel_reward                ")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_person_reward            select * from excel_person_reward         ")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_special_project          select * from excel_special_project       ")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_person_special_project   select * from excel_person_special_project")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_outstanding              select * from excel_outstanding           ")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_person_outstanding       select * from excel_person_outstanding    ")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_guide                    select * from excel_guide                 ")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_guide_special_project    select * from excel_guide_special_project ")
    spark.sql("insert into table dm.dm_neo4j_manual_excel_person_guide             select * from excel_person_guide          ")





  }

}
