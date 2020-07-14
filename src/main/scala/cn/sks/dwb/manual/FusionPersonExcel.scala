package cn.sks.dwb.manual

import cn.sks.util.{DefineUDF, PersonFusionUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FusionPersonExcel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("FusionPersonExcel")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val targetPerson= spark.sql("select id as person_id,zh_name,org_name from dm.dm_neo4j_person")


    // 1--outstanding
    val person_outstanding= spark.sql("select * from dwd.wd_manual_excel_outstanding")
    val tuple_outstanding: (DataFrame, DataFrame, DataFrame) = PersonFusionUtil.fusionPerson_Org(spark, person_outstanding, "id", targetPerson, "person_id")

    val rel_person_one_outstanding: DataFrame = tuple_outstanding._1.toDF("id","person_id")
    val rel_person_more_outstanding: DataFrame = tuple_outstanding._2.toDF("id","person_id")
    val rel_person_not_fusion_outstanding: DataFrame = tuple_outstanding._3

    val outstanding_one = rel_person_one_outstanding.join(person_outstanding,Seq("id"),"left")
        .select("person_id","id","outstanding_tittle","outstanding_level","include_outstanding","publish_date","batches")
    val outstanding_more = rel_person_more_outstanding.join(person_outstanding,Seq("id"),"left")
      .select("person_id","id","outstanding_tittle","outstanding_level","include_outstanding","publish_date","batches")


//  2--  GuideCompilationExpert
    val person_guide= spark.sql("select * from dwd.wd_manual_excel_guide_compilation_expert")
    val tuple_guide = PersonFusionUtil.fusionPerson_Org(spark, person_guide, "id", targetPerson, "person_id")

    val rel_person_one_guide: DataFrame = tuple_guide._1.toDF("id","person_id")
    val rel_person_more_guide: DataFrame = tuple_guide._2.toDF("id","person_id")
    val rel_person_not_fusion_guide: DataFrame = tuple_guide._3

    val guide_one = rel_person_one_guide.join(person_guide,Seq("id"),"left")
      .select("person_id","id","guide_name","publish_department","guide_year","prof_title")
    val guide_more = rel_person_more_guide.join(person_guide,Seq("id"),"left")
      .select("person_id","id","guide_name","publish_department","guide_year","prof_title")


    // 3--- ReviewExpert
    val person_review= spark.sql("select * from dwd.wd_manual_excel_review_expert")
    val tuple_review = PersonFusionUtil.fusionPerson_Org(spark, person_review, "id", targetPerson, "person_id")

    val rel_person_one_review: DataFrame = tuple_review._1.toDF("id","person_id")
    val rel_person_more_review: DataFrame = tuple_review._2.toDF("id","person_id")
    val rel_person_not_fusion_review: DataFrame = tuple_review._3

    val review_one = rel_person_one_review.join(person_review,Seq("id"),"left")
      .select("person_id","id","special_name","special_type","review_type","review_group","review_rounds","review_year")
    val review_more = rel_person_more_review.join(person_review,Seq("id"),"left")
      .select("person_id","id","special_name","special_type","review_type","review_group","review_rounds","review_year")


    // 4--- reward (person_reward,project_reward)
    val person_reward = spark.sql(
      """
        |select id,zh_name,en_name,org_name,
        |  zh_title,en_title,publish_date,session,reward_level,include_award,reward_rank,
        |   'reward_person'  as source  from dwd.wd_manual_excel_reward_person
        |  union
        |select id,zh_name,null as en_name,org_name,
        |   zh_title,en_title,publish_date,session,reward_level,include_award,reward_rank,
        |   'reward_project' as source  from dwd.wd_manual_excel_reward_project
      """.stripMargin)

    val tuple_reward = PersonFusionUtil.fusionPerson_Org(spark, person_reward, "id", targetPerson, "person_id")

    val rel_person_one_reward: DataFrame = tuple_reward._1.toDF("id","person_id")
    val rel_person_more_reward: DataFrame = tuple_reward._2.toDF("id","person_id")
    val rel_person_not_fusion_reward: DataFrame = tuple_reward._3

    val reward_one = rel_person_one_reward.join(person_reward,Seq("id"),"left")
      .select("person_id","id","zh_title","en_title","publish_date","session","reward_level","include_award","reward_rank","source")
    val reward_more = rel_person_more_reward.join(person_reward,Seq("id"),"left")
      .select("person_id","id","zh_title","en_title","publish_date","session","reward_level","include_award","reward_rank","source")



    // 5-- ProjectPerson
    val person_project= spark.sql("select * from dwd.wd_manual_excel_project")
    val tuple_project = PersonFusionUtil.fusionPerson_Org(spark, person_project, "id", targetPerson, "person_id")

    val rel_person_one_project: DataFrame = tuple_project._1.toDF("id","person_id")
    val rel_person_more_project: DataFrame = tuple_project._2.toDF("id","person_id")
    val rel_person_not_fusion_project: DataFrame = tuple_project._3

    val project_one = rel_person_one_project.join(person_project,Seq("id"),"left")
      .select("person_id","id","org_name","prof_title")
    val project_more = rel_person_more_project.join(person_project,Seq("id"),"left")
      .select("person_id","id","org_name","prof_title")


    rel_person_not_fusion_outstanding.createOrReplaceTempView("rel_person_not_fusion_outstanding")
    rel_person_not_fusion_guide.createOrReplaceTempView("rel_person_not_fusion_guide")
    rel_person_not_fusion_review.createOrReplaceTempView("rel_person_not_fusion_review")
    rel_person_not_fusion_reward.createOrReplaceTempView("rel_person_not_fusion_reward")
    rel_person_not_fusion_project.select("id","zh_name","org_name","prof_title","classification").createOrReplaceTempView("rel_person_not_fusion_project")

    spark.sql("insert into table dwb.wb_person_excel_outstanding_person_not_fusion    select * from rel_person_not_fusion_outstanding")
    spark.sql("insert into table dwb.wb_person_excel_guide_compilation_expert_person_not_fusion   select * from rel_person_not_fusion_guide")
    spark.sql("insert into table dwb.wb_person_excel_review_expert_person_not_fusion   select * from rel_person_not_fusion_review")
    spark.sql("insert into table dwb.wb_person_excel_reward_person_not_fusion          select * from rel_person_not_fusion_reward")
    spark.sql("insert into table dwb.wb_person_excel_project_person_not_fusion         select * from rel_person_not_fusion_project")

    outstanding_one   .createOrReplaceTempView("outstanding_one")
    outstanding_more  .createOrReplaceTempView("outstanding_more")
    guide_one         .createOrReplaceTempView("guide_one")
    guide_more        .createOrReplaceTempView("guide_more")
    review_one        .createOrReplaceTempView("review_one")
    review_more       .createOrReplaceTempView("review_more")
    reward_one        .createOrReplaceTempView("reward_one")
    reward_more       .createOrReplaceTempView("reward_more")
    project_one       .createOrReplaceTempView("project_one")
    project_more      .createOrReplaceTempView("project_more")


    spark.sql("insert into table dwb.wb_person_excel_outstanding_person_rel_one   select * from outstanding_one")
    spark.sql("insert into table dwb.wb_person_excel_outstanding_person_rel_more  select * from outstanding_more")

    spark.sql("insert into table dwb.wb_person_excel_guide_compilation_expert_person_rel_one   select * from guide_one")
    spark.sql("insert into table dwb.wb_person_excel_guide_compilation_expert_person_rel_more  select * from guide_more")

    spark.sql("insert into table dwb.wb_person_excel_review_expert_person_rel_one   select * from review_one")
    spark.sql("insert into table dwb.wb_person_excel_review_expert_person_rel_more  select * from review_more")

    spark.sql("insert into table dwb.wb_person_excel_reward_person_rel_one   select * from reward_one")
    spark.sql("insert into table dwb.wb_person_excel_reward_person_rel_more  select * from reward_more")

    spark.sql("insert into table dwb.wb_person_excel_project_person_rel_one   select * from project_one")
    spark.sql("insert into table dwb.wb_person_excel_project_person_rel_more  select * from project_more")



  }

}
