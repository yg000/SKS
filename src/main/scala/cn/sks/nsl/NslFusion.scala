package cn.sks.nsl

import cn.sks.util.PersonFusionUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.junit.Test

object NslFusion {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("product_keywords_nsfc_clean_translate")
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "32g")
    .config("spark.cores.max", "8")
    .config("spark.rpc.askTimeout", "300")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    .config("spark.debug.maxToStringFields", "200")
    .config("spark.driver.maxResultSize", "4G")
    .config("sethive.enforce.bucketing", "true")
    .config("spark.cross.dc.inputs.location.prefix.substitute.enabled", "false")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  spark.sql(
    """
      |select * from dm.dm_neo4j_product_person_conference union
      |select * from dm.dm_neo4j_product_person_criterion union
      |select * from dm.dm_neo4j_product_person_journal union
      |select * from dm.dm_neo4j_product_person_monograph union
      |select * from dm.dm_neo4j_product_person_patent
    """.stripMargin).createOrReplaceTempView("person_achievement_rel")

  def main(args: Array[String]): Unit = {

//      clean_person_advisor_children()
        fusion_teacher()
//        fusion_student()
//      fusison_not_exists()
//      fusison_one2more()

  }
  def fusion_student():Unit={

    val originDF = spark.sql(
      """
        |select psn_code,name as zh_name,org_name from(
        |select * from dwd.wd_person_json a where
        |exists( select * from dwd.wd_person_children_json b where a.name=b.name and a.org_name=b.org_name))c
      """.stripMargin)

    val targetDF = spark.sql("select * from dm.dm_neo4j_person")
    //解析的人在总人表 一对一和一对多
    PersonFusionUtil.fusionPerson_Org(spark, originDF, "psn_code", targetDF, "id")._1
      .union(PersonFusionUtil.fusionPerson_Org(spark, originDF, "psn_code", targetDF, "id")._2)
      .createOrReplaceTempView("person_rel")
    //    spark.sql("insert overwrite table dwb.wb_person_rel select * from person_rel")
    //    spark.sql("select count(*) from person_rel").show()  //一对一7504    所有 28961

    originDF.createOrReplaceTempView("person_exists_student")
    spark.sql("insert overwrite table dwb.wb_person_exists_student select * from person_exists_student")


    spark.sql(
      """
        |select b.targetID,a.zh_name as name,a.org_name from person_exists_student a
        | left join person_rel b on a.psn_code=b.originID where b.targetID is not null
      """.stripMargin).createOrReplaceTempView("wd_person_json_replace")

    //    spark.sql("insert overwrite table dwb.wb_person select * from wd_person_json_replace")
    println("-------------------------")
    //advisor  解析的学生在总人表一对一和一对多
    val originteacherDF = spark.sql("select children_id,children_name as zh_name,children_org_name as org_name,children_year from dwd.wd_person_children_json")
    PersonFusionUtil.fusionPerson_Org(spark, originteacherDF, "children_id", targetDF, "id")._1
      .union(PersonFusionUtil.fusionPerson_Org(spark, originteacherDF, "children_id", targetDF, "id")._2)
      .createOrReplaceTempView("student_rel")
    //    spark.sql("select count(*) from teacher_rel").show()           //一对一3331 所有9690
    //    spark.sql("insert overwrite table dwb.wb_teacher_rel select * from teacher_rel")



    //学生替换person_id
    println("==============替换后的学生数据量============")
    spark.sql(
      """
        |select
        | b.targetID,
        | a.name,
        | a.year,
        | a.children_name,
        | a.children_year,
        | a.children_attr_id,
        | a.org_name,
        | a.children_org_name,
        | 'advisor' as relation
        | from dwd.wd_person_children_json a
        | left join student_rel b on a.children_id=b.originID where b.targetID is not null
      """.stripMargin).createOrReplaceTempView("wd_person_student_json_replace")
    //    spark.sql("select count(*) from wd_person_teacher_json_replace").show()      //teacher 21700
    //    spark.sql("insert overwrite table dwb.wb_person_advisor select * from wd_person_teacher_json_replace")

    //    spark.sql(
    //      """
    //        |
    //        |select * from person_achievement_rel a where
    //        |exists(select * from wd_person_teacher_json_replace b where b.targetID=a.person_id)
    //      """.stripMargin).createOrReplaceTempView("advisor_achievement")

    println("=====人的person_id+学生表=====")
    spark.sql(
      """
        |select
        | b.targetID as person_id,
        | a.targetID as student_person_id,
        | a.name,
        | a.year,
        | a.children_name,
        | a.children_year,
        | a.children_attr_id,
        | a.org_name,
        | a.children_org_name from wd_person_student_json_replace a left join
        | wd_person_json_replace b on a.name=b.name and a.org_name=b.org_name
      """.stripMargin).createOrReplaceTempView("person_id_student")

//    spark.sql("select count(*) from person_id_student").show()         //266218      所有607049
    //    spark.sql("insert overwrite table dwb.wb_advisor_achievement_rel select * from advisor_achievement")

    println("============人和成果的关系==========")

    spark.sql(
      """
        |select
        | a.person_id,
        | a.student_person_id,
        | a.name,
        | a.year,
        | a.children_name,
        | a.children_year,
        | a.children_attr_id,
        | a.org_name,
        | a.children_org_name,
        | b.achievement_id from person_id_student a left join dwb.wb_person_achievement_rel b
        |on a.person_id=b.person_id
      """.stripMargin).createOrReplaceTempView("person_achievement")//721177

//    spark.sql("select count(*) from person_achievement").show()
    //    spark.sql("insert overwrite table dwb.wb_person_achievement_rel_exists_advisor select * from person_achievement")

    println("===============学生和成果的关系============")
    val student_achievement=spark.sql(
      """
        |select
        | a.person_id,
        | a.student_person_id,
        | a.name,
        | a.year,
        | a.children_name,
        | a.children_year,
        | a.children_attr_id,
        | a.org_name,
        | a.children_org_name,
        | a.achievement_id,
        | b.achievement_id as student_achievement_id
        |  from person_achievement a left join dwb.wb_person_achievement_rel b
        | on b.person_id=a.student_person_id
      """.stripMargin)
    student_achievement.repartition(50).write.format("hive")
      .mode("overwrite").insertInto("dwb.wb_person_student_achievement")

    println("--------------------student ending------------------")

//    println("============人和学生的关系==========")
//    val person_student_rel=spark.sql(
//      """
//        |select person_id,student_person_id,children_year from student_achievement
//        |where achievement_id=student_achievement_id group by person_id,student_person_id,children_year
//      """.stripMargin)
//    person_student_rel.createOrReplaceTempView("person_student_rel")
//    //    person_student_rel.repartition(50).write.format("hive").mode("overwrite").insertInto("dwb.wb_person_children_rel")
//
//    println("=============补充人和学生的关系==============")
//    spark.sql(
//      """
//        |select
//        | person_id,
//        | student_person_id,
//        | name,
//        | year,
//        | children_name,
//        | children_year,
//        | children_attr_id,
//        | org_name,
//        | children_org_name from student_achievement
//        |where achievement_id =student_achievement_id
//        |group by person_id,
//        | student_person_id,
//        | name,
//        | year,
//        | children_name,
//        | children_year,
//        | children_attr_id,
//        | org_name,
//        | children_org_name
//      """.stripMargin).createOrReplaceTempView("person_student")
////    spark.sql("select count(*) from person_student ").show()
//
//
//    val person_student_not_exists=spark.sql(
//      """
//        |select * from dwd.wd_person_children_json a
//        |where not exists(select * from person_student b
//        | where a.children_name=b.children_name and
//        |       a.children_org_name =b.children_org_name and
//        |       a.children_attr_id=b.children_attr_id and
//        |       a.name=b.name and
//        |       a.org_name=b.org_name)
//      """.stripMargin)
//    person_student_not_exists.createOrReplaceTempView("person_student_not_exists")
//
//
//
//    person_student_not_exists.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    person_student_not_exists.repartition(20).write.
//      format("hive").mode("overwrite").insertInto("dwb.wb_person_children_nsl")
//
//    println("=======不存在的人的学生的关系========")
//    val person_student_not_exists_rel=spark.sql(
//      """
//        |select b.person_id,a.children_id,a.name,a.children_year from person_student_not_exists a
//        | left join person_student b
//        | on a.name=b.name and a.org_name=b.org_name where b.person_id is not null group by b.person_id,a.children_id,a.children_year,a.name
//      """.stripMargin)
//    person_student_not_exists.createOrReplaceTempView("person_student_not_exists_rel")
//
//
//    println("=======不存在的人和学生的关系========")
//    spark.sql(
//      """
//        |select b.psn_code,* from person_student_not_exists a
//        |left join person_exists_student b on a.name=b.name and a.org_name=b.org_name
//      """.stripMargin).createOrReplaceTempView("psn_code_student")
//
//    val person_nsl=spark.sql(
//      """
//        |select * from psn_code_student a where not exists
//        |(select * from person_student_not_exists_rel d where c.children_id=d.children_id and c.name=d.name)
//      """.stripMargin)
//    person_nsl.createOrReplaceTempView("person_nsl")
//
//    person_student_not_exists_rel.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    person_student_not_exists_rel.drop("name").repartition(20).write.
//      format("hive").mode("overwrite").insertInto("dwb.wb_person_children_nsl_rel")
//
//    spark.sql("insert into table dwb.wb_person_nsl select psn_code,name,org_name,'nsl' as source from person_nsl")
//    spark.sql("insert into table dwb.wb_person_children_nsl_rel select psn_code,children_id from person_nsl group by psn_code,children_id")



    //    spark.sql("select count(*) from person_advisor_rel").show()

  }


  def fusion_teacher():Unit={

    //person
    val originDF = spark.sql(
  """
    |select psn_code,name as zh_name,org_name from(
    |select * from dwd.wd_person_json a where
    |exists( select * from dwd.wd_person_advisor_json b where a.name=b.name and a.org_name=b.org_name))c
  """.stripMargin)

    val targetDF = spark.sql("select * from dm.dm_neo4j_person")
    //解析的人在总人表 一对一和一对多
    PersonFusionUtil.fusionPerson_Org(spark, originDF, "psn_code", targetDF, "id")._1
    .union(PersonFusionUtil.fusionPerson_Org(spark, originDF, "psn_code", targetDF, "id")._2)
      .createOrReplaceTempView("person_rel")
//    spark.sql("insert overwrite table dwb.wb_person_rel select * from person_rel")
//    spark.sql("select count(*) from person_rel").show()  //一对一7504    所有 28961

    originDF.createOrReplaceTempView("person_exists_advisor")
    spark.sql("insert overwrite table dwb.wb_person_exists_advisor select * from person_exists_advisor")
//    println("===============")


    spark.sql(
      """
        |select b.targetID,a.zh_name as name,a.org_name from person_exists_advisor a
        | left join person_rel b on a.psn_code=b.originID where b.targetID is not null
      """.stripMargin).createOrReplaceTempView("wd_person_json_replace")

//    spark.sql("insert overwrite table dwb.wb_person select * from wd_person_json_replace")
    println("-------------------------")
    //advisor  解析的老师在总人表一对一和一对多
    val originteacherDF = spark.sql("select children_id,children_name as zh_name,children_org_name as org_name from dwd.wd_person_advisor_json")
    PersonFusionUtil.fusionPerson_Org(spark, originteacherDF, "children_id", targetDF, "id")._1
      .union(PersonFusionUtil.fusionPerson_Org(spark, originteacherDF, "children_id", targetDF, "id")._2)
      .createOrReplaceTempView("teacher_rel")
//    spark.sql("select count(*) from teacher_rel").show()           //一对一3331 所有9690
//    spark.sql("insert overwrite table dwb.wb_teacher_rel select * from teacher_rel")


    //导师替换person_id
    println("==============替换后的导师数据量============")
    spark.sql(
      """
        |select
        | b.targetID,
        | a.name,
        | a.year,
        | a.children_name,
        | a.children_year,
        | a.children_attr_id,
        | a.org_name,
        | a.children_org_name,
        | 'advisor' as relation
        | from dwd.wd_person_advisor_json a
        | left join teacher_rel b on a.children_id=b.originID where b.targetID is not null
      """.stripMargin).createOrReplaceTempView("wd_person_teacher_json_replace")
//    spark.sql("select count(*) from wd_person_teacher_json_replace").show()      //teacher 21700
//    spark.sql("insert overwrite table dwb.wb_person_advisor select * from wd_person_teacher_json_replace")

//    spark.sql(
//      """
//        |
//        |select * from person_achievement_rel a where
//        |exists(select * from wd_person_teacher_json_replace b where b.targetID=a.person_id)
//      """.stripMargin).createOrReplaceTempView("advisor_achievement")

    println("=====人的person_id+老师表=====")
    spark.sql(
      """
        |select
        | b.targetID as person_id,
        | a.targetID as teacher_person_id,
        | a.name,
        | a.year,
        | a.children_name,
        | a.children_year,
        | a.children_attr_id,
        | a.org_name,
        | a.children_org_name from wd_person_teacher_json_replace a left join
        | wd_person_json_replace b on a.name=b.name and a.org_name=b.org_name
      """.stripMargin).createOrReplaceTempView("person_id_advisor")

//    spark.sql("select count(*) from person_id_advisor").show()         //266218      所有607049
//    spark.sql("insert overwrite table dwb.wb_advisor_achievement_rel select * from advisor_achievement")

    println("============人和成果的关系==========")

    spark.sql(
      """
        |select
        | a.person_id,
        | a.teacher_person_id,
        | a.name,
        | a.year,
        | a.children_name,
        | a.children_year,
        | a.children_attr_id,
        | a.org_name,
        | a.children_org_name,
        | b.achievement_id from person_id_advisor a left join dwb.wb_person_achievement_rel b
        |on a.person_id=b.person_id
      """.stripMargin).createOrReplaceTempView("person_achievement")//721177

//    spark.sql("select count(*) from person_achievement").show()   //848286          所有721177
//    spark.sql("insert overwrite table dwb.wb_person_achievement_rel_exists_advisor select * from person_achievement")

    println("===============老师和成果的关系============")
    val advisor_achievement=spark.sql(
      """
        |select
        | a.person_id,
        | a.teacher_person_id,
        | a.name,
        | a.year,
        | a.children_name,
        | a.children_year,
        | a.children_attr_id,
        | a.org_name,
        | a.children_org_name,
        | a.achievement_id,
        | b.achievement_id as teacher_achievement_id
        |  from person_achievement a left join dwb.wb_person_achievement_rel b
        | on b.person_id=a.teacher_person_id
      """.stripMargin)
    advisor_achievement.repartition(20).write.format("hive")
        .mode("overwrite").insertInto("dwb.wb_person_advisor_achievement")
    println("--------------------teacher ending------------------")

//    println("============人和导师的关系==========")
//
////    val person_advisor_rel= spark.sql(
////      """
////        |select person_id,teacher_person_id,year from advisor_achievement where achievement_id=teacher_achievement_id group by person_id,teacher_person_id,year
////      """.stripMargin)
////    //    spark.sql("select count(*) from person_advisor_rel").show()
////
////    person_advisor_rel.repartition(50).write.format("hive").mode("overwrite").insertInto("dwb.wb_person_advisor_rel")
//
//    println("============补充人和导师的关系==========")
//    spark.sql(
//      """
//        |select
//        | person_id,
//        | teacher_person_id,
//        | name,
//        | year,
//        | children_name,
//        | children_year,
//        | children_attr_id,
//        | org_name,
//        | children_org_name from advisor_achievement
//        |where achievement_id =teacher_achievement_id
//        |group by person_id,
//        | teacher_person_id,
//        | name,
//        | year,
//        | children_name,
//        | children_year,
//        | children_attr_id,
//        | org_name,
//        | children_org_name
//      """.stripMargin).createOrReplaceTempView("person_teacher")
//    //    spark.sql("select count(*) from person_student ").show()
//
////    spark.sql("select * from person_teacher where children_name='洪德元' and children_org_name='中国科学院植物研究所'").show()
////    while(true){}
//
//    val person_teacher_not_exists=spark.sql(
//      """
//        |select * from dwd.wd_person_advisor_json a
//        |where not exists(select * from person_teacher b
//        | where a.children_name=b.children_name and
//        |       a.children_org_name =b.children_org_name and
//        |       a.children_attr_id=b.children_attr_id and
//        |       a.name=b.name and
//        |       a.org_name=b.org_name)
//      """.stripMargin)
//    person_teacher_not_exists.createOrReplaceTempView("person_teacher_not_exists")
//
////
////    person_teacher_not_exists.repartition(20).write.
////      format("hive").mode("overwrite").insertInto("dwb.wb_person_advisor_nsl")
//
//    println("=======不存在的人的导师的关系========")
//    val person_teacher_not_exists_rel=spark.sql(
//      """
//        |select b.person_id,a.children_id,a.year,a.name from person_teacher_not_exists a
//        | left join person_teacher b
//        | on a.name=b.name and a.org_name=b.org_name where b.person_id is not null group by b.person_id,a.children_id,a.year,a.name
//      """.stripMargin)
//    person_teacher_not_exists_rel.createOrReplaceTempView("person_teacher_not_exists_rel")
////    person_teacher_not_exists_rel.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
////    person_teacher_not_exists_rel.drop("name").repartition(20).write.
////      format("hive").mode("overwrite").insertInto("dwb.wb_person_advisor_nsl_rel")
//
//
//    println("=======不存在的人和导师的关系========")
//    spark.sql(
//      """
//        |select b.psn_code,* from person_teacher_not_exists a
//        |left join person_exists_advisor b on a.name=b.zh_name and a.org_name=b.org_name
//      """.stripMargin).createOrReplaceTempView("psn_code_advisor")
//
//    val person_nsl=spark.sql(
//      """
//        |select *,'nsl' as source from psn_code_advisor a where not exists
//        |(select * from person_teacher_not_exists_rel b where a.children_id=b.children_id and a.name=b.name)
//      """.stripMargin)
//
////    person_nsl.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    person_nsl.createOrReplaceTempView("person_nsl")
//
//    person_nsl.select("psn_code","children_id","year").repartition(10).write
//        .format("hive").mode("overwrite").insertInto("dwb.wb_person_advisor_nsl_rel")
//
//    person_nsl.select("psn_code","org_name","source").repartition(10).write
//      .format("hive").mode("overwrite").insertInto("dwb.wb_person_nsl")
////    spark.sql("insert overwrite table dwb.wb_person_nsl select psn_code,name,org_name,'nsl' as source from person_nsl")

//    spark.sql("insert into table dwb.wb_person_advisor_nsl_rel select psn_code,children_id from person_nsl group by psn_code,children_id")
  }


  def fusison_not_exists():Unit={

    val originDF = spark.sql("select psn_code,name as zh_name,org_name from dwd.wd_person_json")
    val targetDF = spark.sql("select * from dm.dm_neo4j_person")
    PersonFusionUtil.fusionPerson_Org(spark, originDF, "psn_code", targetDF, "id")._3
      .createOrReplaceTempView("wd_person_json_not_exists")

    //    spark.sql(
    //      """
    //        |select b.targetID,a.name,a.org_name from dwd.wd_person_json a
    //        | left join rel b on a.psn_code=b.originID where b.targetID is not null
    //      """.stripMargin).createOrReplaceTempView("wd_person_json_replace")
    //
    spark.sql("insert overwrite table dwd.wd_person_not_exists_json select * from wd_person_json_not_exists")


  }

  def fusison_one2more():Unit={


    val originDF = spark.sql("select psn_code,name as zh_name,org_name from dwd.wd_person_json")
    val targetDF = spark.sql("select * from dm.dm_neo4j_person")
    PersonFusionUtil.fusionPerson_Org(spark, originDF, "psn_code", targetDF, "id")._2
      .createOrReplaceTempView("rel")
    //    spark.sql("select count(*) from rel").show()  //7504
    //    spark.sql("select count(*) from dwd.wd_person_json").show()           //15223
    spark.sql(
      """
        |select b.targetID,a.name,a.org_name from dwd.wd_person_json a
        | left join rel b on a.psn_code=b.originID where b.targetID is not null
      """.stripMargin).createOrReplaceTempView("wd_person_json_replace")

    spark.sql("insert overwrite table dwd.wd_person_json_one2more select * from wd_person_json_replace")


  }

  def clean_person_advisor_children():Unit={

        //student 学生去重，生成md5
        spark.sql(
          """
            |select
            |teacher_name,teacher_year,children_name,children_year,children_attr_id,org_name,children_org_name
            |from ods.o_json_person_children
            |group by
            |teacher_name,teacher_year,children_name,children_year,children_attr_id,org_name,children_org_name
          """.stripMargin).createOrReplaceTempView("person_children_group")


        spark.sql(
          """
            |insert overwrite table dwd.wd_person_children_json
            |select md5(concat_ws('_',children_name,children_attr_id,children_org_name)) as children_id,
            |teacher_name,teacher_year,children_name,children_year,children_attr_id,org_name,children_org_name,'student' as relation
            | from person_children_group
          """.stripMargin)
        println("============children group by count============")
        spark.sql("select count(*) from dwd.wd_person_children_json").show()

        //teacher  老师去重，生成md5
        spark.sql(
          """
            |select
            |name,year,children_name,children_year,children_attr_id,org_name,children_org_name
            |from ods.o_json_person_teacher
            |group by
            |name,year,children_name,children_year,children_attr_id,org_name,children_org_name
          """.stripMargin).createOrReplaceTempView("person_teacher_group")

        spark.sql(
          """
            |insert overwrite table dwd.wd_person_advisor_json
            |select md5(concat_ws('_',children_name,children_attr_id,children_org_name)) as children_id,
            |name,year,children_name,children_year,children_attr_id,org_name,children_org_name,'advisor' as relation
            | from person_teacher_group
          """.stripMargin)
        println("============teacher group by count============")
        spark.sql("select count(*) from dwd.wd_person_advisor_json").show()


//        spark.sql(
//          """
//            | select * from
//            | (select name,org_name,count(*) c from ods.o_json_person group by name,org_name having c = 1)a
//            | where exists (select * from dwd.wd_person_advisor_children_json b where a.name=b.name and a.org_name=b.org_name)
//          """.stripMargin).drop("c").createOrReplaceTempView("psncode_uniqueness")
//
//        spark.sql("insert overwrite table dwd.wd_person_json " +
//          "select * from ods.o_json_person a where exists (select * from psncode_uniqueness b where a.name=b.name and a.org_name=b.org_name)")
//
//        println("=========person exists========")
//       spark.sql("select count(*) from dwd.wd_person_json").show()
//
//
//        spark.sql(
//          """
//            | select * from
//            | (select name,org_name,count(*) c from ods.o_json_person group by name,org_name having c >1)a
//            | where exists (select * from dwd.wd_person_advisor_children_json b where a.name=b.name and a.org_name=b.org_name)
//          """.stripMargin).drop("c").createOrReplaceTempView("psn_code_non_uniqueness")
//
//        spark.sql("insert overwrite table dwd.wd_person_psncode_non_uniqueness_json " +
//          "select * from ods.o_json_person a where exists (select * from psn_code_non_uniqueness b where a.name=b.name and a.org_name=b.org_name)")
//
//        println("=========person not exists========")
//        spark.sql("select count(*) from dwd.wd_person_json_psncode_non_uniqueness").show()
//
//

  }
}
