package cn.sks.nsl

import cn.sks.util.PersonFusionUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object NslFusionRel {
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



  def main(args: Array[String]): Unit = {

        fusion_teacher_rel()
        fusion_student_rel()


  }
  def fusion_student_rel():Unit={



    println("=============补充人和学生的关系==============")
    spark.sql(
      """
        |select
        | person_id,
        | student_person_id,
        | name,
        | year,
        | children_name,
        | children_year,
        | children_attr_id,
        | org_name,
        | children_org_name from dwb.wb_person_student_achievement
        |where person_achievement_id =student_achievement_id
        |group by person_id,
        | student_person_id,
        | name,
        | year,
        | children_name,
        | children_year,
        | children_attr_id,
        | org_name,
        | children_org_name
      """.stripMargin).createOrReplaceTempView("person_student")
//    spark.sql("select count(*) from person_student ").show()
    //    println("============人和学生的关系==========")
    //    val person_student_rel=spark.sql(
    //      """
    //        |select person_id,student_person_id,children_year from dwb.wb_person_student_achievement
    //        |where achievement_id=student_achievement_id group by person_id,student_person_id,children_year
    //      """.stripMargin)
    //    person_student_rel.createOrReplaceTempView("person_student_rel")
    //    person_student_rel.repartition(50).write.format("hive").mode("overwrite").insertInto("dwb.wb_person_children_rel")

    val person_student_not_exists=spark.sql(
      """
        |select * from dwd.wd_person_children_json a
        |where not exists(select * from person_student b
        | where a.children_name=b.children_name and
        |       a.children_org_name =b.children_org_name and
        |       a.children_attr_id=b.children_attr_id and
        |       a.name=b.name and
        |       a.org_name=b.org_name)
      """.stripMargin)
    person_student_not_exists.createOrReplaceTempView("person_student_not_exists")



    person_student_not_exists.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    person_student_not_exists.repartition(20).write.
//      format("hive").mode("overwrite").insertInto("dwb.wb_person_children_nsl")

    println("=======不存在的人的学生的关系========")
    val person_student_not_exists_rel=spark.sql(
      """
        |select b.person_id,a.children_id,a.name,a.children_year from person_student_not_exists a
        | left join person_student b
        | on a.name=b.name and a.org_name=b.org_name where b.person_id is not null group by b.person_id,a.children_id,a.children_year,a.name
      """.stripMargin)
    person_student_not_exists_rel.createOrReplaceTempView("person_student_not_exists_rel")
    person_student_not_exists_rel.drop("name").repartition(20).write.
          format("hive").mode("overwrite").insertInto("dwb.wb_person_children_nsl_rel")
//    spark.sql("select * from person_student_not_exists_rel where name='闫保平'").show()
//    while(true){}
    println("=======不存在的人和学生的关系========")
    spark.sql(
      """
        |select b.psn_code,a.children_id,a.name,a.org_name,a.children_year from person_student_not_exists a
        |left join dwb.wb_person_exists_student b on a.name=b.zh_name and a.org_name=b.org_name
      """.stripMargin).createOrReplaceTempView("psn_code_student")

    spark.sql("select * from psn_code_student where psn_code='760504'").show()

    val person_nsl=spark.sql(
      """
        |select a.psn_code,a.children_id,a.name,a.org_name,a.children_year from psn_code_student a where not exists
        |(select * from person_student_not_exists_rel
        | b where a.children_id=b.children_id and a.name=b.name)
      """.stripMargin)
    person_nsl.createOrReplaceTempView("person_nsl")



    person_nsl.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    person_nsl.drop("name").drop("org_name").write.format("hive")
      .mode("append").insertInto("dwb.wb_person_children_nsl_rel")


    person_nsl.select("psn_code","name","org_name").repartition(10).write
      .format("hive").mode("append").insertInto("dwb.wb_person_nsl")
//    spark.sql("insert into table dwb.wb_person_children_nsl_rel select psn_code,children_id from person_nsl group by psn_code,children_id")


    println("--------------------student ending------------------")

  }


  def fusion_teacher_rel():Unit={


    println("============人和导师的关系==========")

//    val person_advisor_rel= spark.sql(
//      """
//        |select person_id,teacher_person_id,year from advisor_achievement where achievement_id=teacher_achievement_id group by person_id,teacher_person_id,year
//      """.stripMargin)
//    //    spark.sql("select count(*) from person_advisor_rel").show()
//
//    person_advisor_rel.repartition(50).write.format("hive").mode("overwrite").insertInto("dwb.wb_person_advisor_rel")

    println("============补充人和导师的关系==========")
    spark.sql(
      """
        |select
        | person_id,
        | advisor_person_id,
        | name,
        | year,
        | children_name,
        | children_year,
        | children_attr_id,
        | org_name,
        | children_org_name from dwb.wb_person_advisor_achievement
        |where person_achievement_id =advisor_achievement_id
        |group by person_id,
        | advisor_person_id,
        | name,
        | year,
        | children_name,
        | children_year,
        | children_attr_id,
        | org_name,
        | children_org_name
      """.stripMargin).createOrReplaceTempView("person_teacher")
    //    spark.sql("select count(*) from person_student ").show()

//    spark.sql("select * from person_teacher where children_name='洪德元' and children_org_name='中国科学院植物研究所'").show()
//    while(true){}

    val person_teacher_not_exists=spark.sql(
      """
        |select * from dwd.wd_person_advisor_json a
        |where not exists(select * from person_teacher b
        | where a.children_name=b.children_name and
        |       a.children_org_name =b.children_org_name and
        |       a.children_attr_id=b.children_attr_id and
        |       a.name=b.name and
        |       a.org_name=b.org_name)
      """.stripMargin)
    person_teacher_not_exists.createOrReplaceTempView("person_teacher_not_exists")

//
//    person_teacher_not_exists.repartition(20).write.
//      format("hive").mode("overwrite").insertInto("dwb.wb_person_advisor_nsl")

    println("=======不存在的人的导师的关系========")
    val person_teacher_not_exists_rel=spark.sql(
      """
        |select b.person_id,a.children_id,a.year,a.name from person_teacher_not_exists a
        | left join person_teacher b
        | on a.name=b.name and a.org_name=b.org_name where b.person_id is not null group by b.person_id,a.children_id,a.year,a.name
      """.stripMargin)
    person_teacher_not_exists_rel.createOrReplaceTempView("person_teacher_not_exists_rel")
//    person_teacher_not_exists_rel.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    person_teacher_not_exists_rel.drop("name").repartition(20).write.
      format("hive").mode("overwrite").insertInto("dwb.wb_person_advisor_nsl_rel")


    println("=======不存在的人和导师的关系========")
    spark.sql(
      """
        |select b.psn_code,a.children_id,a.name,a.org_name,a.year from person_teacher_not_exists a
        |left join dwb.wb_person_exists_advisor b on a.name=b.zh_name and a.org_name=b.org_name
      """.stripMargin).createOrReplaceTempView("psn_code_advisor")

    val person_nsl=spark.sql(
      """
        |select a.psn_code,a.children_id,a.name,a.org_name,a.year from psn_code_advisor a where not exists
        |(select * from person_teacher_not_exists_rel b where a.children_id=b.children_id and a.name=b.name)
      """.stripMargin)

//    person_nsl.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    person_nsl.createOrReplaceTempView("person_nsl")
    person_nsl.printSchema()
    person_nsl.select("psn_code","children_id","year").repartition(10).write
        .format("hive").mode("append").insertInto("dwb.wb_person_advisor_nsl_rel")

    println("=================wb_person_nsl====================")
    person_nsl.select("psn_code","name","org_name").repartition(10).write
      .format("hive").mode("overwrite").insertInto("dwb.wb_person_nsl")
//    spark.sql("insert overwrite table dwb.wb_person_nsl select psn_code,name,org_name,'nsl' as source from person_nsl")

//    spark.sql("insert into table dwb.wb_person_advisor_nsl_rel select psn_code,children_id from person_nsl group by psn_code,children_id")
    println("--------------------teacher ending------------------")
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
