package cn.sks.nsl

import org.apache.spark.sql.SparkSession
import cn.sks.util.{DefineUDF, OrganizationUtil}
object TeacherStuden02 {

  val spark: SparkSession = SparkSession.builder()
    .master("local[40]")
    .config("spark.deploy.mode", "clent")
    .config("executor-memory", "12g")
    .config("executor-cores", "6")
    .config("spark.local.dir", "/data/tmp")
    //      .config("spark.drivermemory", "32g")
    //      .config("spark.cores.max", "16")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    //.config("spark.sql.shuffle.partitions", "120")
    .appName("TeacherStudent")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")
  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })
  def main(args: Array[String]): Unit = {

    spark.sql(
      """
        |select * from dm.dm_neo4j_person_product
    """.stripMargin).createOrReplaceTempView("person_achievement_rel")

    OrganizationUtil.getStandardOrganization(spark).createOrReplaceTempView("org_tb")


    spark.sql(
      """
        |select
        |children_name as student,
        |name as advisor,
        |children_org_name as student_org,
        |org_name as advisor_org,
        |children_year
        | from ods.o_json_person_children_group
        | union all
        | select
        | name as student,
        | children_name as advisor,
        | org_name as student_org,
        | children_org_name as advisor_org,
        | year as children_year
        | from ods.o_json_person_teacher_group
        |""".stripMargin).createOrReplaceTempView("person_children")

    spark.sql(
      """
        |select student as person_name,
        |student_org as org_name from person_children
        |union all
        |select advisor as person_name,
        |advisor_org as org_name from person_children
        |""".stripMargin).createOrReplaceTempView("person_union")

    spark.sql(
      """
        |select
        |b.id as person_id,
        |advisor,
        |advisor_org as org_name
        |from person_children a left join dm.dm_neo4j_person b on a.student = b.zh_name and clean_fusion(a.student_org) = clean_fusion(b.org_name)
        |""".stripMargin).createOrReplaceTempView("dm_neo4j_experience_study_b")



    spark.sql(
      """
        |select a.person_id as student_id,
        | b.id as teacher_id
        | from dm_neo4j_experience_study_b a left join  dm.dm_neo4j_person b on a.advisor = b.zh_name and clean_fusion(a.org_name) = clean_fusion(b.org_name)
        |""".stripMargin).createOrReplaceTempView("student_teacher")

    spark.sql(
      """
        |select
        |a.student_id,
        |a.teacher_id,
        |b.achievement_id,
        |c.achievement_id
        | from student_teacher  a
        | join  person_achievement_rel b on a.student_id = b.person_id
        | join  person_achievement_rel c on a.teacher_id = c.person_id where b.achievement_id = c.achievement_id
        |""".stripMargin).select("student_id","teacher_id").dropDuplicates().createOrReplaceTempView("student_teacher")

    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_person_advisor partition (flag='student_advisor_json')
        |select student_id,teacher_id,null from student_teacher
        |""".stripMargin)



    spark.read.table("dm.dm_neo4j_person_advisor").filter("flag = 'student_advisor_json' ").createOrReplaceTempView("student_advisor_json")

    spark.sql(
      """
        |select
        |b.zh_name as stu_name,
        |c.zh_name as adv_name,
        |b.org_name as stu_org,
        |c.org_name as adv_org,
        |person_id,
        |advisor_id
        |from student_advisor_json a
        |left join dm.dm_neo4j_person b on person_id= b.id
        |left join dm.dm_neo4j_person c on advisor_id= c.id
        |""".stripMargin).createOrReplaceTempView("student_advisor_json")

    spark.sql(
      """
        |select
        |student,
        |advisor,
        |student_org,
        |advisor_org
        | from person_children a
        |left join student_advisor_json b on a.student  = b.stu_name and a.advisor = b.adv_name and a.student_org = b.stu_org and a.advisor_org = b.adv_org
        |where stu_name is null and adv_name is null and stu_org is null and adv_org is null
        |""".stripMargin).createOrReplaceTempView("person_advisor_without")

    spark.sql(
      """
        |select student as person_name,
        |student_org as org_name from person_advisor_without
        |union
        |select advisor as person_name,
        |advisor_org as org_name from person_advisor_without
        |""".stripMargin).createOrReplaceTempView("person_advisor_union")

    spark.sql(
      """
        |insert overwrite table dwb.wb_person_add partition(flag = 'relation_student_advisor_json')
        |select concat('nsl_',md5(concat(person_name,org_name))),
        | person_name,
        | org_name from person_advisor_union
        |""".stripMargin)

    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_person_advisor partition (flag='student_advisor_json_supplement')
        |select concat('nsl_',md5(concat(student,student_org))),
        | concat('nsl_',md5(concat(advisor,advisor_org))),
        | null
        | from person_advisor_without
        |""".stripMargin)





  }
}
