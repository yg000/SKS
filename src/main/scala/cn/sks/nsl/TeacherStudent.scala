package cn.sks.nsl

import org.apache.spark.sql.SparkSession
import cn.sks.util.{DefineUDF, OrganizationUtil}
object TeacherStudent {

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

    val dm_neo4j_experience_study = spark.sql(
      """
        |select person_id,
        | org_id,
        | advisor,
        | degree,
        | award_year as year
        | from dm.dm_neo4j_experience_study
        |""".stripMargin)

    spark.read.table("dm.dm_neo4j_experience_study").select("person_id","org_id","advisor")
      .unionAll(spark.read.table("dm.dm_neo4j_experience_postdoctor").select("person_id","org_id","advisor")).cache()
      .createOrReplaceTempView("dm_neo4j_experience_study")

    spark.sql(
      """
        |select
        |person_id
        |,org_id
        |,explode(split(advisor,";")) as advisor
        |from dm_neo4j_experience_study where advisor is not null and advisor not in ('无','')
        |""".stripMargin).createOrReplaceTempView("dm_neo4j_experience_study_a")


    spark.sql(
      """
        |select
        |person_id,
        |advisor,
        |org_name
        |from dm_neo4j_experience_study_a a join org_tb b on a.org_id = b.org_id
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
        |insert overwrite table dm.dm_neo4j_person_advisor partition (flag='experience_study')
        |select student_id,teacher_id,null from student_teacher
        |""".stripMargin)

    spark.read.table("dm.dm_neo4j_person_advisor").filter("flag = 'experience_study'").createOrReplaceTempView("experience_study_person_advisor")

    spark.sql(
      """
        |select
        |person_id,
        |advisor_id,
        |zh_name
        |from experience_study_person_advisor a left join dm.dm_neo4j_person b on advisor_id= b.id
        |""".stripMargin).dropDuplicates("person_id","zh_name").createOrReplaceTempView("mid_tb")

    spark.sql(
      """
        |select
        |concat('nsl_',md5(concat(a.advisor,a.org_name))) as person_id,
        | a.advisor,
        | a.org_name
        |from dm_neo4j_experience_study_b a
        |left join mid_tb b on a.person_id = b.person_id and a.advisor = b.zh_name
        |where b.person_id is null and b.zh_name is null
        |""".stripMargin).dropDuplicates().createOrReplaceTempView("tmp")

    spark.sql(
      """
        |insert overwrite table dwb.wb_person_add partition(flag='relation_experience_study')
        |select * from tmp
        |""".stripMargin)

    spark.sql(
      """
        |insert overwrite table dm.dm_neo4j_person_advisor partition (flag='experience_study_supplement')
        |select
        |a.person_id,
        |concat('nsl_',md5(concat(a.advisor,a.org_name))),
        |null
        |from dm_neo4j_experience_study_b a
        |left join mid_tb b on a.person_id = b.person_id and a.advisor = b.zh_name
        |where b.person_id is null and b.zh_name is null
        |""".stripMargin)





  }
}
