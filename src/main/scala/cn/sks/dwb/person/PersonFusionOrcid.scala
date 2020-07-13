package cn.sks.dwb.person

import cn.sks.util.{DefineUDF, NameToPinyinUtil}
import org.apache.spark.sql.SparkSession

object PersonFusionOrcid {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("yarn")
      .appName("PersonFusionOrcid")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })

    val df = spark.sql("select * from dwb.wb_person_nsfc_sts_academician_csai")
    val person_to_pinyin = NameToPinyinUtil.nameToPinyin(spark,df,"zh_name")
    person_to_pinyin.createOrReplaceTempView("person_to_pinyin")
    person_to_pinyin.show(3)
    val person_origin= spark.sql(
      """
        |select * ,CleanFusion(zh_name) as clean_zh_name,
        | CleanFusion(en_name_normal) as clean_en_name_normal,
        | CleanFusion(en_name_inverted) as clean_en_name_inverted
        |from person_to_pinyin
        |""".stripMargin)
    person_origin.createOrReplaceTempView("person_origin")

    val person_orcid = spark.sql(
      """
        |select *,CleanFusion(en_name) as clean_en_name
        | from ods.o_orcid_person
        |""".stripMargin)
    person_orcid.createOrReplaceTempView("person_orcid")

    val rel_person= spark.sql(
      """
        |select * from (
        |  select
        |  a.person_id as orcid_id ,b.person_id
        |  from  person_orcid a join person_origin b
        |  on a.clean_en_name=b.clean_zh_name
        |  )a
        |group by orcid_id,person_id
      """.stripMargin)
    println(rel_person.count())

    val rel_person1= spark.sql(
      """
        |select * from (
        |  select
        |  a.person_id as orcid_id ,b.person_id
        |  from  person_orcid a join person_origin b
        |  on a.clean_en_name=b.clean_en_name_normal
        |  )a
        |group by orcid_id,person_id
      """.stripMargin)
    val rel_person2= spark.sql(
      """
        |select * from (
        |  select
        |  a.person_id as orcid_id ,b.person_id
        |  from  person_orcid a join person_origin b
        |  on a.clean_en_name=b.clean_en_name_inverted
        |  )a
        |group by orcid_id,person_id
      """.stripMargin)
    println("--------------------------")
    println("--------------------------")
    println("---------------------------")
    println("--------------------------rel_person"+rel_person.count())
    println("--------------------------rel_person1"+rel_person1.count())
    println("---------------------------rel_person2"+rel_person2.count())
    println("--------------------------")
    println("--------------------------")
    println("---------------------------")




  }
}

