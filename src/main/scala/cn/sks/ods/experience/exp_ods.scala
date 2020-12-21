package cn.sks.ods.experience

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object exp_ods {

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("exp_ods")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })


  def main(args: Array[String]): Unit = {
    spark.read.option("header", true).csv("src/main/resources/exp_org_illegal_name.csv").createOrReplaceTempView("illegal_name")
    //spark.read.table("illegal_name").show(1000)
    spark.sql(
      """
        |select
        |psn_code,
        |dx_date1 as start_date,
        |dx_date1 as end_date,
        |dx_college1 as org_name,
        |dx_profession1 as profession,
        |dx_degree1 as degree,
        |dx_country1 as country,
        |dx_year1 as award_year,
        |dx_advisor1 as advisor
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |dx_date2 as start_date,
        |dx_date2 as end_date,
        |dx_college2 as org_name,
        |dx_profession2 as profession,
        |dx_degree2 as degree,
        |dx_country2 as country,
        |dx_year2 as award_year,
        |dx_advisor2 as advisor
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |yjs_date1 as start_date,
        |yjs_date1 as end_date,
        |yjs_college1 as org_name,
        |yjs_profession1 as profession,
        |yjs_degree1 as degree,
        |yjs_country1 as country,
        |yjs_year1 as award_year,
        |yjs_advisor1 as advisor
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |yjs_date2 as start_date,
        |yjs_date2 as end_date,
        |yjs_college2 as org_name,
        |yjs_profession2 as profession,
        |yjs_degree2 as degree,
        |yjs_country2 as country,
        |yjs_year2 as award_year,
        |yjs_advisor2 as advisor
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |bs_date1 as start_date,
        |bs_date1 as end_date,
        |bs_college1 as org_name,
        |bs_profession1 as profession,
        |bs_degree1 as degree,
        |bs_country1 as country,
        |bs_year1 as award_year,
        |bs_advisor1 as advisor
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |bs_date2 as start_date,
        |bs_date2 as end_date,
        |bs_college2 as org_name,
        |bs_profession2 as profession,
        |bs_degree2 as degree,
        |bs_country2 as country,
        |bs_year2 as award_year,
        |bs_advisor2 as advisor
        |from nsfc.o_person_resume
        |""".stripMargin).createOrReplaceTempView("experience_study")

    spark.sql(
      """
        |select
        |psn_code,
        |gz_date1 as start_date,
        |gz_date1 as end_date,
        |gz_unit1 as org_name,
        |gz_department1 as deparment,
        |gz_title1 as prof_title
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |gz_date2 as start_date,
        |gz_date2 as end_date,
        |gz_unit2 as org_name,
        |gz_department2 as deparment,
        |gz_title2 as prof_title
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |gz_date3 as start_date,
        |gz_date3 as end_date,
        |gz_unit3 as org_name,
        |gz_department3 as deparment,
        |gz_title3 as prof_title
        |from nsfc.o_person_resume
        |""".stripMargin).createOrReplaceTempView("experience_work")

    spark.sql(
      """
        |select
        |psn_code,
        |bsh_date as start_date,
        |bsh_date as end_date,
        |bsh_unit as org_name,
        |bsh_advisor as advisor,
        |bsh_onjob as onjob
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |bsh_date1 as start_date,
        |bsh_date1 as end_date,
        |bsh_unit1 as org_name,
        |bsh_advisor1 as advisor,
        |bsh_onjob1 as onjob
        |from nsfc.o_person_resume
        |union all
        |select
        |psn_code,
        |bsh_date2 as start_date,
        |bsh_date2 as end_date,
        |bsh_unit2 as org_name,
        |bsh_advisor2 as advisor,
        |bsh_onjob2 as onjob
        |from nsfc.o_person_resume
        |""".stripMargin).createOrReplaceTempView("experience_postdoctor")

//
//    spark.sql(
//      """
//        |insert overwrite table ods.o_nsfc_experience_study
//        |select
//        |psn_code,
//        |trim(split(start_date,'-')[0]),
//        |trim(split(end_date,'-')[1]),
//        |trim(org_name),
//        |profession,
//        |degree,
//        |country,
//        |award_year,
//        |advisor
//        |from experience_study where org_name is not null and  trim(org_name) not in (select * from illegal_name)
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |insert overwrite table ods.o_nsfc_experience_work
//        |select
//        |psn_code,
//        |trim(split(start_date,'-')[0]),
//        |trim(split(end_date,'-')[1]),
//        |trim(org_name),
//        |deparment,
//        |prof_title
//        |from experience_work where org_name is not null and trim(org_name) not in (select * from illegal_name)
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |insert overwrite table ods.o_nsfc_experience_postdoctor
//        |select
//        |psn_code,
//        |trim(split(start_date,'-')[0]),
//        |trim(split(end_date,'-')[1]),
//        |trim(org_name),
//        |advisor,
//        |onjob
//        |from experience_postdoctor where org_name is not null and trim(org_name) not in (select * from illegal_name)
//        |""".stripMargin)






  }

}
