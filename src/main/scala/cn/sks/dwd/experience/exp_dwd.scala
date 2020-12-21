package cn.sks.dwd.experience

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession
import cn.sks.util.BuildOrgIDUtil
object exp_dwd {
  val spark = SparkSession.builder()
    //.master("local[40]")
    .appName("exp_dwd")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","10")
    .config("spark.local.dir", "/data/tmp")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })
//
  def main(args: Array[String]): Unit = {
    val experience_study_df = spark.read.table("ods.o_nsfc_experience_study")
    val experience_work_df = spark.read.table("ods.o_nsfc_experience_work")
    val experience_postdoctor_df = spark.read.table("ods.o_nsfc_experience_postdoctor")
    val csai_person_work_experience_df = spark.read.table("ods.o_csai_person_work_experience_all")

    BuildOrgIDUtil.buildOrganizationExpID(spark,experience_study_df,"org_name","nsfc_experience_study").createOrReplaceTempView("o_nsfc_experience_study")
    BuildOrgIDUtil.buildOrganizationExpID(spark,experience_work_df,"org_name","nsfc_experience_work").createOrReplaceTempView("o_nsfc_experience_work")
    BuildOrgIDUtil.buildOrganizationExpID(spark,experience_postdoctor_df,"org_name","nsfc_experience_postdoctor").createOrReplaceTempView("o_nsfc_experience_postdoctor")
    BuildOrgIDUtil.buildOrganizationID(spark,csai_person_work_experience_df,"org_name","csai_work_experience").createOrReplaceTempView("csai_person_work_experience")






    spark.sql(
      """
        |insert overwrite table dwd.wd_nsfc_experience_study
        |select
        |zh_name,
        |md5(a.psn_code),
        |concat(if(split(start_date,'\\.')[0]='','0000',split(start_date,'\\.')[0]),'-',right(100+ifnull(cast(split(start_date,'\\.')[1] as int),9),2)) as start_date,
        |concat(if(split(end_date,'\\.')[0]='','0000',split(end_date,'\\.')[0]),'-',right(100+ifnull(cast(split(end_date,'\\.')[1] as int),6),2)) as end_date,
        |new_org_name,
        |org_id,
        |a.profession,
        |a.degree,
        |a.country,
        |a.award_year,
        |a.advisor,
        |"nsfc" as source
        |from o_nsfc_experience_study a left join nsfc.o_person b on a.psn_code  = b.psn_code
        |""".stripMargin)

    spark.sql(
      """
        |insert overwrite table dwd.wd_nsfc_experience_work
        |select
        |zh_name,
        |md5(a.psn_code),
        |concat(if(split(start_date,'\\.')[0]='','0000',split(start_date,'\\.')[0]),'-',right(100+ifnull(cast(split(start_date,'\\.')[1] as int),9),2)) as start_date,
        |concat(if(split(end_date,'\\.')[0]='','0000',split(end_date,'\\.')[0]),'-',right(100+ifnull(cast(split(end_date,'\\.')[1] as int),6),2)) as end_date,
        |a.org_name,
        |org_id,
        |a.deparment,
        |a.prof_title,
        |"nsfc" as source
        |from o_nsfc_experience_work a left join nsfc.o_person b on a.psn_code  = b.psn_code
        |""".stripMargin)

    spark.sql(
      """
        |insert overwrite table dwd.wd_nsfc_experience_postdoctor
        |select
        |zh_name,
        |md5(a.psn_code),
        |concat(if(split(start_date,'\\.')[0]='','0000',split(start_date,'\\.')[0]),'-',right(100+ifnull(cast(split(start_date,'\\.')[1] as int),9),2)) as start_date,
        |concat(if(split(end_date,'\\.')[0]='','0000',split(end_date,'\\.')[0]),'-',right(100+ifnull(cast(split(end_date,'\\.')[1] as int),6),2)) as end_date,
        |a.org_name,
        |org_id,
        |a.advisor,
        |a.onjob,
        |"nsfc" as source
        |from o_nsfc_experience_postdoctor a left join nsfc.o_person b on a.psn_code  = b.psn_code
        |""".stripMargin)

      spark.sql(
      """
        |select
        |null as zh_name,
        |person_id,
        |concat(substr(start_date,0,4),'-00-00') as start_date,
        |concat(substr(end_date,0,4),'-00-00') as  end_date,
        |org_name,
        |org_id,
        |null,
        |title,
        |"csai" as source
        |from  (select *,row_number() over (partition by person_id,org_id order by start_date) as tid from  csai_person_work_experience)a  where tid = 1
        |""".stripMargin).createOrReplaceTempView("mid_wd_nsfc_experience_work_0")



    spark.sql(
      """
        |select
        |null,
        |ifnull(b.person_id_to,a.person_id) as person_id,
        |start_date,
        |end_date,
        |org_name,
        |org_id,
        |null,
        |title,
        |"csai" as source
        |from  mid_wd_nsfc_experience_work_0 a
        |left join  dwb.wb_person_rel b on a.person_id = b.person_id_from
        |""".stripMargin).createOrReplaceTempView("mid_wd_nsfc_experience_work")


    spark.sql(
      """
        |insert into table dwd.wd_nsfc_experience_work
        |select
        |null,
        |person_id,
        |min(start_date),
        |max(end_date),
        |org_name,
        |org_id,
        |null,
        |title,
        |"csai" as source
        |from  mid_wd_nsfc_experience_work group by person_id,org_name,org_id,title
        |""".stripMargin)




//    spark.sql(
//      """
//        |insert into table dwd.wd_nsfc_experience_work
//        |select
//        |null,
//        |a.person_id,
//        |concat(substr(a.start_date,0,4),'-00-00') as start_date,
//        |concat(substr(a.end_date,0,4),'-00-00') as  end_date,
//        |a.org_name,
//        |a.org_id,
//        |null,
//        |a.title
//        |from csai_person_work_experience a left join dwd.wd_nsfc_experience_work b on a.person_id = b.person_id and a.org_id = b.org_id where b.person_id is null or b.org_id is null
//        |""".stripMargin)

//    spark.sql(
//      """
//        |select count(*) from dwd.wd_nsfc_experience_work
//        |""".stripMargin).show()


  }
}
