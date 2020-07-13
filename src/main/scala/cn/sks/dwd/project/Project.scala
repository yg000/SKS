package cn.sks.dwd.project

import org.apache.spark.sql.SparkSession

object Project {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("Project")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()



    spark.sql(
      """
        |insert into table dwd.wd_project_nsfc
        |select
        | a.project_id
        |,a.zh_title
        |,a.en_title
        |,a.prj_no
        |,b.person_id
        |,b.zh_name as psn_name
        |,c.name as org_name
        |,a.grant_code
        |,a.grant_name
        |,a.subject_code1
        |,a.subject_code2
        |,substr(a.start_date,0,10) as start_date
        |,substr(a.end_date,0,10)   as   end_date
        |,substr(a.stat_year,0,4) as approval_year
        |,a.duration
        |,a.status
        |,a.csummary
        |,a.esummary
        |,a.post_dr_no
        |,a.dr_candidate_no
        |,a.ms_candidate_no
        |,a.middle_no
        |,a.junior_no
        |,a.senior_no
        |,a.no_of_unit
        |,a.inv_no
        |,a.total_amt
        |,a.total_inamt
        |,a.change_amt
        |,a.all_amt
        |,a.indirect_amt
        |,a.sbgz_amt
        |,a.ind_inamt
        |,a.source
        |from ods.o_nsfc_project   a
        |left join  ods.o_nsfc_person      b
        |on a.psn_code =b.psn_code
        |left join  ods.o_nsfc_organization     c
        |on a.org_code =c.org_code
      """.stripMargin)




  }




}
