package cn.sks.dwb.organization

import org.apache.spark.sql.SparkSession
import cn.sks.util.{AchievementUtil, BuildOrgIDUtil, DefineUDF}

object org_final {
  val spark = SparkSession.builder()
    .master("local[8]")
    .appName("dddd")
    .config("spark.deploy.mode", "client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions", "10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })

  def main(args: Array[String]): Unit = {
    AchievementUtil.getDataTrace(spark,"ods.o_csai_organization_all","dwb.wb_organization")
    AchievementUtil.getDataTrace(spark,"ods.o_nsfc_organization_psn","dwb.wb_organization")
    AchievementUtil.getDataTrace(spark,"ods.o_ms_organization","dwb.wb_organization")
    AchievementUtil.getDataTrace(spark,"dwb.wb_organization_manual_sts_nsfc","dwb.wb_organization")
//    spark.sql("""
//                |select org_name,"csai" as source from ods.o_csai_organization_all
//                |union
//                |select name as org_name,"nsfc" as source from ods.o_nsfc_organization_psn
//                |""".stripMargin).createOrReplaceTempView("org_name_all")


        spark.sql("""
                    |select
                    |ifnull(a.org_id,b.org_id) as org_id,
                    |ifnull(a.org_name,b.name) as org_name,
                    |type,
                    |nationality,
                    |province ,
                    |city,
                    |address,
                    |longitude,
                    |latitude,
                    |ifnull(a.source,'nsfc') as source
                    |from ods.o_csai_organization_all a full outer join ods.o_nsfc_organization_psn b on a.org_name = b.name
                    |""".stripMargin).createOrReplaceTempView("org_name_all")

    spark.sql( """
                 |select * from (select *,row_number()over(partition by clean_fusion(org_name) order by org_name) as tid from org_name_all)a where tid =1
                 |""".stripMargin).createOrReplaceTempView("org_name_all")

    spark.sql("""
                |select
                |a.org_name as org_name,
                |type,
                |nationality,
                |a.province as province,
                |a.city as city,
                |a.address as address,
                |longitude,
                |latitude,
                |a.source as source
                |from org_name_all a left join  dwb.wb_organization_manual_sts_nsfc b on clean_fusion(a.org_name) = clean_fusion(b.org_name)
                | where b.org_name is null
                |""".stripMargin).createOrReplaceTempView("o_tmp_organization_name")


    spark.sql("""
                |insert overwrite table dwb.wb_organization
                |select *,'0' as flag from dwb.wb_organization_manual_sts_nsfc
                |""".stripMargin)

    spark.sql("""
                |insert into table dwb.wb_organization
                |select md5(org_name),
                |null as social_credit_code,
                |a.org_name,
                |null as en_name,
                |null as alias,
                |null as registration_date,
                |null as  org_type,
                |null as nature,
                |null as isLegalPersonInstitution,
                |null as legal_person,
                |null as belongtocode,
                |address as address,
                |nationality as country,
                |province as province,
                |city as city,
                |null as district,
                |null as zipcode,
                |source as source,
                |'1' as flag  from o_tmp_organization_name a
                |left join dwb.wb_organization_comparison_table b on clean_fusion(org_name) = clean_fusion(non_standard_name) where b.non_standard_name is null
                |""".stripMargin)

    spark.sql( """
                 |select * from (select *,row_number()over(partition by clean_fusion(org_en_name) order by org_en_name desc) as tid from ods.o_ms_organization)a where tid =1
                 |""".stripMargin).createOrReplaceTempView("o_ms_organization")


    spark.sql("""
                |insert into table dwb.wb_organization
                |select concat('en_',md5(org_en_name)) ,
                |null as social_credit_code,
                |org_en_name as org_name,
                |org_en_name as en_name,
                |null as alias,
                |null as registration_date,
                |null as  org_type,
                |null as nature,
                |null as isLegalPersonInstitution,
                |null as legal_person,
                |null as belongtocode,
                |null as address,
                |null as country,
                |null as province,
                |null as city,
                |null as district,
                |null as zipcode,
                |"ms" as source,
                |'1' as flag  from (select distinct trim(org_en_name) as org_en_name from o_ms_organization) a
                |left join dwb.wb_organization b on clean_fusion(b.org_name) = clean_fusion(a.org_en_name) where b.org_name is null
                |""".stripMargin)

  }

}