package cn.sks.dwb.organization

import cn.sks.dm.organization.OrganizationDm.spark
import org.apache.spark.sql.SparkSession
import cn.sks.util.{AchievementUtil, BuildOrgIDUtil, DefineUDF, OrganizationUtil}

object org_dwb {




  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("org_dwb")
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

    spark.sql("""
                |select  pinyin,split(merger_name,',')[1] as province,short_name,name  from ods.o_const_dictionary_china_region where level_type in('1')
                |""".stripMargin).createOrReplaceTempView("dictionary_china_province")

    spark.sql("""
                |select  pinyin,split(merger_name,',')[1] as province,short_name,name,zip_code from (select * ,row_number()over(partition by short_name order by level_type,id) as tid from ods.o_const_dictionary_china_region where level_type in('2','3'))a where tid = 1
                |""".stripMargin).createOrReplaceTempView("dictionary_china_region")

    OrganizationUtil.dropDuplicates(spark,spark.read.table("dwd.wd_organization_sts"),"org_name","org_name").createOrReplaceTempView("wd_organization_sts")
    OrganizationUtil.dropDuplicates(spark,spark.read.table(" dwd.wd_organization_nsfc "),"org_name","org_name").createOrReplaceTempView("wd_organization_nsfc")


    spark.sql("""
                |insert overwrite table dwb.wb_organization_manual_sts
                |select ifnull(a.org_id,b.org_id),
                |ifnull(a.social_credit_code,b.social_credit_code),
                |ifnull(a.org_name,b.org_name),
                |ifnull(a.en_name,b.en_name),
                |ifnull(a.alias,b.alias),
                |ifnull(a.registration_date,b.registration_date),
                |ifnull(a.org_type,b.org_type),
                |ifnull(a.nature,b.nature),
                |ifnull(a.islegalPersonInstitution,b.islegalPersonInstitution),
                |ifnull(a.legal_person,b.legal_person),
                |ifnull(a.belongtocode,b.belongtocode),
                |ifnull(a.address,b.address),
                |ifnull(a.country,b.country),
                |ifnull(d.name,ifnull(a.province,b.province)),
                |ifnull(c.name,ifnull(a.city,b.city)),
                |ifnull(a.district,b.district),
                |ifnull(a.zipcode,b.zipcode),
                |ifnull(a.source,b.source)
                |from   dwd.wd_organization_manual a full outer join  wd_organization_sts b on clean_fusion(a. org_name) = clean_fusion(b.org_name)
                |left join dictionary_china_region c on ifnull(a.city,b.city) = c.short_name
                |left join dictionary_china_province d on ifnull(a.province,b.province) = d.short_name
                |""".stripMargin)
      //.repartition(50).write.format("hive").mode("overwrite").insertInto("")

    spark.sql("""
                |insert overwrite table dwb.wb_organization_manual_sts_nsfc
                |select ifnull(a.org_id,b.org_id),
                |ifnull(a.social_credit_code,b.social_credit_code),
                |ifnull(a.org_name,b.org_name),
                |ifnull(a.en_name,b.en_name),
                |ifnull(a.alias,b.alias),
                |ifnull(a.registration_date,b.registration_date),
                |ifnull(a.org_type,b.org_type),
                |ifnull(a.nature,b.nature),
                |ifnull(a.islegalPersonInstitution,b.islegalPersonInstitution),
                |ifnull(a.legal_person,b.legal_person),
                |ifnull(a.belongtocode,b.belongtocode),
                |ifnull(a.address,b.address),
                |ifnull(a.country,b.country),
                |ifnull(d.name,ifnull(a.province,b.province)),
                |ifnull(c.name,ifnull(a.city,b.city)),
                |ifnull(a.district,b.district),
                |ifnull(a.zipcode,b.zipcode),
                |ifnull(a.source,b.source)
                |from   dwb.wb_organization_manual_sts a full outer join  wd_organization_nsfc b on clean_fusion(a. org_name) = clean_fusion(b.org_name)
                |left join dictionary_china_region c on ifnull(a.city,b.city) = c.short_name
                |left join dictionary_china_province d on ifnull(a.province,b.province) = d.short_name
                |""".stripMargin)

//    spark.sql("""
//                |insert overwrite table dwb.wb_organization_manual_sts_nsfc_rel
//                |select a.org_id,
//                |b.org_id,
//                |null
//                |from   dwb.wb_organization_manual_sts a join  dwd.wd_organization_nsfc b on a. org_name = b.org_name
//                |""".stripMargin)

  }

}
