package cn.sks.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object BuildOrgIDUtil {
  /* 生成对应单位org_id
     参数：
            spark
            oldDf    :原始表
            org_name :组织名字对应列
            source   :wb_organization_add表flag列对应值
     返回值 :删除原有表org_id,增加标准表new_org_name,org_id
    */

  def buildOrganizationID(spark:SparkSession,oldDf:DataFrame,org_name:String,source:String):DataFrame={

    spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    val newTb = "dwb.wb_organization_add"



    spark.sql(
      s"""
         |create table if not exists ${newTb} like dwb.wb_organization
         |""".stripMargin)

    spark.sql(
      s"""
        |select * from ${newTb}
        |union
        |select * from dwb.wb_organization
        |""".stripMargin).createOrReplaceTempView("wb_organization")

    oldDf.drop("org_id").createOrReplaceTempView("oldtb")

//    spark.sql("""
//                |select  non_standard_name,standard_name from (select * ,row_number()over(partition by non_standard_name order by standard_name desc) as tid from dwb.wb_organization_comparison_table )a where tid = 1
//                |""".stripMargin).createOrReplaceTempView("wb_organization_comparison_table")

    spark.sql(
      s"""
         |select a.org_name as org_name
         | from
         |(select ifnull(standard_name,a.$org_name )  as org_name from   oldtb  a left join dwb.wb_organization_comparison_table b on non_standard_name = a.$org_name ) a
         |left join wb_organization c on clean_fusion(a.org_name) = clean_fusion(c.org_name)  where c.org_name is null
         |""".stripMargin).createOrReplaceTempView("org_product_lack")


    spark.sql( s"""
                  |select * from (select *,row_number()over(partition by clean_fusion(org_name) order by org_name desc) as tid from org_product_lack) a where tid =1
                  |""".stripMargin).createOrReplaceTempView("org_product_lack")


    spark.sql(
      s"""
         |insert into table $newTb partition (flag='$source')
         |select md5(trim(org_name)),
         |null as social_credit_code,
         |trim(org_name),
         |null as en_name,
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
         |null as source
         | from org_product_lack where org_name is not null
         |""".stripMargin)

    spark.sql(
      s"""
         |select * from $newTb
         |union all
         |select * from dwb.wb_organization
         |""".stripMargin).createOrReplaceTempView("wb_organization")

    spark.sql(
      s"""
         |select a.*,c.org_id,c.org_name as new_org_name
         |from
         |(select ifnull(standard_name,a.$org_name )  as tmp_org_name,a.* from oldtb a left join dwb.wb_organization_comparison_table b on non_standard_name = a.$org_name) a
         |left join  wb_organization c on clean_fusion(a.tmp_org_name) = clean_fusion(c.org_name)
         |""".stripMargin).drop("tmp_org_name")


  }
}
