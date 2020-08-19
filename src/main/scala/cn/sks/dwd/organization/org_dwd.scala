package cn.sks.dwd.organization

import cn.sks.BaiduTranslate.baidu.TransApi
import org.apache.spark.sql.SparkSession
import cn.sks.util.{DefineUDF,NameToPinyinUtil}
import cn.sks.util.OrganizationUtil
object org_dwd {

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("org_dwd")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  spark.sqlContext.udf.register("TransformEnglish", (str: String) => {
    DefineUDF.transformEnglish(str)

  })
  spark.sqlContext.udf.register("TransformChinese", (str: String) => {
    DefineUDF.transformChinese(str)
  })

  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })


  def main(args: Array[String]): Unit = {

    spark.sql("""
                |select  pinyin,split(merger_name,',')[1] as province,short_name,name  from ods.o_const_dictionary_china_region where level_type in('1')
                |""".stripMargin).createOrReplaceTempView("dictionary_china_province")

   spark.sql("""
               |select  pinyin,split(merger_name,',')[1] as province,short_name,name,zip_code from (select * ,row_number()over(partition by pinyin order by level_type,id) as tid from ods.o_const_dictionary_china_region where level_type in('2','3'))a where tid = 1
               |""".stripMargin).createOrReplaceTempView("dictionary_china_region")


    OrganizationUtil.dropDuplicates(spark,spark.read.table("ods.o_manual_organization_standards_institutions"),"chinese_name_of_organization","id").createOrReplaceTempView("manual_organization_standards_institutions")
    OrganizationUtil.dropDuplicates(spark,spark.read.table("ods.o_manual_organization_zky_inst"),"inst_name","id").createOrReplaceTempView("manual_organization_zky_inst")
    OrganizationUtil.dropDuplicates(spark,spark.read.table("ods.o_manual_organization_sts"),"org_name","registration_date").createOrReplaceTempView("manual_organization_sts")
    OrganizationUtil.dropDuplicates(spark,spark.read.table("ods.o_manual_organization_grid"),"org_name","established").createOrReplaceTempView("manual_organization_grid")


//    spark.sql(    """
//                    |insert overwrite table dwd.wd_organization_manual
//                    |select md5(trim(chinese_name_of_organization)) as org_id,
//                    |null as social_credit_code,
//                    |trim(chinese_name_of_organization) as org_name,
//                    |english_name_of_organization as en_name,
//                    |replace(b.abbr," ",";") as alias,
//                    |null as registration_date,
//                    |organization_type as org_type,
//                    |null as nature,
//                    |null as isLegalPersonInstitution,
//                    |b.legal_repre as legal_person,
//                    |null as belongtocode,
//                    |null as address,
//                    |'中国' as country,
//                    |c.province as province,
//                    |c.short_name as city,
//                    |null as district,
//                    |c.zip_code as zipcode,
//                    |'manual' as source
//                    | from manual_organization_standards_institutions a left join manual_organization_zky_inst b on clean_fusion(a.chinese_name_of_organization) = clean_fusion(b.inst_name)
//                    | left join dictionary_china_region c on replace(a.city,' City','')= c.pinyin
//                    |""".stripMargin)
//
//
//    spark.sql( """
//                    |insert into table dwd.wd_organization_manual select md5(inst_name) as org_id,
//                    |null as social_credit_code,
//                    |inst_name as org_name,
//                    |TransformEnglish(inst_name) as en_name,
//                    |replace(abbr," ",";") as alias,
//                    |null as registration_date,
//                    |"Facility" as org_type,
//                    |null as nature,
//                    |null as isLegalPersonInstitution,
//                    |legal_repre,
//                    |null as belongtocode,
//                    |null as address,
//                    |'中国' as country,
//                    |province as province,
//                    |city as city,
//                    |null as district,
//                    |null as zipcode,
//                    |'manual' as source
//                    | from manual_organization_zky_inst where clean_fusion(inst_name) not in (select clean_fusion(chinese_name_of_organization) from manual_organization_standards_institutions where chinese_name_of_organization is not null)
//                    |""".stripMargin)
//
//
//    spark.sql( """
//                 |insert into table dwd.wd_organization_manual select
//                 |concat('en_',md5(a.org_name)) as org_id,
//                 |null as social_credit_code,
//                 |a.org_name as org_name,
//                 |a.org_name as en_name,
//                 |a.alias,
//                 |concat(established,'-01-01') registration_date,
//                 |a.org_type,
//                 |null as nature,
//                 |null as isLegalPersonInstitution,
//                 |null as legal_repre,
//                 |null as belongtocode,
//                 |null as address,
//                 |a.country,
//                 |a.state as province,
//                 |a.city as city,
//                 |null as district,
//                 |null as zipcode,
//                 |'manual' as source
//                 | from manual_organization_grid a  left join dwd.wd_organization_manual b on clean_fusion(a.org_name)= clean_fusion(b.en_name)  where b.org_name is null
//                 |""".stripMargin)
//
//
//
//
//    spark.sql("""
//                |select
//                |    org_id
//                |   ,null as social_credit_code
//                |   ,name as org_name
//                |   ,en_name
//                |   ,null as alias
//                |   ,concat(regyear,'-01-01') as registration_date
//                |   ,c.zh_cn_caption as org_type
//                |   ,d.zh_cn_caption as nature
//                |   ,null as isLegalPersonInstitution
//                |   ,faren as legal_person
//                |   ,b.zh_cn_caption   as   belongtocode
//                |   ,null as address
//                |   ,null as country
//                |   ,province
//                |   ,city
//                |   ,null as district
//                |   ,zipCode
//                |   ,'nsfc' as source
//                |from ods.o_nsfc_organization a
//                |left join ods.o_nsfc_const_dictionary b
//                |on b.category ='unit_relation' and  a.belongtocode=b.code
//                |left join ods.o_nsfc_const_dictionary c
//                |on c.category ='org_type' and  a.orgtype=c.code
//                |left join ods.o_nsfc_const_dictionary d
//                |on d.category ='91' and  a.nature=d.code
//                |""".stripMargin).createOrReplaceTempView("organization_nsfc")
//
//
//    spark.sql("""
//                |insert overwrite table dwd.wd_organization_nsfc
//                |select md5(if(org_name like '%1',substr(org_name,1,length(org_name)-1),org_name)),
//                |social_credit_code,
//                |if(org_name like '%1',substr(org_name,1,length(org_name)-1),org_name) as org_name,
//                |en_name,
//                |alias,
//                |registration_date,
//                |case
//                |       when   `org_name` like '%保健院%' or  `org_name` like '%医院%' or  `org_name` like '%急救中心%' or  `org_name` like '%中医%' or  `org_name` like '%健康中心%' or  `org_name` like '%医疗%' or  `org_name` like '%疾病预防%'   then  "Healthcare"
//                |       when   `org_name` like '%研究院%' or  `org_name` like '%研究所%' or  `org_name` like '%研究中心%'  or  `org_name` like '%设计院%'  or  `org_name` like '%学会%'  or  `org_name` like '%技术中心%'  then  "Facility"
//                |       when   `org_name` like '%血液中心%' or  `org_name` like '%科学中心%' or  `org_name` like '%基金%' or  `org_name` like '%协会%' or  `org_name` like '%学会%' or  `org_name` like '%组织%' or  `org_name` like '%商会%'   then  "Nonprofit"
//                |       when   `org_name` like '%博物馆%' or  `org_name` like '%保护区%' or  `org_name` like '%动物园%' or    `org_name` like '%植物园%' or  `org_name` like '%图书馆%' or  `org_name` like '%科技馆%' or  `org_name` like '%天文馆%'   then  "Archive"
//                |       when   `org_name` like '%集团%' or `org_name` like '%银行%' or `org_name` like '%厂' or `org_name` like '%（中国）%' or  `org_name` like '%公司%'   then  "Company"
//                |       when   `org_name` like '%政府%' or  `org_name` like '%气象台%' or  `org_name` like '%办公室%' or  `org_name` like '%工作站%' or  `org_name` like '%政府%' or  `org_name` like '%防疫站%' or  `org_name` like '%部队%' or  `org_name` like '%委员会%' or  `org_name` like '%国务院%' or  `org_name` like '%所' or  `org_name` like '%司' or  `org_name` like '%厅' or  `org_name` like '%科学院%' or  `org_name` like '%局%' or  `org_name` like '%部' or  `org_name` like '%军事%'   then  "Government"
//                |       when   `org_name` like '%中学%' or  `org_name` like '%学校%' or  `org_name` like '%大学%' or  `org_name` like '%学院%'   then  "Education"
//                |       else   'Other'  end as org_type,
//                |nature,
//                |isLegalPersonInstitution,
//                |legal_person,
//                |belongtocode,
//                |address,
//                |country,
//                |province,
//                |city,
//                |district,
//                |zipcode,
//                |source from   (select * ,row_number()over(partition by if(org_name like '%1',substr(org_name,1,length(org_name)-1),org_name) order by org_id desc) as tid from organization_nsfc where length(org_name)>2)a where tid = 1
//                |""".stripMargin)
//
//    spark.sql("""
//                |insert overwrite table dwd.wd_organization_sts
//                |select md5(org_name) as org_id,
//                |social_credit_code,
//                |org_name,
//                |null as en_name,
//                |null as alias,
//                |concat_ws('-',substr(registration_date,1,4),substr(registration_date,5,2),substr(registration_date,7,2)) as registration_date,
//                |"Company" as org_type,
//                |null as nature,
//                |null as isLegalPersonInstitution,
//                |legal_person,
//                |null as belongtocode,
//                |address,
//                |'中国' as  country,
//                |split(merger_name,',')[1] as province,
//                |concat(if( length(if(address like '%市%', ifnull(split(split(address,'市')[0],'省')[1],split(address,'市')[0]),null )) <4,if(address like '%市%', ifnull(split(split(address,'市')[0],'省')[1],split(address,'市')[0]),null ),null),'市') as city,
//                |null as district,
//                |b.zip_code as zipcode,
//                |'sts' as source
//                |from  manual_organization_sts a
//                |left join (select * ,row_number()over(partition by short_name order by level_type,id) as tid from ods.o_const_china_region) b on if( length(if(address like '%市%', ifnull(split(split(address,'市')[0],'省')[1],split(address,'市')[0]),null )) <4,if(address like '%市%', ifnull(split(split(address,'市')[0],'省')[1],split(address,'市')[0]),null ),null)  = b.short_name and b.tid = 1
//                |""".stripMargin)



    spark.stop()


  }
}
