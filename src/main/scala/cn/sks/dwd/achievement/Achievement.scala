package cn.sks.dwd.achievement

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object Achievement {
  def main(args: Array[String]): Unit = {
    achievement_to_dwd()
  }
  def achievement_to_dwd() = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("achievement_to_dwd")
      .config("spark.deploy.mode", "4g")
      .config("spark.drivermemory", "32g")
      .config("spark.cores.max", "16")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()
    spark.sqlContext.udf.register("clean_div",(str:String) =>{
      DefineUDF.clean_div(str)
    })
    spark.sqlContext.udf.register("clean_separator_authors",(authors:String)=>{
      if (authors== null) null
      else DefineUDF.ToDBC(DefineUDF.clean_separator(authors))
    })

    spark.sql(
      """
        |insert into table  dwd.wd_product_all_nsfc
        |select achievement_id, '1' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,  'product_business' as source from ods.o_nsfc_project_journal      union all
        |select achievement_id, '2' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,  'product_business' as source from ods.o_nsfc_project_conference       union all
        |select achievement_id, '3' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,  'product_business' as source from ods.o_nsfc_project_reward  union all
        |select achievement_id, '4' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,  'product_business' as source from ods.o_nsfc_project_monograph    union all
        |select achievement_id, '5' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,  'product_business' as source from ods.o_nsfc_project_patent
        |union all
        |select achievement_id, '1' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'product_person' as source from ods.o_nsfc_product_journal               union all
        |select achievement_id, '2' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'product_person' as source from ods.o_nsfc_product_conference   union all
        |select achievement_id, '3' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'product_person' as source from ods.o_nsfc_product_reward     union all
        |select achievement_id, '4' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'product_person' as source from ods.o_nsfc_product_monograph    union all
        |select achievement_id, '5' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'product_person' as source from ods.o_nsfc_product_patent                union all
        |select achievement_id, '6' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'product_person' as source from ods.o_nsfc_product_criterion
        |union all
        |select achievement_id, '1' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'npd' as source  from  ods.o_nsfc_npd_journal      union all
        |select achievement_id, '2' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'npd' as source  from  ods.o_nsfc_npd_conference   union all
        |select achievement_id, '3' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'npd' as source  from  ods.o_nsfc_npd_reward        union all
        |select achievement_id, '4' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'npd' as source  from  ods.o_nsfc_npd_monograph         union all
        |select achievement_id, '5' as product_type, clean_div(zh_title) as zh_title, clean_div(en_title) as en_title, clean_separator_authors(clean_div(authors)) as authors,   'npd' as source  from  ods.o_nsfc_npd_patent
      """.stripMargin)

    spark.sql(
      """
        |insert into table dwd.wd_product_person_ext_nsfc
        |select
        |   a.person_id
        |  ,a.achievement_id
        |  ,a.product_type
        |  ,b.zh_name
        |  ,null as en_name
        |  ,c.zh_title
        |  ,c.en_title
        |  ,c.authors
        |  ,'nsfc' as source
        |from ods.o_nsfc_product_person a
        |left join dwd.wd_person_nsfc b
        | on a.person_id=b.person_id
        |left join dwd.wd_product_all_nsfc  c
        |on a.achievement_id=c.achievement_id and a.product_type=c.product_type
      """.stripMargin)

  }
}
