package cn.sks.dwb.person

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object PersonFusionCsai_4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("PersonFusionCsai_4")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanAllString",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })

    val person_origin=spark.sql(
      """
        |select
        | a.* ,b.zh_title as zh_title ,CleanAllString(b.zh_title) as clean_zh_title
        |from dwb.wb_person_nsfc_sts_academician a left join dwd.wd_product_person_ext_nsfc b
        |on a.person_id = b.person_id
      """.stripMargin)
    person_origin.createOrReplaceTempView("person_origin")

    val person_csai =spark.sql(
      """
        |select
        | a.* ,b.zh_title as zh_title ,CleanAllString(b.zh_title) as clean_zh_title
        |from dwd.wd_person_csai a left join dwd.wd_product_person_ext_csai  b
        |on a.person_id = b.person_id
      """.stripMargin)


    person_csai.createOrReplaceTempView("person_csai")
    val person_csai_zh = spark.sql("select * from person_csai where zh_title is not null ")
    person_csai_zh.createOrReplaceTempView("person_csai_zh")

//    //61636265
//    println(person_csai_zh.select("person_id").distinct().count())

    val rel_person= spark.sql(
      """
        |select * from (
        |  select  a.person_id as person_id, b.person_id as person_id_csai ,
        |  concat("{","\"from\"",":",b.source,",","\"to\"",":",a.source,",","\"rule\"",":","\"zh_name+zh_title\"","}") as source
        |  from  person_origin a join person_csai_zh b
        |  on a.zh_name=b.zh_name and a.clean_zh_title = b.clean_zh_title
        |  )a
        |group by person_id,person_id_csai,source
      """.stripMargin)
    rel_person.createOrReplaceTempView("rel_person")

//    println("--------------------rel_person-------------------------"+rel_person.count())
//    println("-------------person_id_nsfc--------------------------------"+rel_person.select("person_id").distinct().count())
//    println("-----------person_id_csai----------------------------------"+rel_person.select("person_id_csai").distinct().count())


    val person_csai_zh_not_exists = spark.sql("select * from  dwd.wd_person_csai a  where not exists (select * from rel_person b where a.person_id=b.person_id_csai)")
    person_csai_zh_not_exists.createOrReplaceTempView("person_csai_zh_not_exists")
//    println("person_csai_zh_not_exists-----"+person_csai_zh_not_exists.count())


    spark.sql("insert into table dwb.wb_person_nsfc_sts_academician_csai_rel select person_id_csai,person_id,source from rel_person")
    spark.sql(
      """
        | insert into table dwb.wb_person_nsfc_sts_academician_csai
        | select
        |     a.person_id
        |    ,zh_name
        |    ,en_name
        |    ,gender
        |    ,nation
        |    ,birthday
        |    ,birthplace
        |    ,org_id
        |    ,org_name
        |    ,dept_name
        |    ,idcard
        |    ,officerno
        |    ,passportno
        |    ,hkidno
        |    ,twidno
        |    ,position
        |    ,prof_title
        |    ,prof_title_id
        |    ,researcharea
        |    ,mobile
        |    ,tel
        |    ,email
        |    ,fax
        |    ,backupemail
        |    ,address
        |    ,nationality
        |    ,province
        |    ,city
        |    ,postcode
        |    ,avatar_url
        |    ,degree
        |    ,degreeyear
        |    ,degreecountry
        |    ,major
        |    ,brief_description
        |    ,if(b.source is null ,a.source,b.source) as source
        | from dwb.wb_person_nsfc_sts_academician a
        | left join rel_person b
        | on a.person_id = b.person_id
      """.stripMargin)

    spark.sql(
      """
        |insert into table dwb.wb_person_nsfc_sts_academician_csai
        |select
        | person_id
        | ,zh_name
        | ,en_name
        | ,gender
        | ,nation
        | ,birthday
        | ,birthplace
        | ,null as org_id
        | ,org_name
        | ,null as dept_name
        | ,null as idcard
        | ,null as officerno
        | ,null as passportno
        | ,null as hkidno
        | ,null as twidno
        | ,null as position
        | ,prof_title
        | ,null as prof_title_id
        | ,null as researcharea
        | ,null as mobile
        | ,null as tel
        | ,null as email
        | ,null as fax
        | ,null as backupemail
        | ,null as address
        | ,nationality
        | ,null as province
        | ,null as city
        | ,null as postcode
        | ,null as avatar_url
        | ,null as degree
        | ,null as degreeyear
        | ,null as degreecountry
        | ,null as major
        | ,brief_description
        | ,source
        |from person_csai_zh_not_exists
      """.stripMargin)





  }

}
