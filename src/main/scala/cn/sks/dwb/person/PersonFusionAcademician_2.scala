package cn.sks.dwb.person


import cn.sks.util.DefineUDF
import org.apache.spark.sql.{DataFrame, SparkSession}

object PersonFusionAcademician_2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("PersonFusionAcademician")
      .config("spark.deploy.mode","4g")
      .config("spark.drivermemory","32g")
      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val person_academician: DataFrame = spark.sql("select * from dwd.wd_person_academician ")
      .drop("outstanding_title","academician_selection_date").dropDuplicates("person_id").cache()
    person_academician.createOrReplaceTempView("person_academician")

    val person_origin  = spark.sql("select * from dwb.wb_person_nsfc_sts ").cache()
    person_origin.createOrReplaceTempView("person_origin")

    // 1,  Personnel fusion (zh_name,org_name,birthday_year)
    val rel_fusion_org_birthday = spark.sql(
      """
        |select a.person_id as person_id_academician,b.person_id  as person_id_nsfc,
        | concat("{","\"from\"",":",a.source,",","\"to\"",":",b.source,",","\"rule\"",":","\"zh_name+org+birthday_year\"","}") as source
        |from person_academician a  join person_origin b
        | on a.zh_name=b.zh_name and trim(a.org_name)= trim(b.org_name)
        |and substr(a.birthday,0,4) = substr(b.birthday,0,4)
      """.stripMargin)
    rel_fusion_org_birthday.take(10).foreach(println)


    val person_not_exists = spark.sql(
      """
        |select * from person_academician a
        |where not exists (select * from person_origin b
        | where a.zh_name=b.zh_name and trim(a.org_name)= trim(b.org_name)
        |and substr(a.birthday,0,4) = substr(b.birthday,0,4)
        |)
      """.stripMargin).cache()
    person_not_exists.createOrReplaceTempView("person_not_exists")


    // 2,  Personnel fusion (zh_name,zh_title,birthday_year)
    spark.sqlContext.udf.register("cleanString", (str: String) => {
      DefineUDF.clean_fusion(str)
    })

    val person_academician_product = spark.sql("select a.*,b.zh_title,CleanString(zh_title) as clean_zh_title from person_not_exists a  left join dwd.wd_product_person_ext_csai b on a.person_id=b.person_id").cache()
    person_academician_product.createOrReplaceTempView("person_academician_product")
    val person_origin_product = spark.sql("select a.*,b.zh_title,CleanString(zh_title) as clean_zh_title from person_origin a left join dwd.wd_product_person_ext_nsfc b on a.person_id=b.person_id").cache()
    person_origin_product.createOrReplaceTempView("person_origin_product")

    val rel_fusion_product =spark.sql(
      """
        |select * from  (
        |  select a.person_id as person_id_academician,b.person_id as person_id_nsfc ,
        |    concat("{","\"from\"",":",a.source,",","\"to\"",":",b.source,",","\"rule\"",":","\"zh_name+zh_title\"","}") as source
        |   from person_academician_product  a
        |  join person_origin_product b
        |  on a.zh_name =b.zh_name and a.clean_zh_title=b.clean_zh_title
        |) group by person_id_nsfc,person_id_academician,source
      """.stripMargin).cache()
    rel_fusion_product.createOrReplaceTempView("rel_fusion_product")


    val person_academician_product_not_exists = spark.sql("select * from  person_academician_product a  where not exists (select * from rel_fusion_product b where a.person_id=b.person_id_academician)")
        .drop("zh_title","clean_zh_title").dropDuplicates("person_id")
    person_academician_product_not_exists.createOrReplaceTempView("person_academician_product_not_exists")

//    println("rel_fusion_product-------"+rel_fusion_product.count())
//    println("person_academician_product_not_exists-----"+person_academician_product_not_exists.count())


    // 3 --Write to hive library
    spark.sql("insert into table dwb.wb_person_nsfc_sts_academician_artificial select * from person_academician_product_not_exists")

    rel_fusion_org_birthday.union(rel_fusion_product).createOrReplaceTempView("rel_fusion_person_academician")
    spark.sql(
      """
        |insert into table dwb.wb_person_nsfc_sts_academician_rel
        | select person_id_academician,person_id_nsfc as person_id,source
        | from rel_fusion_person_academician
      """.stripMargin)


    spark.sql(
      """
        | insert into table dwb.wb_person_nsfc_sts_academician
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
        | from person_origin a
        | left join rel_fusion_person_academician b
        | on a.person_id = b.person_id_nsfc
      """.stripMargin)



  }

}
