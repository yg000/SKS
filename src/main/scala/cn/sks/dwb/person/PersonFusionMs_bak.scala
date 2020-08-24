package cn.sks.dwb.person

import cn.sks.util.{DefineUDF, NameToPinyinUtil}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
object PersonFusionMs_bak {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("PersonFusionMs")
      .config("spark.deploy.mode","16g")
      .config("spark.drivermemory","96g")
      .config("spark.cores.max","32")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.udf.register("CleanFusion",(str:String) =>{
      DefineUDF.clean_fusion(str)
    })
    //62462674
    val origin = spark.sql("select person_id,zh_name,source from dwb.wb_person_nsfc_sts_academician_csai")
    val person_to_pinyin = NameToPinyinUtil.nameToPinyin(spark,origin,"zh_name")
    person_to_pinyin.createOrReplaceTempView("person_to_pinyin")

    val person_origin= spark.sql(
      """
        |select * ,CleanFusion(zh_name) as clean_zh_name,
        | CleanFusion(en_name_normal) as clean_en_name_normal,
        | CleanFusion(en_name_inverted) as clean_en_name_inverted
        |from person_to_pinyin
        |""".stripMargin).drop("en_name_normal").drop("en_name_inverted")
    person_origin.createOrReplaceTempView("person_origin")


    val person_ms = spark.sql(
      """
        |select *,CleanFusion(zh_name) as clean_zh_name
        | from dwd.wd_person_ms
        |""".stripMargin).dropDuplicates("person_id")
    person_ms.createOrReplaceTempView("person_ms")

    // 187866707
    val product_person_ext = spark.sql(
      """
        |select person_id,CleanFusion(zh_title) as clean_zh_title from dwd.wd_product_person_ext_nsfc
        | union all
        |select person_id,CleanFusion(zh_title) as clean_zh_title from dwd.wd_product_person_ext_csai
        |""".stripMargin)
    product_person_ext.createOrReplaceTempView("product_person_ext")
    println(product_person_ext.count())


    val person_origin_title = spark.sql(
      """
        |select a.*,clean_zh_title from person_origin a left join product_person_ext b on a.person_id =b.person_id  where clean_zh_title is not null
        |""".stripMargin).cache()
    person_origin_title.createOrReplaceTempView("person_origin_title")
    person_origin_title.show(3)

    println("person_ms_title")
    val person_ms_title =spark.sql(
      """
        |select a.*,clean_zh_title from person_ms a left join product_person_ext b on a.person_id =b.person_id
        |""".stripMargin).cache()
    person_ms_title.createOrReplaceTempView("person_ms_title")


    // 1---- clean_zh_name  clean_zh_title
    val rel_person_zh= spark.sql(
      """
        |select * from (
        |  select
        |  a.person_id as person_id_ms,b.person_id  as person_id,
        |  concat("{","\"from\"",":",a.source,",","\"to\"",":",b.source,",","\"rule\"",":","\"zh_name+zh_title\"","}") as source
        |  from  person_ms_title a join person_origin_title b
        |  on a.clean_zh_name=b.clean_zh_name and a.clean_zh_title= b.clean_zh_title
        |  )a
        |group by person_id_ms,person_id,source
      """.stripMargin)
    rel_person_zh.createOrReplaceTempView("rel_person_zh")

    // rel_person_zh  person_id_ms  person_id
    //3987   3164   1120

    val except_zh = spark.sql("select a.* from person_ms_title a where not exists (select * from rel_person_zh b where a.person_id=b.person_id)")
    except_zh.createOrReplaceTempView("except_zh")

    // 2---- clean_en_name_inverted  clean_zh_title
    println("clean_en_name_inverted")
    val rel_person_inverted= spark.sql(
      """
        |select * from (
        |  select
        |  a.person_id as person_id_ms,b.person_id  as person_id,
        |  concat("{","\"from\"",":",a.source,",","\"to\"",":",b.source,",","\"rule\"",":","\"zh_name+zh_title\"","}") as source
        |  from  except_zh a join person_origin_title b
        |  on a.clean_zh_name=b.clean_en_name_inverted and a.clean_zh_title= b.clean_zh_title
        |  )a
        |group by person_id_ms,person_id,source
      """.stripMargin)
    rel_person_inverted.createOrReplaceTempView("rel_person_inverted")


    // 52081022
    val except_inverted = spark.sql("select a.* from person_ms_title a where not exists (select * from rel_person_inverted b where a.person_id=b.person_id)")
    except_inverted.createOrReplaceTempView("except_inverted")
    println(except_inverted.count())


//    2773231    490076   216250
    println(rel_person_inverted.count())
    println(rel_person_inverted.select("person_id_ms").distinct().count())
    println(rel_person_inverted.select("person_id").distinct().count())


    // 3---- clean_en_name_normal  clean_zh_title
    val rel_person_normal= spark.sql(
      """
        |select * from (
        |  select
        |  a.person_id as person_id_ms,b.person_id  as person_id,
        |  concat("{","\"from\"",":",a.source,",","\"to\"",":",b.source,",","\"rule\"",":","\"zh_name+zh_title\"","}") as source
        |  from  person_ms_title a join person_origin_title b
        |  on a.clean_zh_name=b.clean_en_name_normal and a.clean_zh_title= b.clean_zh_title
        |  )a
        |group by person_id_ms,person_id,source
      """.stripMargin)
    rel_person_normal.createOrReplaceTempView("rel_person_normal")

//    240807   30443   19298

    println("clean_en_name_normal")
    println(rel_person_normal.count())
    println(rel_person_normal.select("person_id_ms").distinct().count())
    println(rel_person_normal.select("person_id").distinct().count())


    rel_person_zh.show(5)
    rel_person_zh.take(5).foreach(println)

    rel_person_inverted.show(5)
    rel_person_inverted.take(5).foreach(println)

    rel_person_normal.show(5)

    rel_person_normal.take(5).foreach(println)

    val rel_person: Dataset[Row] = rel_person_zh.union(rel_person_inverted).union(rel_person_normal)
    rel_person.createOrReplaceTempView("rel_person")


    spark.sql("insert into table dwb.wb_person_nsfc_sts_academician_csai_ms_rel select person_id_ms,person_id,source from rel_person")

    spark.sql(
      """
        | insert into table dwb.wb_person_nsfc_sts_academician_csai_ms
        | select
        |     a.person_id
        |    ,zh_name
        |    ,en_name
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
        | from dwb.wb_person_nsfc_sts_academician_csai a
        | left join rel_person b
        | on a.person_id = b.person_id
      """.stripMargin)

    val person_ms_not_exists = spark.sql("select * from dwd.wd_person_ms a where not exists (select * from rel_person b where a.person_id=b.person_id_ms)")
    person_ms_not_exists.createOrReplaceTempView("person_ms_not_exists")

    spark.sql(
      """
        |insert into table dwb.wb_person_nsfc_sts_academician_csai_ms
        |select
        |  person_id
        | ,zh_name
        | ,null as en_name
        | ,null as gender
        | ,null as nation
        | ,null as birthday
        | ,null as birthplace
        | ,org_id
        | ,org_name
        | ,null as dept_name
        | ,null as idcard
        | ,null as officerno
        | ,null as passportno
        | ,null as hkidno
        | ,null as twidno
        | ,null as position
        | ,null as prof_title
        | ,null as prof_title_id
        | ,null as researcharea
        | ,null as mobile
        | ,null as tel
        | ,null as email
        | ,null as fax
        | ,null as backupemail
        | ,null as address
        | ,null as nationality
        | ,null as province
        | ,null as city
        | ,null as postcode
        | ,null as avatar_url
        | ,null as degree
        | ,null as degreeyear
        | ,null as degreecountry
        | ,null as major
        | ,null as brief_description
        | ,source
        |from person_ms_not_exists
      """.stripMargin)







  }
}

