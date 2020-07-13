package cn.sks.dwb.person

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object PersonFusionSts_1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("PersonFusionSts_1")
      .config("spark.deploy.mode","4g")
      .config("spark.drivermemory","16g")
      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val person_nsfc = spark.sql("select * from dwd.wd_person_nsfc").cache()
    person_nsfc.createOrReplaceTempView("person_nsfc")
    val person_arp = spark.sql("select * from dwd.wd_person_arp").cache()
    person_arp.createOrReplaceTempView("person_arp")

    spark.sqlContext.udf.register("cleanString", (str: String) => {
      DefineUDF.clean_fusion(str)
    })

    // 1 -------- fusion (zh_name  email  )
    val rel_email = spark.sql(
      """
        |select * from (
        |select a.person_id as person_id_arp,b.person_id as person_id_nsfc ,
        | concat("{","\"from\"",":",a.source,",","\"to\"",":",b.source,",","\"rule\"",":","\"zh_name+email\"","}") as source
        | from person_arp a
        | join  person_nsfc b on a.zh_name=b.zh_name and cleanString(a.email)=cleanString(b.email) and a.email is not  null
        |)a group by person_id_arp,person_id_nsfc,source
      """.stripMargin)
    rel_email.createOrReplaceTempView("rel_email")

    println(rel_email.count())

    // 2 -------- fusion (zh_name  org_name   birthday)
    // 17675
    val email_not_exists = spark.sql("select * from person_arp a where not exists (select * from rel_email b where a.person_id=b.person_id_arp)")
    email_not_exists.createOrReplaceTempView("email_not_exists")

    // 17756
    val rel_org_birthday =spark.sql(
      """
        |select * from (
        |  select a.person_id as person_id_arp,b.person_id as person_id_nsfc ,
        |  concat("{","\"from\"",":",a.source,",","\"to\"",":",b.source,",","\"rule\"",":","\"zh_name+org_name+birthday_year\"","}") as source
        |  from email_not_exists a
        | join  person_nsfc b on a.zh_name=b.zh_name and cleanString(a.org_name)=cleanString(b.org_name) and substr(a.birthday,0,4) = substr(b.birthday,0,4)
        | and a.org_name is not null and a.birthday is not null
        |)a group by person_id_arp,person_id_nsfc,source
      """.stripMargin)
    rel_org_birthday.createOrReplaceTempView("rel_org_birthday")
    println(rel_org_birthday.count())

    // 23938
    val org_birthday_not_exists = spark.sql("select * from email_not_exists a where not exists (select * from rel_org_birthday b where a.person_id=b.person_id_arp)")
    org_birthday_not_exists.createOrReplaceTempView("org_birthday_not_exists")
    val rel_person = rel_email.union(rel_org_birthday)
    rel_person.createOrReplaceTempView("rel_person")
    println(org_birthday_not_exists.count())

    spark.sql("insert into table  dwb.wb_person_nsfc_sts_rel select person_id_arp,person_id_nsfc as person_id,source  from rel_person")

    spark.sql(
      """
        |insert into table dwb.wb_person_nsfc_sts
        | select
        |    person_id
        |   ,zh_name
        |   ,null as en_name
        |   ,gender
        |   ,null as nation
        |   ,birthday
        |   ,null as birthplace
        |   ,null as org_id
        |   ,org_name
        |   ,dept_name
        |   ,null as  idcard
        |   ,null as  officerno
        |   ,null as  passportno
        |   ,null as  hkidno
        |   ,null as  twidno
        |   ,position
        |   ,prof_title
        |   ,null as  prof_title_id
        |   ,null as  researcharea
        |   ,null as  mobile
        |   ,null as  tel
        |   ,email
        |   ,null as  fax
        |   ,null as  backupemail
        |   ,null as  address
        |   ,null as  nationality
        |   ,null as  province
        |   ,null as  city
        |   ,null as  postcode
        |   ,null as  avatar_url
        |   ,null as  degree
        |   ,null as  degreeyear
        |   ,null as  degreecountry
        |   ,null as  major
        |   ,null as  brief_description
        |   ,source
        |from org_birthday_not_exists
      """.stripMargin)

    spark.sql(
      """
        | insert into table dwb.wb_person_nsfc_sts
        | select
        |     a.person_id
        |    ,a.zh_name
        |    ,a.en_name
        |    ,if(c.gender is null,a.gender,c.gender) as gender
        |    ,a.nation
        |    ,if(c.birthday is null,a.birthday,c.birthday) as birthday
        |    ,a.birthplace
        |    ,if(c.org_name is null,a.org_id,null) as org_id
        |    ,if(c.org_name is null,a.org_name,c.org_name) as org_name
        |    ,if(c.dept_name is null,a.dept_name,c.dept_name) as dept_name
        |    ,a.idcard
        |    ,a.officerno
        |    ,a.passportno
        |    ,a.hkidno
        |    ,a.twidno
        |    ,a.position
        |    ,if(c.prof_title is null,a.prof_title,c.prof_title) as prof_title
        |    ,if(c.prof_title is null,a.prof_title_id,null) as prof_title_id
        |    ,a.researcharea
        |    ,a.mobile
        |    ,a.tel
        |    ,if(c.email is null,a.email,c.email) as email
        |    ,a.fax
        |    ,a.backupemail
        |    ,a.address
        |    ,a.nationality
        |    ,a.province
        |    ,a.city
        |    ,a.postcode
        |    ,a.avatar_url
        |    ,a.degree
        |    ,a.degreeyear
        |    ,a.degreecountry
        |    ,a.major
        |    ,a.brief_description
        |    ,if(b.source is null ,a.source,b.source) as source
        | from person_nsfc a
        | left join rel_person b
        | on a.person_id = b.person_id_nsfc
        | left join person_arp c
        | on c.person_id = b.person_id_arp
      """.stripMargin)




  }

}

