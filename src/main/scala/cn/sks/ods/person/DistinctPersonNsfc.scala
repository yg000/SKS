package cn.sks.ods.person

import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession
object DistinctPersonNsfc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("test")
      .config("spark.deploy.mode","4g")
      .config("spark.drivermemory","16g")
      .config("spark.cores.max","8")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields", "100")
      .enableHiveSupport()
      .getOrCreate()


    println("[---------------------------- 不要跑")
    while(true){

    }


    spark.sqlContext.udf.register("CleanAll", (str: String) => {
      DefineUDF.clean_fusion(str)
    })

    val origin_person = spark.sql(
      """
        |select psn_code,zh_name,card_code,email,mobile,CleanAll(card_code) as clean_card,CleanAll(email) as clean_email,CleanAll(mobile) as clean_mobile
        |from nsfc_middle.m_person_new   where length(psn_code)<30   limit 1000
        |""".stripMargin)
    origin_person.createOrReplaceTempView("origin_person")
    origin_person.show()
    //    psn_code  zh_name card_code mobile  email  org_name birthday

    // 1、card
    val card_exists= spark.sql("select * from origin_person where card_code is not null ")
    card_exists.createOrReplaceTempView("card_exists")

    val card_exists_dis = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by zh_name,clean_card order by psn_code ) rank from card_exists
        |)a where rank =1
      """.stripMargin).drop("rank")
    card_exists_dis.createOrReplaceTempView("card_exists_dis")

    val rel_card = spark.sql(
      """
        |select a.psn_code as old ,b.psn_code as new from card_exists a left join card_exists_dis b
        | on a.zh_name=b.zh_name and a.clean_card =b.clean_card
        |""".stripMargin)
    val card_dis= origin_person.except(card_exists).union(card_exists_dis).cache()
    card_dis.createOrReplaceTempView("card_dis")


    // 2、email
    val email_exists = spark.sql("select *  from card_dis where email is not null").cache()
    email_exists.createOrReplaceTempView("email_exists")

    val email_exists_dis = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by zh_name,clean_email order by psn_code ) rank from email_exists
        |)a where rank =1
      """.stripMargin).drop("rank")
    email_exists_dis.createOrReplaceTempView("email_exists_dis")

    val rel_email = spark.sql(
      """
        |select a.psn_code as old ,b.psn_code as new from email_exists a left join email_exists_dis b
        | on a.zh_name=b.zh_name and a.clean_email =b.clean_email
        |""".stripMargin)
    println(rel_email.count())

    val email_dis= card_dis.except(email_exists).union(email_exists_dis).cache()
    email_dis.createOrReplaceTempView("email_dis")


    // 3、mobile
    val mobile_exists = spark.sql("select *  from emial_dis where mobile is not null").cache()
    mobile_exists.createOrReplaceTempView("mobile_exists")

    val mobile_exists_dis = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by zh_name,clean_mobile order by psn_code ) rank from mobile_exists
        |)a where rank =1
      """.stripMargin).drop("rank")
    mobile_exists_dis.createOrReplaceTempView("mobile_exists_dis")

    val rel_mobile = spark.sql(
      """
        |select a.psn_code as old ,b.psn_code as new from mobile_exists a left join mobile_exists_dis b
        | on a.zh_name=b.zh_name and a.clean_mobile =b.clean_mobile
        |""".stripMargin)
    println(rel_mobile.count())

    val mobile_dis= email_dis.except(mobile_exists).union(mobile_exists_dis).cache()
    println(mobile_dis.count())

    // org_name birthday







  }

}
