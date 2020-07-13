package cn.sks.dwb.relation

import org.apache.spark.sql.SparkSession

object RelPersonSubject {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("RelPersonSubject")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val product_person= spark.sql(
      """
        |select * from dwb.wb_product_person
      """.stripMargin)
    product_person.createOrReplaceTempView("product_person")

    val product_subject= spark.sql(
      """
        |select * from dwb.wb_product_all_subject
      """.stripMargin)
    product_subject.createOrReplaceTempView("product_subject")

    product_subject.show(3)
    println(product_subject.count())



//    1 1170 4520
    val person_subject =spark.sql(
      """
        |
        |select b.person_id ,a.*  from product_subject a left join product_person b
        | on a.achievement_id=b.achievement_id where person_id is not null
        |
      """.stripMargin).drop("achievement_id")
    person_subject.createOrReplaceTempView("person_subject")
    println(person_subject.count())

    person_subject.show(3)

    spark.sql("insert into table dwb.wb_person_subject  select * from person_subject")










  }



}
