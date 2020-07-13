package cn.sks.dwb.relation

import org.apache.spark.sql.SparkSession

object RelPersonProduct {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("RelPerson")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val product_all_person= spark.sql(
      """
        |select * from dwb.wb_product_all_person
      """.stripMargin)
    product_all_person.createOrReplaceTempView("product_all_person")


    val academician_rel = spark.sql(
      """
        |select person_id_academician,person_id from dwb.wb_person_nsfc_sts_academician_rel
      """.stripMargin).dropDuplicates("person_id_academician","person_id")
    academician_rel.createOrReplaceTempView("academician_rel")

    val csai_rel= spark.sql(
      """
        |select person_id_csai,person_id from dwb.wb_person_nsfc_sts_academician_csai_rel
      """.stripMargin).dropDuplicates("person_id_csai","person_id")
    csai_rel.createOrReplaceTempView("csai_rel")


    val ms_rel =spark.sql(
      """
        |select person_id_ms,person_id from dwb.wb_person_nsfc_sts_academician_csai_ms_rel
      """.stripMargin).dropDuplicates("person_id_ms","person_id")
    ms_rel.createOrReplaceTempView("ms_rel")

    val product_all_person_academician = spark.sql(
      """
        |   select if(b.person_id is null,a.person_id,b.person_id) as  person_id ,achievement_id,product_type
        |   from product_all_person a left join academician_rel b on a.person_id =b.person_id_academician
      """.stripMargin)
    product_all_person_academician.createOrReplaceTempView("product_all_person_academician")

    val product_all_person_csai = spark.sql(
      """
        |   select if(b.person_id is null,a.person_id,b.person_id) as  person_id ,achievement_id,product_type
        |   from product_all_person_academician a left join csai_rel b on a.person_id =b.person_id_csai
      """.stripMargin)
    product_all_person_csai.createOrReplaceTempView("product_all_person_csai")


    val product_person_ms = spark.sql(
      """
        |   select if(b.person_id is null,a.person_id,b.person_id) as  person_id ,achievement_id,product_type
        |   from product_all_person_csai a left join ms_rel b on a.person_id =b.person_id_ms
      """.stripMargin)
    product_person_ms.createOrReplaceTempView("product_person_ms")

    //    product_person_ms 187045824    product_all_person  183096180
    spark.sql("insert into table dwb.wb_product_person  select person_id,achievement_id,product_type from product_person_ms")
















  }



}
