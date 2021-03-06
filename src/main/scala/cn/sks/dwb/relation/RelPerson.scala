package cn.sks.dwb.relation

import org.apache.spark.sql.SparkSession

object RelPerson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.master("local[40]")
      .config("spark.deploy.mode", "clent")
      .config("executor-memory", "12g")
      .config("executor-cores", "6")
      .config("spark.local.dir", "/data/tmp")
      //      .config("spark.drivermemory", "32g")
      //      .config("spark.cores.max", "16")
      .config("spark.sql.shuffle.partitions", "120")
      .appName("RelPerson")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()



    //person_subject
    val product_person= spark.sql(
      """
        |select * from dwb.wb_relation_product_person
      """.stripMargin)
    product_person.createOrReplaceTempView("product_person")

    val product_subject= spark.sql(
      """
        |select * from dwb.wb_relation_product_subject
      """.stripMargin)
    product_subject.createOrReplaceTempView("product_subject")


//    1 1170 4520
    val person_subject =spark.sql(
      """
        |
        |select b.person_id ,a.*  from product_subject a left join product_person b
        | on a.achievement_id = b.achievement_id where person_id is not null
        |
      """.stripMargin).drop("achievement_id")
    person_subject.createOrReplaceTempView("person_subject")
    println(person_subject.count())

    spark.sql(
      """
        |select count(*) as one_rank_count,person_id,one_rank_id from person_subject group by person_id,one_rank_id
        |""".stripMargin).createOrReplaceTempView("one_rank_count")
    spark.sql(
      """
        |select count(*) as two_rank_count,person_id,two_rank_id from person_subject group by person_id,two_rank_id
        |""".stripMargin).createOrReplaceTempView("two_rank_count")

    spark.sql(
      """
        |select ifnull(d.person_id_to,a.person_id) as person_id,
        |a.one_rank_id,
        |one_rank_no,
        |one_rank_name,
        |a.two_rank_id,
        |two_rank_no,
        |two_rank_name,
        |b.one_rank_count,
        |c.two_rank_count
        |from person_subject a
        |left join one_rank_count b on a.person_id = b.person_id and a.one_rank_id = b.one_rank_id
        |left join two_rank_count c on a.person_id = c.person_id and a.two_rank_id = c.two_rank_id
        |left join dwb.wb_person_rel d on a.person_id = d.person_id_from
        |""".stripMargin)
      .dropDuplicates("person_id","two_rank_id")
      .repartition(30).write.format("hive").mode("overwrite").insertInto("dwb.wb_person_subject")




  }



}
