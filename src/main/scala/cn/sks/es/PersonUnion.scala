package cn.sks.es

import org.apache.spark.sql.SparkSession

object PersonUnion {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      //.master("local[40]")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("neo4jcsv")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val person = spark.sql("select * from dm.dm_es_person").drop("org_id")

    val org = spark.sql("select id as org_id,org_name from dm.dm_es_organization")


    val keyword = spark.sql("select keyword_id,keyword from dm.dm_neo4j_keyword")
    val person_keyword = spark.sql("select person_id,keywords_id as keyword_id from dm.dm_neo4j_person_keywords")

    val subject = spark.sql("select two_rank_id,two_rank_name from dm.dm_neo4j_subject")
    val person_subject = spark.sql("select person_id,two_rank_id keyword from dm.dm_neo4j_person_subject")

    person_keyword.join(keyword,Seq("keyword_id")).createOrReplaceTempView("keyword")

    val keyword_2 = spark.sql(
      """
        |select
        |person_id as id ,
        |concat("[",concat_ws(",",collect_set(to_json(struct(keyword as name,keyword_id as id)))),"]")  as keyword_2
        |from keyword group by person_id
      """.stripMargin)

    person_subject.join(subject,Seq("two_rank_id")).createOrReplaceTempView("subject")

    val subject_2 = spark.sql(
      """
        |select
        |person_id as id ,
        |concat("[",concat_ws(",",collect_set(to_json(struct(two_rank_name as name,two_rank_id as id)))),"]")  as subject_2
        |from subject group by person_id
      """.stripMargin)


    person.join(org,Seq("org_name"),"left")
      .join(keyword_2,Seq("id"),"left")
      .join(subject_2,Seq("id"),"left")
      .createOrReplaceTempView("person")

    spark.sql(
      """
        |select
        |id
        |,zh_name
        |,en_name
        |,gender
        |,nation
        |,birthday
        |,birthplace
        |,org_id
        |,org_name
        |,dept_name
        |,idcard
        |,officerno
        |,passportno
        |,hkidno
        |,twidno
        |,position
        |,prof_title
        |,prof_title_id
        |,researcharea
        |,keyword_2 as keyword
        |,subject_2 as subject
        |,mobile
        |,tel
        |,email
        |,fax
        |,backupemail
        |,address
        |,nationality
        |,province
        |,city
        |,postcode
        |,avatar_url
        |,degree
        |,degreeyear
        |,degreecountry
        |,major
        |,brief_description
        |,source
        |from person
      """.stripMargin).repartition(250).createOrReplaceTempView("result")

    spark.sql("insert overwrite table dm.dm_es_person_union select * from result")

  }
}
