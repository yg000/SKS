package cn.sks.ods.ms

import cn.sks.dwb.organization.org_dwb.spark
import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object ReBuildPersonID {
  val spark = SparkSession.builder()
    .master("local[20]")
    .appName("org_dm")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    .config("spark.sql.shuffle.partitions","10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")


  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })

  def main(args: Array[String]): Unit = {

//    id                  	string
//      achivement_id       	string
//      person_id           	string
//      person_name         	string
//      org_id              	string
//      org_name            	string
//      isfirstauthor       	string
//      source              	string


//    spark.sql(
//      """
//        |select count(distinct achivement_id,person_name) from ods.o_ms_product_author_copy where org_name is null
//        |""".stripMargin)


    import spark.sql
    sql(
      """
        |
        |""".stripMargin)


//    spark.sql(
//      """
//        |insert overwrite table  ods.o_ms_product_author
//        |select
//        |id,
//        |achivement_id,
//        |md5(concat(person_name,clean_fusion(org_name))) as person_id,
//        |person_name,
//        |org_id,
//        |org_name,
//        |isfirstauthor,
//        |source
//        | from ods.o_ms_product_author_copy where org_name is not null
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |insert into table  ods.o_ms_product_author
//        |select
//        |id,
//        |achivement_id,
//        |md5(concat(person_name,achivement_id)) as person_id,
//        |person_name,
//        |org_id,
//        |org_name,
//        |isfirstauthor,
//        |source
//        | from ods.o_ms_product_author_copy where org_name is null
//        |""".stripMargin)

    spark.read.table("ods.o_ms_product_author").select("person_id","person_name","source").dropDuplicates("person_id").createOrReplaceTempView("o_ms_product_author")
    spark.sql(
      """
        |insert overwrite table  ods.o_ms_product_person
        | select null,person_id,person_name,source from o_ms_product_author
        |""".stripMargin)



    spark.read.table("ods.o_ms_product_author").select("person_id","org_id","org_name","source").dropDuplicates("person_id","org_name").createOrReplaceTempView("o_ms_product_author")
    spark.sql(
      """
        |insert overwrite table  ods.o_ms_product_person_work_experience
        | select person_id,org_id,org_name,source from o_ms_product_author where org_name is not null
        |""".stripMargin)

  }
}
