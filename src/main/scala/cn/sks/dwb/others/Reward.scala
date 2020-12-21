package cn.sks.dwb.others

import org.apache.spark.sql.{Column, SparkSession}
import cn.sks.util.{AchievementUtil, DefineUDF}

/*

论文数据的整合的整体的代码

 */
object Reward {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[12]")
      .config("spark.deploy.mode", "8g")
      .config("spark.drivermemory", "32g")
      .config("spark.cores.max", "16")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .appName("conference")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")


    spark.udf.register("clean_div", DefineUDF.clean_div _)

//    //项目产出成果===================
    val product_reward_ndp = spark.sql(
      """
        |select
        |achievement_id
        |,clean_div(zh_title) zh_title
        |,clean_div(authors)  person_name
        |from ods.o_npd_award_nsfc
      """.stripMargin)

    //人产出成果-论文
    val business_re_nsfc = spark.sql(
      """
        |select
        |achievement_id
        |,clean_div(zh_title) zh_title
        |,clean_div(authors) person_name
        |from ods.o_product_business_award_nsfc
      """.stripMargin)

    val person_reward = spark.sql(
      """
        |select
        |achievement_id
        |,clean_div(zh_title) zh_title
        |,clean_div(authors) person_name
        |from ods.o_product_scientific_reward_nsfc
      """.stripMargin)
    //=================================修改融合策略=============================================================
    //将基金委对应的论文成果对应的作者和论文的字段合并到一块儿
    person_reward.unionAll(business_re_nsfc).unionAll(product_reward_ndp).dropDuplicates("achievement_id")
      .select("achievement_id", "zh_title", "person_name")
      .repartition(2).createOrReplaceTempView("person_product_reward")

    spark.sql("insert overwrite table dwd.wd_product_fusion_data_reward_nsfc  select * from person_product_reward")


    //融合的
    val person_product_reward_csai = spark.sql(
      """
        |select
        |achievement_id as achievement_id_csai
        |,product_type
        |,lower(translate(zh_name," ","")) as person_name
        |,lower(translate(zh_title," ","")) as  zh_title
        |from dwd.wd_product_person_ext_csai where product_type='3'
      """.stripMargin).createOrReplaceTempView("person_product_re_csai")

    val person_product_reward_nsfc = spark.sql(
      """
        |select
        |achievement_id as achievement_id_nsfc
        |,lower(translate(zh_title," ","")) zh_title
        |,lower(translate(person_name," ","")) person_name
        |from dwd.wd_product_fusion_data_reward_nsfc
      """.stripMargin).createOrReplaceTempView("person_product_re_nsfc")

    //
    spark.sql(
      """
        |select
        |b.achievement_id_csai
        |,a.achievement_id_nsfc
        |,"4" as product_type
        |from person_product_re_nsfc a join person_product_re_csai b on a.zh_title=b.zh_title where a.person_name like concat("%",b.person_name,"%")
      """.stripMargin)
      .dropDuplicates("achievement_id_nsfc")
      .select("achievement_id_csai", "achievement_id_nsfc", "product_type")
      .repartition(1)
      .createOrReplaceTempView("product_ronghe_re")

    spark.sql("insert overwrite table dwb.wb_product_reward_csai_nsfc_rel  select * from product_ronghe_re")

    //合并基金委和科协的论文的成果数据
    val rel_co = spark.sql("select achievement_id_nsfc,achievement_id_nsfc as achievement_id from dwb.wb_product_reward_csai_nsfc_rel")


    val col_1: Column = new Column("achievement_id_nsfc")

    val wb_business_re = spark.sql("select * from wd_product_reward_business_nsfc")
    val wb_re = spark.sql("select * from wd_product_reward_nsfc")
    val wb_npd_re = spark.sql("select * from wd_product_reward_npd_nsfc")
    val wb_scai_re = spark.sql("select * from wd_product_reward_csai")


    val co_union = wb_scai_re.union(wb_npd_re).union(wb_re)
      .union(wb_business_re).dropDuplicates("achievement_id")

    co_union.join(rel_co, Seq("achievement_id"), "left")
      .filter(col_1.isNull).drop("achievement_id_nsfc")
      .repartition(8)
      .createOrReplaceTempView("re_union_co")

    spark.sql("insert overwrite table dwb.wb_product_reward_csai_nsfc select * from re_union_co")


    spark.stop()


  }
}
