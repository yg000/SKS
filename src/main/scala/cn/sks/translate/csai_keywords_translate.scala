package cn.sks.translate

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.sks.BaiduTranslate.baidu.Baidu_Translate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object csai_keywords_translate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("product_keywords_nsfc_clean_translate")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "16g")
      .config("spark.cores.max", "8")
      .config("spark.rpc.askTimeout", "300")
      .config("hive.metastore.uris", "thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields", "200")
      .config("spark.driver.maxResultSize", "4G")
      .config("sethive.enforce.bucketing", "true")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
//
    val csv= spark.read.csv("src/main/resources/csai_keywords_final_01.csv")

    csv.toDF("key").createOrReplaceTempView("translate")

    spark.sql(
      """
        |select
        |split(key,"¤")[0] as keywords_id,
        |split(key,"¤")[1] as zh_keywords,
        |split(key,"¤")[2] as en_keywords,
        |'csai' as source
        | from translate
        |""".stripMargin).createOrReplaceTempView("translated")
    spark.sql("insert into table nsfc.o_csai_keyword_translate_final select * from translated  ")

//    spark.sql("select sum(size(split(zh_keywords,';'))) from translated where size(split(en_keywords,';')) <10 or size(split(en_keywords,';')) >10 ").show()
    spark.sql("select * from translated where en_keywords ='null' ").show()
    println("=================================")
    while(true){}
//
//    spark.sql("insert overwrite table nsfc.o_csai_keyword_not_translate " +
//      "select * from nsfc.o_csai_keywords_concat_50_20 a where not exists " +
//      "(select * from translated b where a.keywords_id=b.keywords_id)")

    println("================start translate==================")
    import java.sql.DriverManager

    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
    }catch {
      case ex:ClassNotFoundException=>
    }

    val jdbcUrl="jdbc:hive2://10.0.82.132:10000/nsfc"
    val hiveUser="root"
    val hivePassword="123456"
    val sql="select * from nsfc.o_csai_keyword_translate_final where en_keywords ='null'"

    val con=DriverManager.getConnection(jdbcUrl,hiveUser,hivePassword)
    val ptsm=con.prepareStatement(sql)
    val rs=ptsm.executeQuery()

    val bw:BufferedWriter=new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream("src/main/resources/csai_keywords_final_01.csv",true)))

    val list=new util.ArrayList[String]()

    while(rs.next()){
      Thread.sleep(1000)

      val key=rs.getString(2)

      val keywords_zh_en=rs.getString(1)+"¤"+rs.getString(2)+
        "¤"+Baidu_Translate.translate(key,"en")+"\n"


//      println(keywords_zh_en)
      bw.write(keywords_zh_en)

      bw.flush()

    }


    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
  }
}
