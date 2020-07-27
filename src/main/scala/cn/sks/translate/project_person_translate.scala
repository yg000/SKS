package cn.sks.translate

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.sks.BaiduTranslate.baidu.Baidu_Translate
import org.apache.spark.sql.SparkSession

object project_person_translate {
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

//    val csv= spark.read.csv("src/main/resources/csai_keywords_concat2.csv")
//    csv.toDF("key").createOrReplaceTempView("translate")
//    spark.sql(
//      """
//        |select
//        |split(key,"¤")[0] as keywords_id,
//        |split(key,"¤")[1] as zh_keywords,
//        |split(key,"¤")[2] as en_keywords,
//        |'csai' as source
//        | from translate
//        |""".stripMargin).createOrReplaceTempView("translated")
//    spark.sql("insert overwrite table nsfc.o_csai_keyword_translate select * from translated")
//
//    spark.sql("insert overwrite table nsfc.o_csai_keyword_not_translate " +
//      "select * from nsfc.o_csai_keywords_concat_2 a where not exists " +
//      "(select * from translated b where a.rank=b.keywords_id)")

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
    val sql="select * from dwd.wd_project_person_keywords_split_nsfc where zh_keywords='null' "

    val con=DriverManager.getConnection(jdbcUrl,hiveUser,hivePassword)
    val ptsm=con.prepareStatement(sql)
    val rs=ptsm.executeQuery()

    val bw:BufferedWriter=new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream("src/main/resources/project_person_keywords.csv",true)))

    val list=new util.ArrayList[String]()

    while(rs.next()){
      Thread.sleep(1000)

      val key=rs.getString(5)

      val keywords_zh_en=rs.getString(1)+"¤"+rs.getString(2)+"¤"+rs.getString(3)
        "¤"+Baidu_Translate.translate(key,"en")+rs.getString(5)+"¤"+rs.getString(6)+"¤"+rs.getString(7)+"\n"


//      println(keywords_zh_en)
      bw.write(keywords_zh_en)

      bw.flush()

    }


    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
  }
}
