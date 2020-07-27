package cn.sks.translate

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.sks.BaiduTranslate.baidu.Baidu_Translate
import org.apache.spark.sql.SparkSession

object product_keywords_nsfc_clean_translate {
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

    val csv= spark.read.csv("src/main/resources/product_keywords_tran.csv")
    csv.toDF("key").createOrReplaceTempView("translate")
    spark.sql(
      """
        |select
        |split(key,"¤")[0] as product_id,
        |split(key,"¤")[1] as zh_keywords,
        |split(key,"¤")[2] as en_keywords
        | from translate
        |""".stripMargin).createOrReplaceTempView("translated")

    spark.sql("insert overwrite table nsfc.wd_product_keywords_not_translate_nsfc " +
      "select * from nsfc.wd_product_keywords_nsfc_zh_null a where not exists " +
      "(select * from translated b where a.product_id=b.product_id)")

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
    val sql="select * from nsfc.wd_product_keywords_not_translate_nsfc"

    val con=DriverManager.getConnection(jdbcUrl,hiveUser,hivePassword)
    val ptsm=con.prepareStatement(sql)
    val rs=ptsm.executeQuery()

    val bw:BufferedWriter=new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream("src/main/resources/product_keywords_tran.csv",true)))

    val list=new util.ArrayList[String]()
    while(rs.next()){
        Thread.sleep(1000)
      val key=rs.getString(3)
      val keywords_zh_en=rs.getString(1)+"¤"+Baidu_Translate.translate(key,"zh")+"¤"+rs.getString(3)+"\n"
//      println(key)
//      list.add(keywords_zh_en)
      bw.write(keywords_zh_en)
//      for(i <- 0 until list.size()){
//        if (i%1000==0){
      bw.flush()
//        }
//      }
    }




    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
  }
}
