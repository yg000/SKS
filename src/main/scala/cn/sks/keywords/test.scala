package cn.sks.keywords

import java.util.regex.Pattern
import util.control.Breaks._

import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("OrcidXmlParse")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "16g")
      .config("spark.cores.max", "8")
      .config("spark.rpc.askTimeout","300")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .config("spark.debug.maxToStringFields","200")
      .config("spark.driver.maxResultSize","4G")
      .config("sethive.enforce.bucketing","true")
      .enableHiveSupport()
      .getOrCreate()

    val str = "U-238(O-18;O-16f)反应；..裂变碎片动能 分布；质量分布；U－239(n;理论;;f;erwe)截面；模型理论计算 asdva23r4.l/;p[pytsreWDSCB;SAEQadadsfdsa.."
    println(str.replaceAll("(.*)..","$1"))


    val str1 = str.replaceAll("(\\(.*);(.*);(.*\\))", "$1,$2,$3")

    println(str.stripSuffix(";"))

    spark.sqlContext.udf.register("CleanString", (str: String) => {
      if (str == null) null
      else {
        val str1=  str.replaceAll("<strong>", "").replace("</strong>", "")
          .replaceAll("<b>", "").replaceAll("</b>", "")
          .replaceAll("#", "").replaceAll(";", "")
          .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\\*", "")
          .replaceAll(" ", "").replaceAll(",", "")
          .replaceAll("，", "").replaceAll("；", "")
          .replaceAll("-", "").replaceAll("\\.", "")
          .replaceAll("\t", "").replaceAll("\\s", "")
          .toLowerCase

        val p = Pattern.compile("[^a-zA-Z0-9\u4E00-\u9FFF]")
        val matcher = p.matcher(str1)
        // 把其他字符替换成
        val str_new = matcher.replaceAll("")
        str_new
      }
    })

    val strings: Array[String] = str.reverse.split("")

    var sb=""
    val zh="[\u4e00-\u9fa5]"
    val en="[A-Za-z]+"

      breakable{

        for(i <- 0 until strings.size){

        if(strings(i).matches(zh))
          break

          sb+=strings(i)
        }

      }
//
//    for(i <- 0 until strings.size){
//
//      if(strings(i)==";" && flag==1){
//        strings(i)=",,,"
//      }
//      sb+=strings(i)
//    }

    println(sb.reverse)


//      def split(str:String):String = {
//        var flag=0
//        var str_new=""
//
//        for(a <- str){
//          if (a=="("){
//            flag +=1
//          }else if(a==")"){
//            flag -=1
//          }
//
//          if(a ==";" && flag==0){
//            a=" "
//          }
//        }
//        str
//      }

  }
}
