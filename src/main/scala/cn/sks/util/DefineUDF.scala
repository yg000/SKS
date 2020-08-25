package cn.sks.util

import java.util.regex.{Matcher, Pattern}

import cn.sks.BaiduTranslate.baidu.TransApi
import cn.sks.ods.ms.Test02.data2MySQL
import com.google.gson.{JsonObject, JsonParser}
import org.json.JSONObject

import scala.collection.mutable.ArrayBuffer
object DefineUDF {
  def transformEnglish(str:String): String = {

    var APP_ID: String ="20200608000489509"
    var SECURITY_KEY: String ="W0Aid0W8krDd0z79vn1_"
    if(Math.floor(Math.random()*2) ==0){
      APP_ID ="20200608000489509"
      SECURITY_KEY ="W0Aid0W8krDd0z79vn1_"
    }else{
      APP_ID ="20200617000497648"
      SECURITY_KEY ="AWuX5fymN1X61Bfxkxi4"
    }

    val api = new TransApi(APP_ID, SECURITY_KEY)
    try {
      val query = str
      var i = 0
      var jsonString = "{\"error_code\":\"54003\",\"error_msg\":\"Invalid Access Limit\"}"
      while (jsonString =="{\"error_code\":\"54003\",\"error_msg\":\"Invalid Access Limit\"}") {
        jsonString = api.getTransResult(query, "zh", "en")
        i += 1
        if(i>6){
          jsonString = ""
        }
      }
      val json = new JSONObject(jsonString.toString)
      val json0 = new JSONObject(json.get("trans_result").toString.replace("[", "").replace("]", ""))
      System.out.println(jsonString)
      json0.get("dst").toString
    } catch {
      case e:Exception=>null
    }
  }

  def transformChinese(str:String): String = {

    var APP_ID: String ="20200608000489509"
    var SECURITY_KEY: String ="W0Aid0W8krDd0z79vn1_"
    if(Math.floor(Math.random()*2) ==0){
      APP_ID ="20200608000489509"
      SECURITY_KEY ="W0Aid0W8krDd0z79vn1_"
    }else{
      APP_ID ="20200617000497648"
      SECURITY_KEY ="AWuX5fymN1X61Bfxkxi4"
    }


    val api = new TransApi(APP_ID, SECURITY_KEY)
    try {
      val query = str
      var i = 0
      var jsonString = "{\"error_code\":\"54003\",\"error_msg\":\"Invalid Access Limit\"}"
      while (jsonString =="{\"error_code\":\"54003\",\"error_msg\":\"Invalid Access Limit\"}") {
        jsonString = api.getTransResult(query, "en", "zh")
        i += 1
        if(i>6){
          jsonString = ""
        }
      }
      val json = new JSONObject(jsonString.toString)
      val json0 = new JSONObject(json.get("trans_result").toString.replace("[", "").replace("]", ""))
      System.out.println(jsonString)
      json0.get("dst").toString
    } catch {
      case e:Exception=>null
    }
  }

  def unionFlowSource(originFlowSource:String,targetFlowSource:String,rule:String):String={
    val str1= "{\"from\":["
    val str2= "],\"to\":"
    val str3= ",\"rule\":\""
    val str4= "\"}"
    //val rule = "name+title"

    str1 + originFlowSource + str2 + targetFlowSource + str3 + rule + str4
  }
  // 判断是否包含中文
  def isContainChinese(str:String): Boolean ={
    try{
    val pattern: Pattern = Pattern.compile("[\u4e00-\u9fa5]")
    val m: Matcher = pattern.matcher(str)
    if (m.find()) {
     return true
    }
     false
    }catch {
      case ex:NullPointerException=>false
    }
  }
  // 判断是否包含英文
  def isContainEnglish(str:String): Boolean ={
    val pattern: Pattern = Pattern.compile("[a-zA-z]+")
    val m: Matcher = pattern.matcher(str)
    if (m.find()) {
      return true
    }
     false
  }
  def isAllEnglish(str:String): Boolean ={
    str.matches("[a-zA-z]+")
  }
  def isAllChiness(str:String): Boolean ={
    str.matches("[\u4e00-\u9fa5]")
  }

  def translate(str:String,tolanguage:String): String = {

    val APP_ID = "20200608000489509"
    val SECURITY_KEY = "W0Aid0W8krDd0z79vn1_"

    val api = new TransApi(APP_ID, SECURITY_KEY)

    //        String query = "hello world";

    var str1 = api.getTransResult(str, "zh", tolanguage) //中文翻译英文

    //        System.out.println(str);    //输出结果，即json字段
    var jsonObj = new JsonParser().parse(str1).asInstanceOf[JsonObject]
    //解析json字段
    try {
      val res = jsonObj.get("trans_result").toString
      //获取json字段中的 result字段，因为result字段本身即是一个json数组字段，所以要进一步解析
      val js = new JsonParser().parse(res).getAsJsonArray //解析json数组字段
      jsonObj = js.get(0).asInstanceOf[JsonObject] //result数组中只有一个元素，所以直接取第一个元素

      val relust = jsonObj.get("dst").getAsString //得到dst字段，即译文，并输出

      relust
    }catch {
      case ex:NullPointerException=>"null"
    }
  }
  // 清洗 成果中 title 以及authors 中的 标签符号
  def clean_div(data:String): String ={
      if(data== null && data!="") null
      else {
          var htmlStr = data.replaceAll("&lt;","<")
        .replaceAll("&gt;",">")
        .replaceAll("&quot;","\"")
        .replaceAll("&amp;","&")
        .replaceAll("&nbsp;","")

          val script = "<script[^>]*?>[\\s\\S]*?<\\/script>" //定义script的正则表达式
          val style = "<style[^>]*?>[\\s\\S]*?<\\/style>" //定义style的正则表达式
          val html = "<[^>]+>" //定义HTML标签的正则表达式
          val p_script = Pattern.compile(script, Pattern.CASE_INSENSITIVE)
          val m_script = p_script.matcher(htmlStr)
          htmlStr = m_script.replaceAll("") //过滤script标签

          val p_style = Pattern.compile(style, Pattern.CASE_INSENSITIVE)
          val m_style = p_style.matcher(htmlStr)
          htmlStr = m_style.replaceAll("") //过滤style标签

          val p_html = Pattern.compile(html, Pattern.CASE_INSENSITIVE)
          val m_html = p_html.matcher(htmlStr)
          htmlStr = m_html.replaceAll("") //过滤html标签
            .replaceAll("#", "")
            .replaceAll("\\\\", "")
            .replaceAll("\\*", "") .replaceAll("\t", " ")
            .trim
          htmlStr
    }
  }

  //  清洗掉所有的特殊字符（融合时使用） 论文名字 单位
  def clean_fusion(data:String):String = {
    if (data == null) null
    else data.replaceAll("[^a-zA-Z0-9\u4E00-\u9FFF]","")
      .trim.toLowerCase()

  }

  // 统一成果中 authors 字段的分割符号
  def clean_separator(data:String):String = {
    if(data == null) null
      else {
      val dataStr = data.stripSuffix("|").stripSuffix(";").stripSuffix("；").stripSuffix("，").stripSuffix("、").stripSuffix("。").replaceAll("　"," ").replaceAll(" "," ").trim

      if (dataStr.contains("|")) dataStr.replace(";", "").replace("|", ";")
        .replace("。", "").replace(",", " ")
        .replaceAll(" ;",";").replaceAll("; ",";").replace(";;", " ").replaceAll("  "," ")
        .stripSuffix(";")

      else if (dataStr.contains(";"))  dataStr.replace(",", " ").replace("。", "")
        .replaceAll(" ;",";").replaceAll("; ",";").replace(";;", " ").replaceAll("  "," ")
        .stripSuffix(";")

      else if (dataStr.contains("；")) dataStr.replace("；", ";").replace("。", "").replace(",", " ")
        .replaceAll(" ;",";").replaceAll("; ",";").replace(";;", " ").replaceAll("  "," ")
        .stripSuffix(";")

      else if (dataStr.contains("，"))
        dataStr.replace("，", ";").replace("。", "").replace(",", " ")
        .replaceAll(" ;",";").replaceAll("; ",";").replace(";;", " ").replaceAll("  "," ")
        .stripSuffix(";")

      else if (dataStr.contains("、")) dataStr.replace("、", ";").replace("。", "") .replace(",", " ")
        .replaceAll(" ;",";").replaceAll("; ",";").replace(";;", " ").replaceAll("  "," ")
       .stripSuffix(";")
      else if (dataStr.contains(",")) {
        if (dataStr.split(",").length==2 ) {
          if(dataStr.split(",")(0).trim.contains(" ") ||dataStr.split(",")(1).trim.contains(" ")) dataStr.replaceAll(",",";")
          else dataStr.replace(",", " ")
        }else{
          dataStr.replaceAll(",",";").replaceAll("  "," ")
            .replaceAll(" ;",";").replaceAll("; ",";").replace(";;", " ")
            .stripSuffix(";")
        }
      } else dataStr
    }
  }

  // 全角转半角
  def ToDBC(str:String):String={
    var charArray = str.toCharArray
    var ling = ""
    for (i<-0 until charArray.length){
      var t = charArray(i)
      if (t == '\u3000') {
        t == " "
      } else if (t > '\uFF00' && t< '\uFF5F') {
        t = (t-65248).toChar
      }
      ling +=t
    }
    ling
  }

}


