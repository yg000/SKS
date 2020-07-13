package cn.sks.common

import java.io.{File, FileWriter, IOException, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ParseAcademicCommittee {
  def main(args: Array[String]): Unit = {

    // 原始文件路径
    val filePath = "C:\\Users\\admin\\Desktop\\学部委员\\test\\"

    // 学术自传文件保存文件夹
    val writeDir= "C:\\Users\\admin\\Desktop\\学部委员\\test1"

    // 著作等保存文件
    val writePath= "C:\\Users\\admin\\Desktop\\学部委员\\test1\\test.csv"

    val files: Array[File] = new File(filePath).listFiles()
    try {
      val  pw = new PrintWriter(new FileWriter(writePath))
      files.foreach(x=>{
        println(x.getPath)
        val str: String = parseFile(x.getPath, writeDir)
        pw.println(str)
      })
      pw.close();
    }  catch {
      case e:IOException=>e.printStackTrace()
    }



  }

  def parseFile(filePath:String,writeDir:String):String={
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("test")
      .config("spark.deploy.mode","2g")
      .config("spark.drivermemory","8g")
      .config("spark.cores.max","4")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    val authorName = new File(filePath).getName.replace(".txt","")

    val rdd: RDD[String] = spark.sparkContext.textFile(filePath)
    val rdd1= rdd.map(x=>x.replaceAll("　"," ")).filter(x=>x.trim.size>0)

    val build = new StringBuilder
    val build_academic = new StringBuilder
    var num =0
    var key: String = ""
    rdd1.collect().foreach(x=>{
      if(num ==1) build_academic.append(x.trim+"\n")
      if(x.startsWith("学术自传")) {
        num = 1
        build_academic.append(authorName + "\t" + "学术自传" + "\t")
      }
      if(num ==0 ){
        if(!x.startsWith("　") && !x.startsWith(" ") ){
          key =authorName+"\t" +x
        } else build.append(key+"\t"+x.trim.replaceAll("，","\t")+"\n")
      }
    })

    val writePath_academic = writeDir+"\\"+authorName+"_academic.txt"
    writeFile(build_academic.toString(),writePath_academic)

    build.toString()
  }

  def writeFile(str:String,writePath:String)={
    try {
      val  pw = new PrintWriter(new FileWriter(writePath))
      pw.println(str)
      pw.close();
    }  catch {
      case e:IOException=>e.printStackTrace()
    }
  }




}
