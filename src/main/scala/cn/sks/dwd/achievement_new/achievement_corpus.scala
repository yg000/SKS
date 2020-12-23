package cn.sks.dwd.achievement_new



import cn.sks.util.DefineUDF
import org.apache.spark.sql.SparkSession

object achievement_corpus {
  val spark = SparkSession.builder()
    //.master("local[40]")
    .appName("achievement_corpus  ")
    .config("spark.deploy.mode","client")
    .config("spark.cores.max", "8")
    .config("spark.local.dir", "/data/tmp")
    .config("hive.metastore.uris","thrift://10.0.82.132:9083")
    //.config("spark.sql.shuffle.partitions","10")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")

  spark.sqlContext.udf.register("clean_fusion",(str:String) =>{
    DefineUDF.clean_fusion(str)
  })
  spark.sqlContext.udf.register("isContainChinese",(str:String) =>{
    DefineUDF.isContainChinese(str)
  })

  def main(args: Array[String]): Unit = {

    //val path = s"hdfs://192.168.3.138:8020/user/root/corpus/s2-corpus-$stri.gz"
    val path = s"hdfs://10.0.82.131:8020/data/corpus/*gz"
    //spark.read.option("header","true").json(path).printSchema()

    spark.read.json(path)//.printSchema()
    //.write.format("hive").mode("overwrite").insertInto("ods.o_corpus")


    spark.sql(
      """
        |select
        | id,
        | title,
        | authors_mid.name as person_name,
        | authors[0].name as first_author,
        | concat_ws('; ',authors_mid.ids) as person_id
        | from ods.o_corpus
        | lateral view explode(authors) as authors_mid
        |""".stripMargin).repartition(500)
    .write.format("hive").mode("overwrite").saveAsTable("ods.o_corpus_authors")

    //size(split(max(incitations),'; '))
    spark.sql(
      """
        |select
        |id
        |,null
        |,max(title)
        |,if(!isContainChinese(max(title)),max(title),null) en_title
        |,concat_ws('#',collect_list(person_name))
        |,max(first_author)
        |,null
        |,max(year)
        |,null
        |,null
        |,null
        |,null
        |,max(paperabstract)
        |,max(doi)
        |,max(doiurl)
        |,max(journal_name)
        |,max(journal_volume)
        |,null
        |,trim(split(max(journal_pages),'-')[0])
        |,trim(split(max(journal_pages),'-')[1])
        |,if(isContainChinese(max(title)),'cn','en')
        |,null
        |,null
        |,null
        |,null
        |,null
        |,'corpus'
        |,null
        |from ods.o_corpus_authors group by id
      """.stripMargin)
      //.write.format("hive").mode("overwrite").insertInto("ods.o_product_corpus")





    //    authors             	array<struct<ids:array<string>,name:string>>
    //      doi                 	string
    //      doiurl              	string
    //      entities            	array<string>
    //    fieldsofstudy       	array<string>
    //    id                  	string
    //      incitations         	array<string>
    //    journalname         	string
    //      journalpages        	string
    //      journalvolume       	string
    //      magid               	string
    //      outcitations        	array<string>
    //    paperabstract       	string
    //      pdfurls             	array<string>
    //    pmid                	string
    //      s2pdfurl            	string
    //      s2url               	string
    //      sources             	array<string>
    //    title               	string
    //      venue               	string
    //      year                	bigint

//    |-- authors: array (nullable = true)
//    |    |-- element: struct (containsNull = true)
//    |    |    |-- ids: array (nullable = true)
//    |    |    |    |-- element: string (containsNull = true)
//    |    |    |-- name: string (nullable = true)
//    |-- doi: string (nullable = true)
//    |-- doiUrl: string (nullable = true)
//    |-- entities: array (nullable = true)
//    |    |-- element: string (containsNull = true)
//    |-- fieldsOfStudy: array (nullable = true)
//    |    |-- element: string (containsNull = true)
//    |-- id: string (nullable = true)
//    |-- inCitations: array (nullable = true)
//    |    |-- element: string (containsNull = true)
//    |-- journalName: string (nullable = true)
//    |-- journalPages: string (nullable = true)
//    |-- journalVolume: string (nullable = true)
//    |-- magId: string (nullable = true)
//    |-- outCitations: array (nullable = true)
//    |    |-- element: string (containsNull = true)
//    |-- paperAbstract: string (nullable = true)
//    |-- pdfUrls: array (nullable = true)
//    |    |-- element: string (containsNull = true)
//    |-- pmid: string (nullable = true)
//    |-- s2PdfUrl: string (nullable = true)
//    |-- s2Url: string (nullable = true)
//    |-- sources: array (nullable = true)
//    |    |-- element: string (containsNull = true)
//    |-- title: string (nullable = true)
//    |-- venue: string (nullable = true)
//    |-- year: long (nullable = true)

  }
}

