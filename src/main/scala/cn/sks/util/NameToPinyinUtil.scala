package cn.sks.util

import net.sourceforge.pinyin4j.PinyinHelper
import net.sourceforge.pinyin4j.format.{HanyuPinyinCaseType, HanyuPinyinOutputFormat, HanyuPinyinToneType, HanyuPinyinVCharType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object NameToPinyinUtil {
  def nameToPinyin(spark:SparkSession, origin_DF:DataFrame, origin_zh_name:String):DataFrame={
    // 正常翻译
    val normal_pinyin = "en_name_normal"
    // 倒装翻译
    val inverted_pinyin ="en_name_inverted"
    // 汉语翻译为拼音
    spark.sqlContext.udf.register("udf_ToPinyin",(str:String,familyType:String,book_family_pinyin:String,orderType:String)=>{
      if (str == null) null
      else nameToPinyinRule(DefineUDF.ToDBC(str),familyType,book_family_pinyin,orderType)
    })

    // 清洗人名中的数字
    spark.sqlContext.udf.register("clean_num",(str:String)=>{
      if (str==null) null
      else str.replaceAll("[0-9]{1,}","")
        .replaceAll("•","·")
        .replaceAll("▪","·")
        .replaceAll("\\([^)]*\\)","")
        .replaceAll("\\（[^)]*\\）","").trim
    })
    origin_DF.createOrReplaceTempView("temp")
    val fields = new StringBuilder
    origin_DF.schema.fieldNames.foreach(x=>{
      if(x.equals(origin_zh_name)) fields.append(s"clean_num(${origin_zh_name}) as ${origin_zh_name},")
      else fields.append(x+",")
    })
    val sqlString = fields.toString.stripSuffix(",")
    spark.sql(s"select ${sqlString} from temp").createOrReplaceTempView("origin")

    val const_book_family_name = spark.sql("select * from ods.o_const_book_family_name")
    const_book_family_name.createOrReplaceTempView("const_book_family_name")
    // 复姓   欧阳娜娜
    val compound_surname =  spark.sql(
      s"""
        |select *,udf_ToPinyin(${origin_zh_name},"compound_surname",pinyin,"normal") as ${normal_pinyin},
        |  udf_ToPinyin(${origin_zh_name},"compound_surname",pinyin,"inverted") as ${inverted_pinyin}  from (
        |  select a.*,b.pinyin from origin a
        |  left join const_book_family_name b
        |  on substr(a.${origin_zh_name},0,2)=b.family_name and length(a.${origin_zh_name}) in (3,4)
        |  where pinyin is not null
        |)a
        |""".stripMargin).drop("pinyin")
    compound_surname.createOrReplaceTempView("compound_surname")

    spark.sql(s"select a.* from origin a where not exists (select * from compound_surname b where a.${origin_zh_name}=b.${origin_zh_name})")
      .createOrReplaceTempView("except_compound")

    // 单姓    张三
    val single_surname =spark.sql(
      s"""
        |select *,udf_ToPinyin(${origin_zh_name},"single_surname",pinyin,"normal") as ${normal_pinyin},
        |  udf_ToPinyin(${origin_zh_name},"single_surname",pinyin,"inverted") as ${inverted_pinyin}  from (
        |      select a.*,b.pinyin from except_compound a
        |      left join const_book_family_name b
        |      on substr(a.${origin_zh_name},0,1)=b.family_name and length(a.${origin_zh_name}) in (2,3)
        |      where pinyin is not null
        |)a
        |""".stripMargin).drop("pinyin")
    single_surname.createOrReplaceTempView("single_surname")
    spark.sql(s"select a.* from except_compound a where not exists (select * from single_surname b where a.${origin_zh_name}=b.${origin_zh_name})")
      .createOrReplaceTempView("except_compound_single")

    // 名字中带点的 麦迪娜·阿布里克木
    val dot_surname = spark.sql(
      s"""
        |select *,udf_ToPinyin(${origin_zh_name},"dot_surname",'pinyin',"normal") as ${normal_pinyin},
        | udf_ToPinyin(${origin_zh_name},"dot_surname",'pinyin',"inverted") as ${inverted_pinyin}  from (
        |      select * from except_compound_single where ${origin_zh_name} like '%·%'
        |)a
        |""".stripMargin)
    dot_surname.createOrReplaceTempView("dot_surname")

    // other   哈尔勒哈西
    val other_surname = spark.sql(
      s"""
        |select a.*,udf_ToPinyin(${origin_zh_name},"other",'pinyin',"normal") as ${normal_pinyin},
        | udf_ToPinyin(${origin_zh_name},"other",'pinyin',"inverted") as ${inverted_pinyin}  from except_compound_single a
        |where not exists (select * from dot_surname b where a.${origin_zh_name}=b.${origin_zh_name})
        |""".stripMargin)

    other_surname.createOrReplaceTempView("other_surname")

    val outDF = spark.sql(
      s"""
        |select * from compound_surname  union all
        |select * from single_surname union all
        |select * from dot_surname union all
        |select * from other_surname
        |""".stripMargin)

    outDF.show(3)
    outDF
  }

  // 用于作者翻译 （具体规则）
  def nameToPinyinRule(str:String,familyType:String,book_family_pinyin:String,orderType:String): String ={
    val build = new StringBuilder
    // 直接翻译
    if(familyType.equals("other")) {
      if(DefineUDF.isAllEnglish(str.replaceAll(" ","")) && str.trim.contains(" ")){
        val strings: Array[String] = str.replaceFirst(" ", "@").split("@")
        if (orderType.equals("normal"))  build.append(str)
        else build.append(strings(1)).append(" ").append(strings(0))
      } else {
        str.split("").foreach(x=>{
          if(DefineUDF.isAllChiness(x)) build.append(ToPinyin(x))
          else build.append(x)
        })
      }
    }
    else if(familyType.equals("compound_surname")) { //复姓翻译
      if (orderType.equals("normal"))  build.append(book_family_pinyin).append(" ")
      str.drop(2).split("").foreach(x=>{
        if(DefineUDF.isAllChiness(x)) build.append(ToPinyin(x))
        else build.append(x)
      })
      if (orderType.equals("inverted"))  build.append(" ").append(book_family_pinyin)
    } else if(familyType.equals("single_surname")) { // 单姓翻译
      if (orderType.equals("normal"))  build.append(book_family_pinyin).append(" ")
      str.drop(1).split("").foreach(x=>{
        if(DefineUDF.isAllChiness(x)) build.append(ToPinyin(x))
        else build.append(x)
      })
      if (orderType.equals("inverted"))  build.append(" ").append(book_family_pinyin)
    } else if(familyType.equals("dot_surname")){ //带 · 的 翻译
      if (orderType.equals("inverted") && str.split("·").length==2) {
        str.split("·")(1).split("").foreach(x=>{
          if(DefineUDF.isAllChiness(x)) build.append(ToPinyin(x))
          else build.append(x)
        })
        build.append("·")
        str.split("·")(0).split("").foreach(x=>{
          if(DefineUDF.isAllChiness(x)) build.append(ToPinyin(x))
          else build.append(x)
        })
      } else {
        str.split("").foreach(x=>{
          if(DefineUDF.isAllChiness(x)) build.append(ToPinyin(x))
          else build.append(x)
        })
      }
    }

    build.toString()
  }


  // 汉语转拼音 （用于作者翻译）
  def ToPinyin(chinese:String): String ={
    if(chinese.equals("瀮") || chinese.equals("飊")){
      if (chinese.equals("瀮")) "lian"
      if(chinese.equals("飊")) "biao"
      else chinese
    } else {
      val defaultFormat: HanyuPinyinOutputFormat = new HanyuPinyinOutputFormat
      defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE)
      defaultFormat.setToneType(HanyuPinyinToneType.WITH_TONE_NUMBER)
      defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
      defaultFormat.setVCharType(HanyuPinyinVCharType.WITH_U_UNICODE)

      var output = new StringBuilder
      val input: Array[Char] = chinese.trim().toCharArray()
      try {
        for (i <- 0 until input.length) {
          if (Character.toString(input(i)).matches("[\\u4E00-\\u9FA5]+")) {
            val temp = PinyinHelper.toHanyuPinyinStringArray(input(i), defaultFormat)
            output.append(temp(0))
            output.append("")
          } else {
            output.append(Character.toString(input(i)))
          }
        }
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
//          println(chinese + "---无法被翻译为拼音")
//          e.printStackTrace()
        }
      }
      output.toString()
    }
  }
}
