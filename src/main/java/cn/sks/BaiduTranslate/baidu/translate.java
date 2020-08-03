package cn.sks.BaiduTranslate.baidu;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class translate {
    public static void main(String[] args) {

        String APP_ID="20200608000489509";
        String SECURITY_KEY="W0Aid0W8krDd0z79vn1_";

        TransApi api = new TransApi(APP_ID, SECURITY_KEY);

        String query = "hello world";
        String str = api.getTransResult(query, "auto", "zh");    //中文翻译英文

        System.out.println(str);    //输出结果，即json字段
        JsonObject  jsonObj = (JsonObject)new JsonParser().parse(str);   //解析json字段
        String res = jsonObj.get("trans_result").toString();   //获取json字段中的 result字段，因为result字段本身即是一个json数组字段，所以要进一步解析
        JsonArray js = new JsonParser().parse(res).getAsJsonArray();   //解析json数组字段
        jsonObj = (JsonObject)js.get(0);    //result数组中只有一个元素，所以直接取第一个元素
        System.out.println(jsonObj.get("dst").getAsString());   //得到dst字段，即译文，并输出

    }
}
