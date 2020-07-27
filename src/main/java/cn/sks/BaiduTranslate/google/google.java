package cn.sks.BaiduTranslate.google;

public class google {

    public static String translate(String str,String tolanguage) {
        GoogleApi googleApi=new GoogleApi();
//        GoogleApi googleApi=new GoogleApi("122.224.227.202",3128);

        try {
            String result=googleApi.translate(str,tolanguage);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
