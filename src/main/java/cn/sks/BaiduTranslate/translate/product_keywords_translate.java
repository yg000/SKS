package cn.sks.BaiduTranslate.translate;

import java.io.*;
import java.sql.*;

public class product_keywords_translate {
    public static void main(String[] args) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String jdbcUrl = "jdbc:hive2://10.0.82.132:10000/nsfc";
        String hiveUser = "root";
        String hivePassword = "123456";
        String sql = "select * from nsfc.wd_product_keywords_nsfc_zh_null limit 10 ";

        try {
            Connection con = DriverManager.getConnection(jdbcUrl, hiveUser, hivePassword);
            PreparedStatement ptsm = con.prepareStatement(sql);
            ResultSet rs = ptsm.executeQuery();
            BufferedWriter bw = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream("src/main/resources/product_keywords_tran.csv", true)));
            PrintWriter pw= new PrintWriter(new File("src/main/resources/product_keywords_tran.csv"));

            String key="";
            while (rs.next()) {

                Thread.sleep(1000);
                key = rs.getString(1) + "；" + rs.getString(3);
                pw.write(key);

            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }


    }
}
