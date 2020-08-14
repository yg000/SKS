package cn.sks.jutil;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.Server;

/**
 * 作者 : iechenyb<br>
 * 类描述: 说点啥<br>
 * 创建时间: 2018年5月31日
 */
public class H2dbUtil {
    private static Server server;
    private static String port = "9080";
    //linux   ~/tomcat/webapps/data/cashDb
    private static String dbDir = "/root/test;CACHE_SIZE=32384;MAX_LOG_SIZE=32384;mv_store=false";// ./h2db/
    private static String user = "sa";
    private static String password = "";

    public static void startServer() {
        try {
            System.out.println("正在启动h2...");
            server = Server.createTcpServer(new String[]{"-tcp", "-tcpAllowOthers", "-tcpPort", port}).start();
        } catch (SQLException e) {
            System.out.println("启动h2出错：" + e.toString());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void stopServer() {
        if (server != null) {
            System.out.println("正在关闭h2...");
            server.stop();
            System.out.println("关闭成功.");
        }
    }

    public static void useH2(String source,String target) {
        try {
            Class.forName("org.h2.Driver");
            Connection conn = DriverManager.getConnection("jdbc:h2:tcp://10.0.80.191:" + port + "/" + dbDir, user,
                    password);
            Statement stat = conn.createStatement();
            // insert data
            //stat.execute("DROP TABLE IF EXISTS data_trace");
            stat.execute("CREATE TABLE if not exists data_trace(source VARCHAR,target varchar, UNIQUE KEY `uk_source_target` (`source`,`target`))");
            stat.execute("INSERT INTO data_trace VALUES('" + source + "','" + target + "')");
            // use data
            ResultSet result = stat.executeQuery("select source,target from data_trace ");
            int i = 1;
            while (result.next()) {
                System.out.println(i++ + ":" + result.getString("source"));
            }
            result.close();
            stat.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("*********start***********");
        H2dbUtil h2 = new H2dbUtil();
        h2.startServer();//调用远端服务时，不需要开启server
        //h2.useH2("a","b");
        //h2.stopServer();
        System.out.println("*********end***********");
    }
}