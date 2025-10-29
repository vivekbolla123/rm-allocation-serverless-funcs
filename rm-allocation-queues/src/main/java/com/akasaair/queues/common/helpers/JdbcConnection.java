//package com.akasaair.queues.common.helpers;
//
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.stereotype.Component;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//
//@Component
//@Configuration
//public class JdbcConnection {
//
//    @Value("${rm.db.url}")
//    private String url;
//
//    @Value("${rm.db.username}")
//    private String user;
//
//    @Value("${RM_DB_APPUSER_PASSWORD}")
//    private String password;
//
//    // Static block to load the JDBC driver
//    static {
//        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public Connection connection() throws SQLException {
//        return DriverManager.getConnection(url, user, password);
//    }
//
//}
