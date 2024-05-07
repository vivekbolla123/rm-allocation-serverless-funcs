package com.akasaair.queues.common.helpers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnection {

    private static final String url = System.getenv("RM_DB_URL");
    private static final String user = System.getenv("RM_DB_USERNAME");
    private static final String password = System.getenv("RM_DB_PASSWORD");

    // Static block to load the JDBC driver
    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public JdbcConnection() {
    }

    public Connection connection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

}
