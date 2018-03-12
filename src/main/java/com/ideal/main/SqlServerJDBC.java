package com.ideal.main;

import com.ideal.util.SqlUtil;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.*;

public class SqlServerJDBC {

    private static Logger logger = Logger.getLogger(SqlServerJDBC.class);
    /**
     * 导出数据
     */
    public static InputStream Exp() {
        SqlUtil sqlUtil = new SqlUtil();
        InputStream in_withcode = null;
        Connection Conn = null;
        try {
            Class.forName(sqlUtil.getJdbcDriver()).newInstance();
            String jdbcUrl = sqlUtil.getJdbcUrl();
            String jdbcUsername = sqlUtil.getJdbcUsername();
            String jdbcPassword = sqlUtil.getJdbcPassword();
            Conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            boolean flag = true;
            int conn = 0;
            while (flag) {
                String Sql = "SELECT * FROM " + sqlUtil.getTablename();
                try {
                    PreparedStatement pst = Conn.prepareStatement(Sql);
                    ResultSet rs = pst.executeQuery(Sql);
                    // 获取列数
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    String row = "";
                    String name = "";
                    boolean nameflag = true;
                    while (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnLabel(i);
                            String value = rs.getString(columnName);
                            row = row + value + "\t";
                            if (nameflag) {
                                name = name + metaData.getColumnName(i) + "\t";
                            }
                        }
                        nameflag = false;
                        row = row.trim() + "\r\n";
                    }
                    String str = name.trim() + "\r\n" + row;
                    in_withcode = new ByteArrayInputStream(str.getBytes("UTF-8"));
                    flag = false;
                    rs.close();
                    pst.close();
                } catch (SQLException e) {
                    if (conn < 10) {
                        conn++;
                        logger.info("连接数据库失败，尝试第" + conn + "次重新连接......");
                    } else {
                        flag = false;
                        logger.info("连接数据库失败，发生错误--->" + e);
                    }
                } catch (IOException e) {
                    logger.error(e);
                    flag = false;
                }
            }
        } catch (SQLException e) {
            logger.error(e);
            return null;
        } catch (InstantiationException e) {
            logger.error(e);
            return null;
        } catch (IllegalAccessException e) {
            logger.error(e);
            return null;
        } catch (ClassNotFoundException e) {
            logger.error(e);
            return null;
        } finally {
            try {
                Conn.close();
            } catch (SQLException e) {
                logger.error(e);
                return null;
            }
            return in_withcode;
        }
    }
}