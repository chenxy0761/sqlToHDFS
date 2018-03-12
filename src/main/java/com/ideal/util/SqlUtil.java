package com.ideal.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class SqlUtil {
    private static Logger logger = Logger.getLogger(SqlUtil.class);
    Properties p = new Properties();
    public SqlUtil(){
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream("config/config.properties"));
            p.load(in);
        } catch (IOException e) {
            logger.error(e+"加载配置文件时发生错误");
        }
        setHOSTNAME(p.getProperty("HOSTNAME"));
        setPORT(p.getProperty("PORT"));
        setUSERNAME(p.getProperty("USERNAME"));
        setPASSWORD(p.getProperty("PASSWORD"));
        setDatabasename(p.getProperty("databasename"));
        setTablename(p.getProperty("tablename"));
        setJdbcUsername(p.getProperty("jdbcUsername"));
        setJdbcPassword(p.getProperty("jdbcPassword"));
        setJdbcUrl(p.getProperty("jdbcUrl"));
        setJdbcDriver(p.getProperty("jdbcDriver"));
        setFtpPath(p.getProperty("ftpPath"));
        setHdfs(p.getProperty("hdfs"));
        setHdfsPath(p.getProperty("hdfsPath"));
    }

    String HOSTNAME ;
    String PORT ;
    String USERNAME;
    String PASSWORD;
    String databasename;
    String tablename;
    String jdbcUsername;
    String jdbcPassword;
    String jdbcUrl;
    String jdbcDriver;
    String ftpPath;

    public String getFtpPath() {
        return ftpPath;
    }

    public void setFtpPath(String ftpPath) {
        this.ftpPath = ftpPath;
    }

    public String getHdfs() {
        return hdfs;
    }

    public void setHdfs(String hdfs) {
        this.hdfs = hdfs;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    String hdfs;
    String hdfsPath;

    public void setHOSTNAME(String HOSTNAME) {
        this.HOSTNAME = HOSTNAME;
    }

    public void setPORT(String PORT) {
        this.PORT = PORT;
    }

    public void setUSERNAME(String USERNAME) {
        this.USERNAME = USERNAME;
    }

    public void setPASSWORD(String PASSWORD) {
        this.PASSWORD = PASSWORD;
    }

    public void setDatabasename(String databasename) {
        this.databasename = databasename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public void setJdbcUsername(String jdbcUsername) {
        this.jdbcUsername = jdbcUsername;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getHOSTNAME() {
        return HOSTNAME;
    }

    public String getPORT() {
        return PORT;
    }

    public String getUSERNAME() {
        return USERNAME;
    }

    public String getPASSWORD() {
        return PASSWORD;
    }

    public String getDatabasename() {
        return databasename;
    }

    public String getTablename() {
        return tablename;
    }

    public String getJdbcUsername() {
        return jdbcUsername;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }
}
