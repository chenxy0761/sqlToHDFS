package com.ideal.main;

import com.ideal.ftpToHDFS.FtpConnection;
import com.ideal.util.SqlUtil;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.MalformedURLException;

public class DataUpdate {
    private static Logger logger = Logger.getLogger(DataUpdate.class);
    SqlUtil sqlUtil = new SqlUtil();
    //ftp服务器地址
    public String hostname = sqlUtil.getHOSTNAME();
    //ftp服务器端口号默认为2121
    public Integer port = Integer.parseInt(sqlUtil.getPORT());
    //ftp登录账号
    public String username = sqlUtil.getUSERNAME();
    //ftp登录密码
    public String password = sqlUtil.getPASSWORD();

    public FTPClient ftpClient = null;

    /**
     * 初始化ftp服务器
     */
    public void initFtpClient() {
        ftpClient = new FTPClient();
        ftpClient.setControlEncoding("utf-8");
        try {
            logger.info("connecting...ftp服务器:" + this.hostname + ":" + this.port);
            ftpClient.connect(hostname, port); //连接ftp服务器
            ftpClient.login(username, password); //登录ftp服务器
            int replyCode = ftpClient.getReplyCode(); //是否成功登录服务器
            if (!FTPReply.isPositiveCompletion(replyCode)) {
                logger.info("connect failed...ftp服务器:" + this.hostname + ":" + this.port);
            }
            logger.info("connect successfu...ftp服务器:" + this.hostname + ":" + this.port);
        } catch (MalformedURLException e) {
            logger.error(e);
        } catch (IOException e) {
            logger.error(e);
        }
    }
    /**
     * 上传文件
     *
     * @param pathname    ftp服务保存地址
     * @param fileName    上传到ftp的文件名
     * @param inputStream 输入文件流
     * @return
     */
    public boolean uploadFile(String pathname, String fileName, InputStream inputStream) {
        boolean flag = false;
        try {
            logger.info("开始将表上传到FTP");
            initFtpClient();
            ftpClient.setFileType(ftpClient.BINARY_FILE_TYPE);
            CreateDirecroty(pathname);
            ftpClient.makeDirectory(pathname);
            ftpClient.changeWorkingDirectory(pathname);
            ftpClient.storeFile(fileName, inputStream);
            inputStream.close();
            ftpClient.logout();
            flag = true;
            logger.info("上传文件到FTP成功");
        } catch (Exception e) {
            logger.error(e+"上传文件到FTP失败");
        } finally {
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.disconnect();
                } catch (IOException e) {
                    logger.error(e+"关闭FTP连接时发生错误");
                }
            }
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    logger.error(e+"上传到FTP的文件流关闭异常");
                }
            }
        }
        return true;
    }
    //改变目录路径
    public boolean changeWorkingDirectory(String directory) {
        boolean flag = true;
        try {
            flag = ftpClient.changeWorkingDirectory(directory);
            if (flag) {
                logger.info("进入文件夹" + directory + " 成功！");

            } else {
                logger.info("进入文件夹" + directory + " 失败！开始创建文件夹");
            }
        } catch (IOException ioe) {
            logger.error(ioe);
        }
        return flag;
    }

    //创建多层目录文件，如果有ftp服务器已存在该文件，则不创建，如果无，则创建
    public boolean CreateDirecroty(String remote) throws IOException {
        boolean success = true;
        String directory = remote + "/";
        // 如果远程目录不存在，则递归创建远程服务器目录
        if (!directory.equalsIgnoreCase("/") && !changeWorkingDirectory(new String(directory))) {
            int start = 0;
            int end = 0;
            if (directory.startsWith("/")) {
                start = 1;
            } else {
                start = 0;
            }
            end = directory.indexOf("/", start);
            String path = "";
            String paths = "";
            while (true) {
                String subDirectory = new String(remote.substring(start, end).getBytes("GBK"), "iso-8859-1");
                path = path + "/" + subDirectory;
                if (!existFile(path)) {
                    if (makeDirectory(subDirectory)) {
                        changeWorkingDirectory(subDirectory);
                    } else {
                        logger.info("创建目录[" + subDirectory + "]失败");
                        changeWorkingDirectory(subDirectory);
                    }
                } else {
                    changeWorkingDirectory(subDirectory);
                }
                paths = paths + "/" + subDirectory;
                start = end + 1;
                end = directory.indexOf("/", start);
                // 检查所有目录是否创建完毕
                if (end <= start) {
                    break;
                }
            }
        }
        return success;
    }
    //判断ftp服务器文件是否存在
    public boolean existFile(String path) throws IOException {
        boolean flag = false;
        FTPFile[] ftpFileArr = ftpClient.listFiles(path);
        if (ftpFileArr.length > 0) {
            flag = true;
        }
        return flag;
    }
    //创建目录
    public boolean makeDirectory(String dir) {
        boolean flag = true;
        try {
            flag = ftpClient.makeDirectory(dir);
            if (flag) {
                logger.info("创建文件夹" + dir + " 成功！");

            } else {
                logger.info("创建文件夹" + dir + " 失败！");
            }
        } catch (Exception e) {
            logger.error(e+"在加载FTP相关文件夹时出错");
        }
        return flag;
    }
    public static void main(String[] args) {
        SqlUtil s = new SqlUtil();
        DataUpdate ftp = new DataUpdate();
        if(SqlServerJDBC.Exp()!=null){
            ftp.uploadFile(s.getFtpPath(), s.getTablename()+".txt", SqlServerJDBC.Exp());
//                FtpConnection fc=new FtpConnection();
//                fc.FtpToHadp();
            logger.info("success!");
        }else {
            logger.info("fail!");
        }



    }
}
