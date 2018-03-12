package com.ideal.ftpToHDFS;

import com.ideal.util.SqlUtil;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.net.URI;


/**
 * Created by wss on 2018/3/5.
 */
public class FtpConnection {
    private static Logger logger = Logger.getLogger(FtpConnection.class);

    public boolean FtpToHadp() {

        SqlUtil s = new SqlUtil();
        FTPClient ftp = new FTPClient();
        Configuration conf = new Configuration();
        InputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        boolean result = true;
        try {
            int reply;
            ftp.connect(s.getHOSTNAME(),Integer.parseInt(s.getPORT()));
            ftp.login(s.getUSERNAME(), s.getPASSWORD());
            ftp.setControlEncoding("UTF-8");
            reply = ftp.getReplyCode();
            ftp.setDataTimeout(60000);
            ftp.setConnectTimeout(60000);
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return result;
            }
            FTPFile[] files = ftp.listFiles(s.getFtpPath());
            FileSystem hs = FileSystem.get(URI.create(s.getHdfs()), conf);
            for (FTPFile file : files) {
                if (!(file.getName().equals(".") || file.getName().equals(".."))) {
                    inputStream = ftp.retrieveFileStream(new String((s.getFtpPath() + s.getTablename()+".txt").getBytes("UTF-8"), "ISO-8859-1"));
                    outputStream = hs.create(new Path(s.getHdfs()+s.getHdfsPath() + file.getName()));
                    IOUtils.copyBytes(inputStream, outputStream, conf, false);
                    if (inputStream != null) {
                        inputStream.close();
                        ftp.completePendingCommand();
                        logger.info("文件上传HDFS成功");
                    }
                }
            }
        } catch (Exception e) {
            result = false;
            logger.error(e+"文件上传到HDFS时发生错误");
        }
        return result;
    }


}





