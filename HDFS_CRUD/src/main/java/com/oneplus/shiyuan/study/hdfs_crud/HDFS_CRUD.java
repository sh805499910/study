package com.oneplus.shiyuan.study.hdfs_crud;

import java.io.IOException;
import java.util.*;

/**
 * HDFS增删改查
 * @author shiyuan.shen
 * @date 2020-8-20
 */
public class HDFS_CRUD {
    public static void main(String[] args) throws IOException {
        IHDFSImpl ihdfs = new IHDFSImpl();
        //创建目录
        ihdfs.mkdir("/aaa");
        //创建新文件
        ihdfs.createFile("/aaa/tyest.txt","This is a test file!".getBytes());
        //读取文件内容
        ihdfs.readFile("/aaa/test.txt");
        //遍历指定目录
        ihdfs.getDirectoryFromHdfs("/");
        //下载文件至本地
        ihdfs.downloadFromHdfs("/aaa/test.txt","D://test.txt");
        //上传本地文件
        ihdfs.uploadFile("D://test.txt","/aaa/");
        //文件重命名
        ihdfs.rename("/aaa/test.txt","/aaa/test2.txt");
        //删除文件
        ihdfs.delete("/aaa/test.txt");

    }
}
