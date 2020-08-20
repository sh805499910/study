package com.oneplus.shiyuan.study.hdfs_crud;

import com.jcraft.jsch.IO;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * HDFS接口
 * @author shiyuan.shen
 * @date 2020-8-20
 */
public interface IHDFS {

    /**
     * 创建目录
     * @param path HDFS路径
     * @return boolean 是否成功
     */
    boolean mkdir(String path) throws IOException;

    /**
     * 创建新文件
     * @param filePath HDFS文件路径
     * @param contents 文件内容
     * @return boolean 是否成功
     */
    boolean createFile(String filePath, byte[] contents) throws IOException;

    /**
     * 读取文件内容
     * @param filePath HDFS文件路径
     * @return String 返回文件内容
     */
    String readFile(String filePath) throws IOException;

    /**
     * 获取指定目录下的文件
     * @param direPath HDFS目录
     * @return List<Map<String,Object>> 文件
     */
    List<Map<String,Object>> getDirectoryFromHdfs(String direPath) throws IOException;

    /**
     * 下载文件到本地
     * @param filePath HDFS文件路径
     * @param srcPath 本地文件路径
     */
    void downloadFromHdfs(String filePath, String srcPath) throws IOException;

    /**
     * 上传本地文件
     * @param srcPath 本地文件路径
     * @param filePath HDFS目标路径
     * @return List<Map<String,Object>> 上传目录下文件
     */
    List<Map<String, Object>> uploadFile(String srcPath, String filePath) throws IOException;

    /**
     * 文件重命名
     * @param oldName 旧文件名
     * @param newName 新文件名
     * @return boolean 返回是否成功
     */
    boolean rename(String oldName, String newName) throws IOException;

    /**
     * 删除文件
     * @param filePath HDFS文件目录
     * @return boolean 返回是否成功
     */
    boolean delete(String filePath) throws IOException;
}
