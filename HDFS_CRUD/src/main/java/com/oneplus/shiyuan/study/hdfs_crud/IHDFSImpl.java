package com.oneplus.shiyuan.study.hdfs_crud;

import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.oneplus.shiyuan.study.hdfs_crud.IHDFS;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class IHDFSImpl implements IHDFS{

    final Configuration conf = HDFSConfig.conf;

    public boolean mkdir(String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        boolean isok = fs.mkdirs(srcPath);
        if(isok){
            System.out.println("create" + path + "dir ok");
        }else {
            System.out.println("create" + path+ "dir failure");
        }
        fs.close();
        return isok;
    }

    public boolean createFile(String filePath, byte[] contents) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(filePath);
        URI uri = dstPath.toUri();
        String path = uri.getPath();
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("create file successly");
        return path != null;
    }

    public String readFile(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream inputStream = null;
        try {
            inputStream = fs.open(srcPath);
//            InputStreamReader ipr = new InputStreamReader(inputStream,"UTF-8");
//            BufferedReader bf = new BufferedReader(ipr);
//            String results = "";
//            String newline = "";
//            while ((newline=bf.readLine()) != null){
//                results += newline + '\n';
//            }
            return readStream(inputStream);
        }finally {
            IOUtils.closeStream(inputStream);
        }
    }

    public List<Map<String, Object>> getDirectoryFromHdfs(String direPath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(direPath),conf);
        FileStatus[] filelist = fs.listStatus(new Path(direPath));
        List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
        for (FileStatus fileStatus : filelist){
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("name",fileStatus.getPath().getName());
            map.put("size",fileStatus.getLen());
            map.put("Type",(fileStatus.isDirectory() ? "dir" : "file"));
            map.put("Path",fileStatus.getPath());
            mapList.add(map);
            System.out.println(direPath);
            System.out.println("Name:" + fileStatus.getPath().getName());
            System.out.println("Size:" + fileStatus.getLen());
            System.out.println("Type:" + (fileStatus.isDirectory() ? "dir" : "file"));
            System.out.println("Path:" + fileStatus.getPath());
        }
        fs.close();
        return mapList;
    }

    public void downloadFromHdfs(String filePath, String srcPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        System.out.println("download path:" + path);
        InputStream in = fs.open(path);
        OutputStream out = new FileOutputStream(srcPath);
        IOUtils.copyBytes(in,out,4096,true);
    }

    public List<Map<String, Object>> uploadFile(String srcPath, String filePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(srcPath);
        Path dst = new Path(filePath);
        if (!fs.exists(dst)){
            fs.mkdirs(dst);
        }
        //调用文件系统的文件复制函数
        fs.copyFromLocalFile(src,dst);
        //打印文件路径
        System.out.println("Upload to" + conf.get("fs.default.name"));
        System.out.println("list file" + '\n');
        FileStatus[] fileStatuses = fs.listStatus(dst);
        List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
        for (FileStatus fileStatus : fileStatuses){
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("Name",fileStatus.getPath().getName());
            map.put("Size",fileStatus.getLen());
            map.put("Type",(fileStatus.isDirectory() ? "dir" : "file"));
            map.put("Path",fileStatus.getPath());
            mapList.add(map);
            System.out.println(fileStatus.getPath());
        }
        fs.close();
        return mapList;
    }

    public boolean rename(String oldName, String newName) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldName);
        Path newpath = new Path(newName);
        boolean isok = fs.rename(oldPath,newpath);
        if(isok){
            System.out.println("rename successs");
        }else {
            System.out.println("rename failure");
        }
        fs.close();
        return isok;
    }

    public boolean delete(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        boolean isok = fs.deleteOnExit(path);
        if(isok){
            System.out.println("delete success");
        }else {
            System.out.println("delete failure");
        }
        fs.close();
        return isok;
    }

    private String readStream(InputStream in){
    try {
        //创建字节数组输出流，用来输出读取到的内容
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //创建缓存大小
        byte[] buffer = new byte[1024];
        //每次读取到的内容长度
        int len = -1;
        while ((len = in.read(buffer)) != -1){
            baos.write(buffer,0,len);
        }
        String content = baos.toString();
        in.close();
        baos.close();
        return content;
    }catch (Exception e){
        e.printStackTrace();
        return e.getMessage();
    }
    }
}
