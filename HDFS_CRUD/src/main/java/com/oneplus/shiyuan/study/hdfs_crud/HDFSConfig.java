package com.oneplus.shiyuan.study.hdfs_crud;

import org.apache.hadoop.conf.Configuration;

public class HDFSConfig {
    static  Configuration conf = new Configuration();
    static{
        conf.set("fs.defaultFS","192.168.137.6:9000");
    }

}
