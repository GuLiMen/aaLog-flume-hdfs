package com.qtech.bigdata.start.IndiaAA.uploadHDFS;


import java.io.*;

import com.qtech.bigdata.util.FileSystemManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class uploadHDFS {
    public static void main(String[] args){
        upload("D:\\LotBak\\India");
    }


    public static void upload(String Path){
//        HashSet<String> data = new HashSet<>();
        FileSystem fs = FileSystemManager.getFileSystem();
        File file = new File(Path);
        try{
            for (File s:file.listFiles()){
                if(s.isDirectory()){
                    upload(s.toString());
                }else{
                   String targetPath = "/tmp1"+s.toString().replaceAll("D:\\\\","").replaceAll("\\\\","/").replaceAll("LotBak","");
                    fs.copyFromLocalFile(new Path(s.toString()), new Path(targetPath));
                }
            }
        }catch (Exception e){
            System.out.println(e);
        }
//        return data;
    }

    private static Configuration getConf() {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFS", "hdfs://nameservice");
        conf.set("dfs.nameservices", "nameservice");
        conf.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02");
        conf.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020");
        conf.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020");
        conf.set("dfs.client.failover.proxy.provider.nameservice",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return conf;
    }
}