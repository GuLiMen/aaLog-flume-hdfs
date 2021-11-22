package com.qtech.bigdata.util;

import com.qtech.bigdata.comm.SendEMailWarning;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static com.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;

public class FileSystemManager {

    private static List<FileSystem> fileSystemPool = new LinkedList<FileSystem>();


    public static FileSystem getFileSystem() {

        FileSystem fileSystem = null;

        try {
            fileSystem = FileSystem.get(getConf());

        } catch (IOException e) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.core.batch.InitConifg.getFileSystem() Job ERROR", "cn.qtech.bigdata.core.batch.InitConifg.getFileSystem() Job 异常 !! 语句: fileSystem = FileSystem.get(conf); \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());
            e.printStackTrace();
        }
        return fileSystem;
    }

    public static Configuration getConf() {
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


    public static void closeFileSystemPool() {
        if (fileSystemPool.size() == 0) {
            System.out.println("fileSystemPool.size() == 0");
            return;
        }
        for (FileSystem fileSystem : fileSystemPool) {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    System.out.println("关闭文件系统池失败 !!");
                    e.printStackTrace();
                }
            }
        }

    }


    public static FileSystem getFileSystemFromPool() {
        System.out.println(fileSystemPool.size()+"  fileSystemPool.size()");
        while (fileSystemPool.size() == 0) {
            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        FileSystem fileSystem = fileSystemPool.remove(0);
        return fileSystem;
    }


    public static void returnFileSystem(FileSystem fileSystem) {
        if (fileSystem != null) {
            System.out.println(fileSystemPool.size()+"  returnFileSystem != null");

            fileSystemPool.add(fileSystem);
        }

    }
}
