package com.qtech.bigdata.start.IndiaAA.uploadHDFS;


import com.qtech.bigdata.util.FileSystemManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;

public class uploadHDFS_linux {
    public static void main(String[] args){

        //上传至HDFS上
        upload("/data/aaIndiaDataAcquisition/India");

    }

    public static void upload(String Path){
//HashSet<String> data = new HashSet<>();
        FileSystem fs = FileSystemManager.getFileSystem();
        File file = new File(Path);
        try{
            for (File s:file.listFiles()){
                if(s.isDirectory()){
                    upload(s.toString());
                }else{
                   String targetPath = "/tmp1"+s.toString().replaceAll("D:\\\\","").replaceAll("\\\\","/").replaceAll("/data/aaIndiaDataAcquisition","");
                    fs.copyFromLocalFile(new Path(s.toString()), new Path(targetPath));
                }
                s.delete();
                System.out.println(s.toString());
            }
        }catch (Exception e){
            System.out.println(e);
        }
//        return data;
    }


}