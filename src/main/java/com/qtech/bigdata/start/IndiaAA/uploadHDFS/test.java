package com.qtech.bigdata.start.IndiaAA.uploadHDFS;


import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.qtech.bigdata.util.FileSystemManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import static com.qtech.bigdata.util.FileSystemManager.getConf;

public class test {

    public static void main(String[] args) throws Exception {
//        Configuration conf = getConf();
//        String localSrc = "D:\\LotBak\\India\\COB1\\EQ01000003300073\\Lot\\11-C0DA01-6-29-D-TEST\\UNIT";
//        String target = "/tmp1/TaiHong/COB7/EQ01000003300179/Lot";
//        File srcFile = new File(localSrc);
        System.out.println(readFileInfo());

    }

    //读取EQ号，确认机台
    public static String readFileInfo() {
        String result = "";
        String area = null;
        String cob = null;

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("./resources/windows.conf")));//构造一个BufferedReader类来读取文件

            String s = null;

            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行

                if (s.toUpperCase().contains("EQ")) {
                    String[] split = s.split("/");

                    area = split[4];
                    cob = split[5];

                    result = area+"-"+cob;
                }
            }
            br.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
