package com.qtech.bigdata.util;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class deleteFile {

    public static void main(String[] args) throws IOException {
        file();
//        System.out.println(time());
    }

    public static void file(){
        File file = new File("/aalogfile/flume");

        //厂
        for (File Factory : file.listFiles()) {
            //区
            for (File cob : Factory.listFiles()) {
                //唯一码
                for (File EQcode : cob.listFiles()) {
                    //Lot
                    for (File Lot : EQcode.listFiles()) {
                        //Lot文件名
                        for (File result : Lot.listFiles()) {
                            Calendar cal = Calendar.getInstance();
                            long time = result.lastModified();
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            cal.setTimeInMillis(time);
                            String fileTime = formatter.format(cal.getTime());
                            if(time().compareTo(fileTime) > 0){
                                System.out.println(result.getPath());
                                System.out.println("修改时间 " + formatter.format(cal.getTime()));
                                System.out.println("一年前时间为"+time());
                                deleteDir(result);
                            }
                        }
                    }
                }
            }
        }
    }

    public static String time() {
        Calendar c = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        c.setTime(new Date());
        c.add(Calendar.MONTH, -6);
        Date y = c.getTime();
        String year = format.format(y);
        return year;
    }


    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}
