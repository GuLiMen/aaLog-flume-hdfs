package com.qtech.bigdata.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * TODO 一次性工具类。
 * 移动指定天数文件至另一个目录
 */
public class moveHdfs {
    public static void main(String[] args) throws IOException {
        //遍历集合，执行move方法
        List<String> strings = aaList();

        for (String s : strings) {
//            String soutPath = s;
//            String path = s.replace("/flume", "/flume_tmp");
//            mv(soutPath, path);
//            mkdir(path);
            System.out.println(s);
        }
//        mkdir();
//        mv("/flume/GuCheng2/COB2/EQ01000003300059/Lot/7-1-C1QA05-B-10-7-D-PL","/flume_tmp/GuCheng2/COB2/EQ01000003300059/Lot/7-1-C1QA05-B-10-7-D-PL");

    }

    public static ArrayList<String> data(){
        ArrayList<String> data = new ArrayList<>();
//        File file = new File("/aalogfile");
        File file = new File("D:\\LotBak");
        for (File flume : file.listFiles()) {
            for (File area : flume.listFiles()){
                for (File cob : area.listFiles()){
                    for (File eid : cob.listFiles()){
                        for (File Lot : eid.listFiles()){
                            String lotName = Lot.getName();
                            if (lotName.contains("10-15-N") || lotName.contains("10-15-n")){
                                for (File Unit : Lot.listFiles()){
                                    for (File log : Unit.listFiles()){
//                                        for (File result : log.listFiles()) {
                                            System.out.println(log.getPath());
                                            data.add(log.getPath().toString());
//                                        }
                                    }

                                }

                            }
                        }
                    }
                }
            }
        }
        return data;
    }

    public static void upload(){
//        HashSet<String> data = new HashSet<>();
        FileSystem fs = FileSystemManager.getFileSystem();

        try{
            for (String datas :data()){
                File file = new File(datas);
                for (File s:file.listFiles()){
//                    if(s.isDirectory()){
////                        upload(s.toString());
//                    }else{
                        String targetPath = "/tmp1"+s.toString().replaceAll("D:\\\\","").replaceAll("\\\\","/").replaceAll("LotBak","").replaceAll("/aalogfile","");
                        fs.copyFromLocalFile(new Path(s.toString()), new Path(targetPath));
//                        System.out.println("hdfs目录为："+targetPath+"----"+s.toString());
//                    }
                }
            }

        }catch (Exception e){
            System.out.println(e);
        }
//        return data;
    }

    //创建文件夹
    public static void mkdir(String pathNew) throws IOException {
        FileSystem fileSystem = FileSystemManager.getFileSystem();
        Path path = new Path(pathNew);
        boolean result = fileSystem.mkdirs(path);
        System.out.println("result--->" + result);
    }

    //移动文件
    public static boolean mv(String path, String newPath) throws IOException {
        boolean result = false;
        FileSystem fs = null;
        try {
            fs = FileSystemManager.getFileSystem();
            if (!fs.exists(new Path(newPath))) {
                result = fs.rename(new Path(path), new Path(newPath));
            } else {
//                LOGGER.warn("HDFS上目录： {} 被占用！",newPath);
                fs.delete(new Path(newPath), true);
                result = fs.rename(new Path(path), new Path(newPath));
                System.out.println("移动成功");
//                System.out.println("HDFS上目录： {} 被占用！" + newPath);
            }
        } catch (Exception e) {
//            LOGGER.error("移动HDFS上目录：{} 失败！", path, e);
            System.out.println("HDFS上目录： {} 被占用！" + newPath);
        } finally {
            fs.close();
        }
        return result;
    }


    public static List<String> aaList() throws IOException {

        //获取文件配置信息
        FileSystem fileSystem = FileSystemManager.getFileSystem();

        //创建StringBuilder对象，添加各厂机台信息
        List<String> info = new ArrayList<>();
        //厂
        for (FileStatus Factory : fileSystem.listStatus(new Path("/flume"))) {

            //区
            for (FileStatus cob : fileSystem.listStatus(new Path(Factory.getPath().toString()))) {

                //唯一码
                for (FileStatus EQcode : fileSystem.listStatus(new Path(cob.getPath().toString()))) {

                    //Lot
                    for (FileStatus Lot : fileSystem.listStatus(new Path(EQcode.getPath().toString()))) {

//                        if(Factory.getPath().getName().equals("TaiHong")){
//                            info.add(Lot.getPath().toString().replace("hdfs://nameservice", ""));
//                        }

                        //Lot文件名
                        for (FileStatus result : fileSystem.listStatus(new Path(Lot.getPath().toString()))) {
                            //获取Lot文件名
//                            if (Factory.getPath().getName().equals("TaiHong")) {
                            String dir = result.getPath().getName();
                            try {
                                // | dir.contains(Date_D())
                                if (dir.contains("10-17-N") | dir.contains("10-17-D") ) {
                                    info.add(result.getPath().toString().replace("hdfs://nameservice", ""));
                                }
                            } catch (IndexOutOfBoundsException e) {
                                System.out.println("Exception thrown  :" + e);
                            }
//                            }
                        }
                    }

                }
            }
        }
        //关闭hdfs资源
        fileSystem.close();
        //返回buf
        return info;
    }

    //指定天数。因数据量太多，故分开跑。且若不指定D-N或出现bug。故日期后指定D或N（D表是白班，N表示夜班）
    public static String Date_D() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -2);
        String yesterday = new SimpleDateFormat("MM-dd").format(cal.getTime());
        String[] split = yesterday.split("-");

        int month = Integer.parseInt(split[0]);
        int date = Integer.parseInt(split[1]);

        String monthNew = "";
        String dateNew = "";

        if (month < 10) {
            monthNew = Integer.toString(month).replace("0", "");
        } else {
            monthNew = Integer.toString(month);
        }
        if (date < 10) {
            dateNew = Integer.toString(date).replace("0", "");
        } else {
            dateNew = Integer.toString(date);
        }
        return monthNew + "-" + dateNew + "-D";
    }

    public static String Date_N() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -2);
        String yesterday = new SimpleDateFormat("MM-dd").format(cal.getTime());
        String[] split = yesterday.split("-");

        int month = Integer.parseInt(split[0]);
        int date = Integer.parseInt(split[1]);

        String monthNew = "";
        String dateNew = "";

        if (month < 10) {
            monthNew = Integer.toString(month).replace("0", "");
        } else {
            monthNew = Integer.toString(month);
        }
        if (date < 10) {
            dateNew = Integer.toString(date).replace("0", "");
        } else {
            dateNew = Integer.toString(date);
        }
        return monthNew + "-" + dateNew + "-N";
    }
}
