package com.qtech.bigdata.util;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class indiaUtil {
     static final int BUFFER_SIZE = 2 * 1024;

    public static void unZip(File srcFile, String destDirPath) throws RuntimeException {
        long start = System.currentTimeMillis();

        // 判断源文件是否存在

        if (!srcFile.exists()) {
            throw new RuntimeException(srcFile.getPath() + "所指文件不存在");

        }

        // 开始解压

        ZipFile zipFile = null;

        try {
            zipFile = new ZipFile(srcFile);

            Enumeration<?> entries = zipFile.entries();

            while (entries.hasMoreElements()) {
                ZipEntry entry = (ZipEntry) entries.nextElement();

                System.out.println("解压" + entry.getName());

                // 如果是文件夹，就创建个文件夹

                if (entry.isDirectory()) {
                    String dirPath = destDirPath + "/" + entry.getName();

                    File dir = new File(dirPath);

                    dir.mkdirs();

                } else {
                    // 如果是文件，就先创建一个文件，然后用io流把内容copy过去

                    File targetFile = new File(destDirPath + "/" + entry.getName());

                    // 保证这个文件的父文件夹必须要存在

                    if(!targetFile.getParentFile().exists()){
                        targetFile.getParentFile().mkdirs();

                    }

                    targetFile.createNewFile();

                    // 将压缩文件内容写入到这个文件中

                    InputStream is = zipFile.getInputStream(entry);

                    FileOutputStream fos = new FileOutputStream(targetFile);

                    int len;

                    byte[] buf = new byte[BUFFER_SIZE];

                    while ((len = is.read(buf)) != -1) {
                        fos.write(buf, 0, len);

                    }

                    // 关流顺序，先打开的后关闭

                    fos.close();

                    is.close();

                }

            }

            long end = System.currentTimeMillis();

            System.out.println("解压完成，耗时：" + (end - start) +" ms");

        } catch (Exception e) {
            throw new RuntimeException("unzip error from ZipUtils", e);

        } finally {
            if(zipFile != null){
                try {
                    zipFile.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //读取文件EID与机台号。使机台号与Lot文件名机台号关联，拿到对用EID
    public static Map<String,String> readFileByLines(String fileName) {
        File file = new File(fileName);

        Map<String, String> data = new HashMap<>();
        BufferedReader reader = null;
        try {
//            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
//            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
//                System.out.println("line " + line + ": " + tempString);
//                line++;
                try{
                    //切割文件内容，1-EQID，2-机台号
                    String[] datas = tempString.split("-");
                    data.put(datas[0],datas[1]);
                }catch (ArrayIndexOutOfBoundsException e){
                    System.out.println(tempString);
                }

            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return data;
    }


    //解析上传文件名，解压放到解析文件名后路径内
    public static HashSet<String> readDir() {

        HashSet<String> info = new HashSet<>();
        String eid = null;
        String cob = "COB1";
        String area = "India";
        String Lot = "Lot";

        //给定目录
        String Path = "D:\\LotBak\\compress";

        File file = new File(Path);
        // 获取该目录下所有文件或者文件夹的File数组
        File[] fileArray = file.listFiles();
        // 遍历该File数组，得到每一个File对象，然后判断
        for (File fileList : fileArray) {
            //得到文件名
            String fileName = fileList.getName();
            //判断文件后缀名是否zip
            if(fileName.endsWith(".zip")){
                //切割拿到的文件名
                String[] split = fileName.split("-");

                //读取配置文件，拿到机台号对应的EQ码
                for (Map.Entry<String, String> entry : readFileByLines("./resources/EQ").entrySet()) {
//                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                    //判断机台号是否相等
                    if(entry.getValue().equalsIgnoreCase(split[0])){
                        eid = entry.getKey();

                    }

                }

            }
        }
        return info;
    }


    //读取文件路径，供解压用
    public static List<String> readList() throws IOException {

        List<String> strings = new ArrayList<>();

        //设置目录
        File path = new File("D:\\LotBak\\compress");
        //读取文件夹下的zip文件
        File[] zipFiles = path.listFiles();

        for (File file : zipFiles) {
            if (file.getName().endsWith(".zip")) {
                strings.add(file.getPath());
            }
        }
        return strings;
    }
}
