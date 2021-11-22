package com.qtech.bigdata.start.IndiaAA.uploadHDFS;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class decompression {
    private static final int BUFFER_SIZE = 2 * 1024;

    /**
     * 递归创建目录
     *
     * @param pathName 传入要创建的目录
     */
    public static void mkdirDir(String pathName) {
        String dir = pathName;
        File f = new File(dir);
        boolean flag = f.mkdirs();
        System.out.println(flag);
    }




    /**
     * zip解压
     *
     * @param srcFile     zip源文件
     * @param destDirPath 解压后的目标文件夹
     * @throws RuntimeException 解压失败会抛出运行时异常
     */

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
                    if (!targetFile.getParentFile().exists()) {
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

            System.out.println("解压完成，耗时：" + (end - start) + " ms");

        } catch (Exception e) {
            throw new RuntimeException("unzip error from ZipUtils", e);

        } finally {
            if (zipFile != null) {
                try {
                    zipFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //读取文件EID与机台号。使机台号与Lot文件名机台号关联，拿到对用EID
    public static Map<String, String> readFileByLines(String fileName) {
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
                try {
                    //切割文件内容，01--
                    String[] datas = tempString.split("-");
                    //
                    data.put(datas[0], datas[1]);
                } catch (ArrayIndexOutOfBoundsException e) {
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


    //拆分上传文件名，解压放到解析文件名后路径内
    public static Map<String, String> readDir() {

        Map<String, String> data = new HashMap<>();
        String eid = null;
        String cob = "COB1";
        String area = "India";
        String Lot = "Lot";
        String Lotname = "";

        //给定目录
        String Path = "D:\\LotBak";

        File file = new File(Path);
        // 获取该目录下所有文件或者文件夹的File数组
        File[] fileArray = file.listFiles();

        if (fileArray.length != 0) {
            // 遍历该File数组，得到每一个File对象，然后判断
            for (File fileList : fileArray) {
                //得到文件名
                String fileName = fileList.getName();
                String filePath = fileList.getPath();

                //判断文件后缀名是否zip
                if (fileName.endsWith(".zip")) {

                    System.out.println(filePath);
                    //切割拿到的文件名
                    String[] split = fileName.split("-");
                    Lotname = fileName.substring(0, fileName.lastIndexOf("-"));
                    //读取配置文件，拿到机台号对应的EQ码
                    for (Map.Entry<String, String> entry : readFileByLines("./resources/EQ").entrySet()) {
//                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                        //判断机台号是否相等
                        if (entry.getValue().equalsIgnoreCase(split[0])) {
                            eid = entry.getKey();
                            //添加压缩文件夹路径与解压后路径至集合中
                            data.put(filePath, Path + "\\" + area + "\\" + cob + "\\" + eid + "\\" + Lot + "\\" + Lotname+"\\UNIT");
                        }
                    }
                }
            }
        }
        return data;
    }

    public static void main(String[] args) throws IOException {

        //打印路径
        for (Map.Entry<String, String> entry : readDir().entrySet()) {
            File sourceFile = new File(entry.getKey());
            File targetFile = new File(entry.getValue());
            //若解压后路径不存在则创建
            if (!targetFile.exists()){
                mkdirDir(entry.getValue());
            }


            //解压至本地
//            unZip(sourceFile,entry.getValue());
            sourceFile.delete();
        }
    }
}
