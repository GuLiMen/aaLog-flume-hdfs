package com.qtech.bigdata.start.IndiaAA;

import java.io.*;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipUtilsWin7TestBak {

    /***
     * 因为是程序在windows环境上运行，太麻烦，所以把所有方法写在一个类里，方便调度程序，且不用额外的jar包
     */

    private static final int BUFFER_SIZE = 2 * 1024;

    //创建日期方法，用于识别
    public static String date() {

        String time;
        Calendar cal = Calendar.getInstance();
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DATE) - 0;

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String date = df.format(new Date());

        String[] s = date.split(" ");

        //判断当前时间属于白班还是夜班，若属于白班，，则返回白班的字符串
        //判断当前时间属于白班还是夜班，若属于白班，，则返回白班的字符串
        if (s[1].compareTo("10:00:00") > 0 && s[1].compareTo("22:00:00") < 0) {
            time = month + "-" + day + "-D";
        }else if (s[1].compareTo("22:00:00") > 0 && s[1].compareTo("00:00:00") < 0){
            time = month + "-" + day + "-N";
        }
        else{
            day = cal.get(Calendar.DATE) - 1;
            time = month + "-" + day + "-N";
        }
        return time;
    }

    public static String date2() {

        String time;
        Calendar cal = Calendar.getInstance();
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DATE) - 2;

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String date = df.format(new Date());

        String[] s = date.split(" ");

        //判断当前时间属于白班还是夜班，若属于白班，，则返回白班的字符串
        if (s[1].compareTo("10:00:00") > 0 && s[1].compareTo("22:00:00") < 0) {
            time = month + "-" + day + "-D";
        }else if (s[1].compareTo("22:00:00") > 0 && s[1].compareTo("00:00:00") < 0){
            time = month + "-" + day + "-N";
        }
        else{
            day = cal.get(Calendar.DATE) - 1;
            time = month + "-" + day + "-N";
        }
        return time;
    }


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
     * 56
     * 压缩成ZIP 方法2
     * 57
     *
     * @param srcFiles 需要压缩的文件列表
     *                 58
     * @param out      压缩文件输出流
     *                 59
     * @throws RuntimeException 压缩失败会抛出运行时异常
     *                          60
     */
    public static void toZip(List<File> srcFiles, OutputStream out) throws RuntimeException {

        long start = System.currentTimeMillis();

        ZipOutputStream zos = null;

        try {

            zos = new ZipOutputStream(out);

            //打印需要压缩的文件
            for (File srcFile : srcFiles) {

                byte[] buf = new byte[BUFFER_SIZE];

                zos.putNextEntry(new ZipEntry(srcFile.getName()));

                int len;

                FileInputStream in = new FileInputStream(srcFile);

                while ((len = in.read(buf)) != -1) {

                    zos.write(buf, 0, len);

                }

                zos.closeEntry();

                in.close();
            }
            long end = System.currentTimeMillis();
            System.out.println("压缩完成，耗时：" + (end - start) + " ms");
        } catch (Exception e) {
            throw new RuntimeException("zip error from ZipUtils", e);
        } finally {

            if (zos != null) {

                try {

                    zos.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //求差积
    public static List<String> noSaveData() {
        //创建新的集合
        List<String> data = new ArrayList<>();
        //readFileEQ():读取文件中传送过的内容，如果为空，直接写，否则，求出差积
        if (readFile().isEmpty()) {
            //打印符合规则的路径
            for (String sourcePath : getfilenamePATH()) {
                //打印路径下符合规则的文件路径
                for (File targetPath : getfilename(sourcePath)) {
                    //将文件路径添加到新的集合中
                    data.add(targetPath.toString());
                }
            }
        } else {
            for (String sourcePath : getfilenamePATH()) {
                for (File targetPath : getfilename(sourcePath)) {
                    data.add(targetPath.toString());
                }
            }
            //用大集合减去小集合，得出当日差集
            data.removeAll(readFile());
        }
        return data;
    }

    //求差积,和上面那个一样，返回值不同
    public static List<File> noSaveData2() {
        //创建新的集合
        List<File> data = new ArrayList<>();
        //readFileEQ():读取文件中传送过的内容，如果为空，直接写，否则，求出差积
        if (readFile().isEmpty()) {
            for (String sourcePath : getfilenamePATH()) {
                for (File targetPath : getfilename(sourcePath)) {
                    data.add(targetPath);
                }
            }
        } else {
            for (String sourcePath : getfilenamePATH()) {
                for (File targetPath : getfilename(sourcePath)) {
                    data.add(targetPath);
                }
            }

            data.removeAll(readFile());
        }
        return data;
    }


    //读取传输过的文件路径
    public static List<String> readFile() {

        List<String> info = new ArrayList<>();
        String result = "";

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("C:\\aaLog2\\aaLogflumehdfs\\resources\\putListBak")));//构造一个BufferedReader类来读取文件

            String s = null;

            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                info.add(s);
            }
            br.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return info;
    }

    //将差积中每一个文件路径都存入文件里，以便于下次判断
    public static void writeMethod() {
        String fileName = "C:\\aaLog2\\aaLogflumehdfs\\resources\\putListBak";
        try {
            //使用这个构造函数时，如果存在kuka.txt文件，
            //则直接往kuka.txt中追加字符串
            FileWriter writer = new FileWriter(fileName, true);

            for (String getPath : noSaveData()) {
                writer.write(getPath);
                writer.write("\r\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 把符合规则的文件路径存入集合里，供@toZip(压缩文件)方法使用
     *
     * @param path 需要压缩的文件夹路径
     * @return 需要压缩的文件
     */
    public static List<File> getfilename(String path) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

        Calendar nowTime2 = Calendar.getInstance();
        nowTime2.add(Calendar.MINUTE, -30);
        String systemTime = sdf.format(nowTime2.getTime());

        //创建集合，存入符合规则的路径
        List<File> info = new ArrayList<>();

        File file = new File(path);
        //进入UNIT文件夹
        if (file.isDirectory() && file.getName().equalsIgnoreCase("UNIT")) {
            //判断文件夹名称是否符合规则
            //打印符合规则下Unit文件夹
            File[] fileArray = file.listFiles();
            try {
                for (File UNIT : fileArray) {
                    // 是否是文件
                    if (UNIT.isFile()) {
                        String logname = UNIT.getName();
                        //获取除后缀1位的名称
                        String log = logname.substring(0, logname.lastIndexOf("."));
                        // 继续判断是否以后缀名为.log的文件
                        if (logname.endsWith(".log")) {
                            if (!log.equals("0")) {
                                //纯数字
                                Pattern digit = Pattern.compile("\\d+");
                                // 判断Log文件名是否以数字为名
                                if (digit.matcher(log).matches()) {
                                    Date lastModifiedDate = new Date(UNIT.lastModified());
                                    SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");    //实例化一个SimpleDateFormat对象
                                    String lastModifiedDateStr = sdFormat.format(lastModifiedDate);
                                    //因为现在是离线拉取数据，考虑到一些文件在写入过程中被拉取，可能会报错或者抛出异常，索性只拉取前半个小时的数据。判断文件最后修改时间是否半小时前
                                    if (systemTime.compareTo(lastModifiedDateStr) > 0) {
                                        // 把符合规则路径添加到List集合里
                                        info.add(UNIT);
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("zip error from ZipUtils", e);
            }
        }
        return info;
    }

    /**
     * @return 需要压缩的文件夹路径
     * @getfilename 将符合规则的文件夹路径存入集合里，供@getfilename方法使用
     */
    public static List<String> getfilenamePATH() {

        List<String> info = new ArrayList<>();

        String Path = "C:/Lot";

        File file = new File(Path);
        // 获取该目录下所有文件或者文件夹的File数组
        File[] fileArray = file.listFiles();
        // 遍历该File数组，得到每一个File对象，然后判断
        //打印D盘Lot文件夹
        for (File Lot : fileArray) {
            //判断文件夹名称是否符合规则
            if (Lot.toString().contains(date())) {
                //打印lot文件夹下有哪些文件夹符合规则
                for (File Reluse : Lot.listFiles()) {
                    //打印符合规则下Unit文件夹
                    info.add(Reluse.getPath());
                }
            }
        }
        return info;
    }

    //删除文件
    public static void rmr(String filenames) {

        File file = new File(filenames);
        File[] files = file.listFiles();
        try {
            for (File s : files) {
                if (s.getName().endsWith(".log")) {
                    s.delete();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void rmFileContent() {
        File file = new File("C:\\aaLog2\\aaLogflumehdfs\\resources\\putListBak");
        String rl = null;
        String special = date2();
        StringBuffer bf = new StringBuffer();

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            while ((rl = br.readLine()) != null) {
                rl = rl.trim();
                if (rl.indexOf(special) == -1) { //或者!r1.startsWith(special)
                    bf.append(rl).append("\r\n");
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //删除文件
    public static void rmFile(String filenames) {

        File file = new File(filenames);

        try {
            file.delete();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void copyFileUsingChannel(File source, File dest) throws IOException {
        FileChannel sourceChannel = null;
        FileChannel destChannel = null;
        try {
            sourceChannel = new FileInputStream(source).getChannel();
            destChannel = new FileOutputStream(dest).getChannel();
            destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
        } finally {
            sourceChannel.close();
            destChannel.close();
        }
    }

    //执行copy操作
    public static void xCopy() throws IOException {
        //打印差积集合
        for (String getPath : noSaveData()) {
            File sourceFilePath = new File(getPath);
            //判断是否是文件
            if (sourceFilePath.isFile()) {
                //File sourceFilePath = new File(getPath);
                //替换Lot文件。使得
                String targetFilePath = sourceFilePath.toString().replace("Lot", "LotBak");
                File targetPath = new File(targetFilePath);
                String targetDirPath = targetPath.toString().substring(0, targetPath.toString().lastIndexOf("\\"));

                File file = new File(targetDirPath);

                if (!file.exists()) {
                    mkdirDir(targetDirPath);
                }

                if (targetPath.exists()) {
                    rmFile(targetFilePath);
                }

                copyFileUsingChannel(sourceFilePath, targetPath);
            }
        }
    }

    public static void main(String[] args) throws IOException {

        File file2 = new File("C:\\aaLog2\\aaLogflumehdfs\\resources\\putListBak");
        if(!file2.exists()){
            file2.createNewFile();
        }

        //程序入口，所有
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        String uuid2 = "-" + uuid;
        rmFileContent();
        String path = "C:\\LotBak\\compress";
        File targetPath = new File(path);
        //判断文件夹路径是否存在
        if (!targetPath.exists()) {
            mkdirDir(path);
        }

//        toZip(noSaveData(),fileOutputStream);

        //循环差积集合，拿到Lot文件名，拼接输出路径与压缩文件名
        if (!noSaveData().isEmpty()) {
            for (File data : noSaveData2()) {
                String[] sourcePath = data.toString().split("\\\\");
                //拿到Lot文件名
                String LotName = sourcePath[2];
                path = "C:\\LotBak\\compress\\" + LotName + uuid2 + ".zip";
                FileOutputStream fileOutputStream = new FileOutputStream(path, true);
                toZip(noSaveData2(), fileOutputStream);
                //若不为空，则压缩完，将压缩完的文件写入记录文件中
                writeMethod();
            }
        }
    }
}