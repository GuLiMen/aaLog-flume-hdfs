package com.qtech.bigdata.start;

import java.io.*;

public class deleteFile {
    public static void main(String[] args) throws Exception {
//        killProcessTree();
        stopTrain();
        deletefile("C:\\aaLog\\apache-flume-1.9.0-bin\\data");

    }

    private static Long pid() {
        Long result = 0l;

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("C:\\aaLog\\bat\\list.txt")));//构造一个BufferedReader类来读取文件

            String s = null;

            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                if(s.contains("C:\\aaLog\\jdk1.8.0_251\\bin\\java.exe")){
                    String[] s1 = s.split("  ");
                    String s2 = s1[2].replaceAll(" ", "");
                    result = Long.valueOf(s2);
                }

            }
            br.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void stopTrain() {
        //使用WMIC获取CPU序列号
        Process process = null;
        try {
            process = Runtime.getRuntime().exec("taskkill /pid " + pid() + "-t -f -im");
            process.getOutputStream().close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * * 通过进程id杀死进程树，下面所有的进程又会被杀死
     **/
    private static void killProcessTree() {
        try {
            String cmd = getKillProcessTreeCmd();
            Runtime rt = Runtime.getRuntime();
            Process killPrcess = rt.exec(cmd);
            killPrcess.waitFor();
            killPrcess.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getKillProcessTreeCmd() {
        String result = "";
        if (pid() != null) {
            result = "cmd.exe /c taskkill /pid " + pid() + " /F /T /IM";
        }
        return result;
    }

    public static boolean deletefile(String delpath) throws Exception {
        try {

            File file = new File(delpath);
            // 当且仅当此抽象路径名表示的文件存在且 是一个目录时，返回 true
            if (!file.isDirectory()) {
                file.delete();
            } else if (file.isDirectory()) {
                String[] filelist = file.list();
                for (int i = 0; i < filelist.length; i++) {
                    File delfile = new File(delpath + "\\" + filelist[i]);
                    if (!delfile.isDirectory()) {
                        delfile.delete();
                        System.out.println(delfile.getAbsolutePath() + "删除文件成功");
                    } else if (delfile.isDirectory()) {
                        deletefile(delpath + "\\" + filelist[i]);
                    }
                }
                System.out.println(file.getAbsolutePath() + "删除成功");
                file.delete();
            }

        } catch (FileNotFoundException e) {
            System.out.println("deletefile() Exception:" + e.getMessage());
        }
        return true;
    }
}
