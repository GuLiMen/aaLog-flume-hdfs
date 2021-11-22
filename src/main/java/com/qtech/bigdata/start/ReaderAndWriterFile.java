package com.qtech.bigdata.start;
import com.qtech.bigdata.util.FileSystemManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class ReaderAndWriterFile {

    public static void main(String[] args) {
        String filePath = "C:\\aaLog\\apache-flume-1.9.0-bin\\job\\windows.conf"; // 文件路径
        ReaderAndWriterFile obj = new ReaderAndWriterFile();
        for(int i=0;i<10;i++){
            obj.write(filePath, obj.read(filePath)); // 读取修改文件
        }
    }



    /**
     * 读取文件内容
     * @param filePath
     * @return
     */

    public String read(String filePath) {
        BufferedReader br = null;
        String line = null;
        StringBuffer buf = new StringBuffer();
        try {
            // 根据文件路径创建缓冲输入流
            br = new BufferedReader(new FileReader(filePath));
            // 循环读取文件的每一行, 对需要修改的行进行修改, 放入缓冲对象中
            while ((line = br.readLine()) != null) {
                // 此处根据实际需要修改某些行的内容

                if (line.contains("EQ")) {
                    String[] EQ = line.split("/");
                    String eq=EQ[6].substring(0,EQ[6].indexOf("%"));
                    String restr=eq();//EQ码写进去的
//                    String restr="ynhtbgfvcdxsujmynhtbgfjg";//EQ码写进去的
                    buf.append(line.replace(eq,restr));
                } else if (line.startsWith("b")) {
                    buf.append(line).append(" start with b");
                }

               // 如果不用修改, 则按原来的内容回写
                else {
                    buf.append(line);
                }
                buf.append(System.getProperty("line.separator"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        // 关闭流
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    br = null;
                }
            }

        }
        return buf.toString();
    }
    /**
     * 将内容回写到文件中
     *
     * @param filePath
     * @param content
     */

    public void write(String filePath, String content) {
        BufferedWriter bw = null;
        try {
            // 根据文件路径创建缓冲输出流
            bw = new BufferedWriter(new FileWriter(filePath));
            // 将内容写入文件中
            bw.write(content);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭流
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    bw = null;
                }
            }
        }
    }

    private static String eq() {
        String filePath ="C:\\aaLog\\aaLogOneClickInstallation\\EQ.txt"; // 文件路径
        BufferedReader br = null;
        String line = null;
        String eq="";
        StringBuffer buf = new StringBuffer();
        try {
            // 根据文件路径创建缓冲输入流
            br = new BufferedReader(new FileReader(filePath));
            // 循环读取文件的每一行, 对需要修改的行进行修改, 放入缓冲对象中
            while ((line = br.readLine()) != null) {
                eq=line;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return eq;
    }
}