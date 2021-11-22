package com.qtech.bigdata.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class readEid {

    /**
     * 按行读取txt文件并返回一个List<String>集合
     * @param path
     * @return List<String>
     */
    public static List<String> getFileContent(String path) {
        List<String> strList = new ArrayList<>();
        File file = new File(path);
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            // 将字节流向字符流转换
            inputStreamReader = new InputStreamReader(new FileInputStream(file), "utf-8");
            // 创建字符流缓冲区
            bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            // 按行读取
            while ((line = bufferedReader.readLine()) != null) {
                strList.add(line);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStreamReader != null) {
                try {
                    inputStreamReader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return strList;
    }
}
