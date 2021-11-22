package com.qtech.bigdata.test;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test1 {

    public static void main(String args[]) throws IOException {
//        System.out.println(date2());
        System.out.println(transformation("BALL SHEAR5(LEAD)"));
    }


    //读取文件EID与机台号。使机台号与Lot文件名机台号关联，拿到对用EID
    public static Map<String, String> readFileByLinesNew(String fileName) {
        File file = new File(fileName);

        Map<String, String> data = new HashMap<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                try {
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

    public static String transformation(String testitem) {
        String testitemNew = testitem.toUpperCase();

        String BALL = ".*BALL.*SHEAR.*LEAD.*";
        String BALL2 = ".*推力.*LEAD.*";
        String BALL3 = ".*LEAD%BALL.*SHEAR.*";
        String BALL4 = ".*LEAD.*推力.*'";
        String BALL5 = ".*推力.*";
        String BALL6 = ".*BALL.*SHEAR.*";

        String WIRE_PULL = ".*WIRE.*PULL.*";
        String WIRE_PULL2 = ".*拉力.*";

        if(Pattern.matches(BALL5, testitemNew) || Pattern.matches(BALL6, testitemNew) ){
            if (Pattern.matches(BALL, testitemNew) || Pattern.matches(BALL2, testitemNew) || Pattern.matches(BALL3, testitemNew) || Pattern.matches(BALL4, testitemNew)) {
                testitemNew = "BALL_SHEAR_LEAD";
            } else {
                testitemNew = "BALL_SHEAR_PAD";
            }
        }else if(Pattern.matches(WIRE_PULL, testitemNew) || Pattern.matches(WIRE_PULL2, testitemNew)){
                testitemNew = "WIRE_PULL";
        }else{
            testitemNew = testitem;
        }

        return testitemNew;
    }

    public static String current() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");//设置日期格式
        String date = df.format(new Date());
        return date + ":00:00";
    }

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
        if (s[1].compareTo("10:00:00") > 0 && s[1].compareTo("22:00:00") < 0) {
            time = month + "-" + day + "-D";
        } else if (s[1].compareTo("22:00:00") > 0 && s[1].compareTo("00:00:00") < 0) {
            time = month + "-" + day + "-N";
        } else {
            day = cal.get(Calendar.DATE) - 1;
            time = month + "-" + day + "-N";
        }
        return time;
    }

    public static void rmFileContent() {
        File file = new File("./resources/putListBak");
        String rl = null;
        String special = date2();
        StringBuffer bf = new StringBuffer();

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            while ((rl = br.readLine()) != null) {
                rl = rl.trim();
                if (rl.indexOf(special) < 0) { //或者!r1.startsWith(special)
                    bf.append(rl).append("\r\n");
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
        } else if (s[1].compareTo("22:00:00") > 0 && s[1].compareTo("00:00:00") < 0) {
            time = month + "-" + day + "-N";
        } else {
            day = cal.get(Calendar.DATE) - 1;
            time = month + "-" + day + "-N";
        }
        return time;
    }
}