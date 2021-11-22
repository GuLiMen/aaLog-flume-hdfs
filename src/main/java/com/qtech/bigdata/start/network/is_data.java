package com.qtech.bigdata.start.network;

import java.io.*;
import java.net.InetAddress;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;

//因拿到机台上去运行，故所有方法写在一个类里

//此功能依赖于当前机台已安装AA采集软件，否则不能运行。。（依赖于AA采集软件里“C:\aaLog\apache-flume-1.9.0-bin\job\windows.conf”文件里的EQ号，厂，区等信息）

public class is_data {

    public static void main(String[] args) {
        uploadInfo();
    }

    //上传信息至数据库
    public static void uploadInfo() {
        Connection con = null;
        String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
        String CONNECTION_URL = "jdbc:impala://10.170.3.13:21050/ods_baseinfo;UseSasl=0;AuthMech=3;UID=qtkj;PWD=qt_qt;characterEncoding=utf-8";
        String insertSql = "insert into aa_mes.ads_aa_is_nwtwork(area,cob,eqid,device_number,uploadtime," +
                "is_network,is_production) values(cast(? as  string),cast(? as  string),cast(? as  string)," +
                "cast(? as  string),cast(? as  string),cast(? as  string),cast(? as  string))";
        String time = "null";
        String eqid = "null";
        String is_production = "null";
        String deviceNum = "null";
        String area = "null";
        String cob = "null";
        String is_network = "null";

        try {
            Class.forName(JDBC_DRIVER);
            con = (Connection) DriverManager.getConnection(CONNECTION_URL);

            for (String s : pinging()) {
                String[] split = s.split("、");

                //获取信息
                area = split[5];
                cob = split[6];
                eqid = split[0];
                deviceNum = split[3];
                time = split[4];
                is_network = split[1];
                is_production = split[2];
                PreparedStatement pst = con.prepareStatement(insertSql);
                pst.setString(1, area);
                pst.setString(2, cob);
                pst.setString(3, eqid);
                pst.setString(4, deviceNum);
                pst.setString(5, time);
                pst.setString(6, is_network);
                pst.setString(7, is_production);
                pst.executeUpdate();
                pst.close();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //测试能否ping通远程主机
    public static boolean ping() throws IOException {
        // TODO Auto-generated method stub
        String host = "10.170.3.11";
        int timeOut = 3000; //超时应该在3钞以上
        boolean status = InetAddress.getByName(host).isReachable(timeOut);
        return status;
    }

    public static boolean ping_2() throws IOException {
        // TODO Auto-generated method stub
        String host = "10.170.3.12";
        int timeOut = 3000; //超时应该在3钞以上
        boolean status = InetAddress.getByName(host).isReachable(timeOut);
        return status;
    }

    //拼接信息返回List集合，供jdbc使用
    public static List<String> pinging() throws IOException{
        List<String> info = new ArrayList<>();
        String indiaDate = indiaTime();
        String eqid = null;
        String state;
        String deviceNum = null;
        String area = null;
        String cob = null;
        String production = null;

        //拿到厂，区,EQ
        if (!readFileInfo().isEmpty()) {
            String[] split = readFileInfo().split("-");
            area = split[0];
            cob = split[1];
            eqid = split[2];
        }
        //测试能否ping通远程主机
        if (ping() == true || ping_2() == true) {
            state = "网络OK";
            System.out.println(getfilenamePATH().size());
            if (!getfilenamePATH().isEmpty() && getfilenamePATH().size() >= 20) {
                info.add(eqid + "、" + state + "、" + "当前时段已生产" + "、" + deviceNum + "、" + indiaDate + "、" + area + "、" + cob);
            } else {
                info.add(eqid + "、" + state + "、" + "当前时段未生产" + "、" + deviceNum + "、" + indiaDate + "、" + area + "、" + cob);
            }
        }
        return info;
    }

    //查看印度当前时间
    public static String indiaTime() {
        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = ft.format(dNow);
        //返回当前时间
        return format;
    }


    //读取EQ号，确认机台
    public static String readFileInfo() {
        String result = "";
        String area = null;
        String cob = null;
        String eq = null;

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("C:\\aaLog\\apache-flume-1.9.0-bin\\job\\windows.conf")));//构造一个BufferedReader类来读取文件

            String s = null;

            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行

                if (s.toUpperCase().contains("EQ")) {
                    String[] split = s.split("/");

                    area = split[4];
                    cob = split[5];
                    eq = split[6].substring(0,16);

                    result = area + "-" + cob+"-"+eq;
                }
            }
            br.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * @return 查看当天生产多少产品或是否生产
     * @getfilename
     */
    public static HashSet<String> getfilenamePATH() {
        Calendar cal = Calendar.getInstance();
        HashSet<String> info = new HashSet<>();

        //监控目录
        String Path = "D:/Lot";

        File file = new File(Path);
        // 获取该目录下所有文件或者文件夹的File数组
        File[] fileArray = file.listFiles();
        // 遍历该File数组，得到每一个File对象，然后判断
        //打印D盘Lot文件夹
        for (File Lot : fileArray) {
            //判断文件夹名称是否符合规则
//            if (Lot.toString().contains(date())) {
                if (Lot.toString().contains(date())) {
                //打印lot文件夹下有哪些文件夹符合规则
                for (File Reluse : Lot.listFiles()) {
                    if (Reluse.getName().equals("UNIT")) {
                        for (File log : Reluse.listFiles()) {
                            long time = log.lastModified();
                            cal.setTimeInMillis(time);
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String logFileTime = formatter.format(cal.getTime());
                            //打印符合规则下Unit文件夹
                            if (log.isFile()) {
                                String name = log.getName().substring(0, log.getName().lastIndexOf("."));
                                Pattern digit = Pattern.compile("\\d+");
                                if (digit.matcher(name).matches() && name != "0") {
                                    if(logFileTime.compareTo(current()) >= 0){
                                        info.add(name);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return info;
    }

    public static String current(){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");//设置日期格式
        String date = df.format(new Date());
        return date+":00:00";
    }

    //创建日期方法，用于识别文件夹是否当天数据
    public static String date() {

        String time;
        Calendar cal = Calendar.getInstance();
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DATE) - 0;

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String date = df.format(new Date());

        String[] s = date.split(" ");

        //判断当前时间属于白班还是夜班，若属于白班，，则返回白班的字符串
        if (s[1].compareTo("08:00:00") > 0 && s[1].compareTo("20:00:00") < 0) {
            time = month + "-" + day + "-D";
        } else if (s[1].compareTo("20:00:00") >= 0 && s[1].compareTo("24:00:00") < 0) {
            time = month + "-" + day + "-N";
        } else {
            day = cal.get(Calendar.DATE) - 1;
            time = month + "-" + day + "-N";
        }
        return time;
    }

}
