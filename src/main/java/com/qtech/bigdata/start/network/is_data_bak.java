package com.qtech.bigdata.start.network;

import java.io.*;
import java.net.InetAddress;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
//因拿到及台上去运行，故所有方法写在一个类里

public class is_data_bak {

    public static void main(String[] args) throws IOException {
        uploadInfo();
    }


    //上传信息至数据库
    public static void uploadInfo() {
        Connection con = null;
        String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
        String CONNECTION_URL = "jdbc:impala://10.170.3.14:21050/ods_baseinfo;UseSasl=0;AuthMech=3;UID=qtkj;PWD=qt_qt;characterEncoding=utf-8";
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
//        PreparedStatement ps = null;

        try {
            Class.forName(JDBC_DRIVER);
            con = (Connection) DriverManager.getConnection(CONNECTION_URL);

            for (String s :pinging()){
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
                pst.setString(2,cob);
                pst.setString(3,eqid);
                pst.setString(4,deviceNum);
                pst.setString(5,time);
                pst.setString(6,is_network);
                pst.setString(7,is_production);
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

    //ping远程主机返回信息，
    public static List<String> pinging() throws IOException, SQLException {
        List<String> info = new ArrayList<>();
        String indiaDate = indiaTime();
        String eqid = null;
        String state;
        String deviceNum = null;
        String area = null;
        String cob = null;

        if (!readFileEQ().isEmpty()){
            eqid = readFileEQ();
        }
        if(!num().isEmpty()){
            deviceNum = num();
        }
        if(!readFileInfo().isEmpty()){
            String[] split = readFileInfo().split("-");
            area = split[0];
            cob = split[1];
        }
        //测试能否ping通远程主机
        if(ping() == true){
            state = "网络OK";
            if(!getfilenamePATH().isEmpty()){
                info.add(eqid+"、"+state+"、"+"今日已生产"+"、"+deviceNum+"、"+indiaDate+"、"+area+"、"+cob);
            }else{
                info.add(eqid+"、"+state+"、"+"今日未生产"+"、"+deviceNum+"、"+indiaDate+"、"+area+"、"+cob);
            }
        }else{
//            state = "网络未连接";
//            data.append(eqid+"-"+state+"-"+"今日未生产"+"-"+num+"-"+indiaDate);
        }
        return info;
    }

    //查看印度当前时间
    public static String indiaTime(){
        Date dNow = new Date( );
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
        String format = ft.format(dNow);
        //返回当前时间
        return format;
    }

    //读取EQ号，确认机台
    public static String readFileEQ() {
        String result = "";

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("C:\\aaLog2\\aaLogflumehdfs\\resources\\windows.conf")));//构造一个BufferedReader类来读取文件

            String s = null;

            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行

                if (s.toUpperCase().contains("EQ")) {
                    String[] split = s.split("/");

                    String EQ = split[6].substring(0, 16);

                    result = EQ;
                }
            }
            br.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    //读取EQ号，确认机台
    public static String readFileInfo() {
        String result = "";
        String area = null;
        String cob = null;

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("C:\\aaLog2\\aaLogflumehdfs\\resources\\windows.conf")));//构造一个BufferedReader类来读取文件

            String s = null;

            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行

                if (s.toUpperCase().contains("EQ")) {
                    String[] split = s.split("/");

                    area = split[4];
                    cob = split[5];

                    result = area+"-"+cob;
                }
            }
            br.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String num() throws SQLException {
        String num = null;
        for (Map.Entry<String, String> entry : data().entrySet()) {
//                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
            if(entry.getKey().equals(readFileEQ())){
                num = entry.getValue();
            }
        }
        return num;
    }

    public static Map<String, String> data() throws SQLException {

        Map<String, String> data = new HashMap<>();

        Connection connection = null;
        PreparedStatement prepareStatement = null;
        ResultSet rs = null;

        try {
            // 加载驱动
            Class.forName("com.cloudera.impala.jdbc41.Driver");
            // 获取连接
            String url = "jdbc:impala://10.170.3.16:21050/ods_machine_extract;UseSasl=0;AuthMech=3;characterEncoding=utf-8";
            String user = "qtkj";
            String password = "qt_qt";
            connection = DriverManager.getConnection(url, user, password);
            // 获取statement，preparedStatement
            String sql = "select eid,device_num from aa_mes.device_mapping";
            prepareStatement = connection.prepareStatement(sql);
            // 设置参数
//            prepareStatement.setLong(1, 1);
            // 执行查询
            rs = prepareStatement.executeQuery();
            // 处理结果集
            while (rs.next()) {
                String equipmentid = rs.getString("eid");
                String mcid = rs.getString("device_num");

                data.put(equipmentid, mcid);

            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭连接，释放资源
            if (rs != null) {
                rs.close();
            }
            if (prepareStatement != null) {
                prepareStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return data;
    }

    /**
     * @return 查看当天是否生产
     * @getfilename
     */
    public static HashSet<String> getfilenamePATH() {

        HashSet<String> info = new HashSet<>();

        String Path = "D:/Lot";

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
        }else if (s[1].compareTo("22:00:00") > 0 && s[1].compareTo("00:00:00") < 0){
            time = month + "-" + day + "-N";
        }
        else{
            day = cal.get(Calendar.DATE) - 1;
            time = month + "-" + day + "-N";
        }
        return time;
    }

}
