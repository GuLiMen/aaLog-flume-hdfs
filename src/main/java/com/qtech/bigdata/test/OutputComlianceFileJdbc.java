package com.qtech.bigdata.test;

import com.qtech.bigdata.comm.SendEMailWarning_bak1;
import com.qtech.bigdata.util.FileSystemManager;
import com.qtech.bigdata.util.testDate;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;

public class OutputComlianceFileJdbc {

    public static void main(String[] args) throws IOException {

        InfoSum();

    }

        public static String OutputComlianceFile_mail() throws IOException {

            //获取文件配置信息
            FileSystem fileSystem = FileSystemManager.getFileSystem();

            //创建StringBuilder对象，返回字符串
            StringBuilder buf = new StringBuilder();

            //指定目录
            for (FileStatus Factory : fileSystem.listStatus(new Path("/flume"))) {

                for (FileStatus cob : fileSystem.listStatus(new Path(Factory.getPath().toString()))) {

                    for (FileStatus EQcode : fileSystem.listStatus(new Path(cob.getPath().toString()))) {

                        for (FileStatus Lot : fileSystem.listStatus(new Path(EQcode.getPath().toString()))) {

                            for (FileStatus result : fileSystem.listStatus(new Path(Lot.getPath().toString()))) {

                                String dir = result.getPath().toString();

                                if (dir.contains(testDate.DateMonthDay())) {

                                    if (!dir.contains("1" + testDate.DateMonthDay())) {
                                        String[] split = dir.split("/");
                                        String factory = split[4];
                                        String region = split[5];
                                        String machine = split[8];

                                        if (factory.equals("GuCheng")) {
                                            String factory_test = factory.replace("GuCheng", "古城");
                                            String[] split1 = machine.split("-");

                                            buf.append(factory_test + "、" + region + "、" + split1[0] + "-" + split1[1] +"、"+EQcode.getPath().getName()+"、"+result.getPath().getName());
                                            buf.append("\r\n");
                                        } else if (factory.equals("TaiHong")) {
                                            String factory_test = factory.replace("TaiHong", "台虹");
                                            String[] split1 = machine.split("-");
                                            buf.append(factory_test + "、" + region + "、" + split1[0] +"、"+EQcode.getPath().getName()+"、"+result.getPath().getName());
                                            buf.append("\r\n");

                                        } else {
                                            String factory_test = factory.replace("ChengBei", "汉浦");
                                            String[] split1 = machine.split("-");
                                            buf.append(factory_test + "、" + region + "、" + split1[0] +"、"+EQcode.getPath().getName()+"、"+result.getPath().getName());
                                            buf.append("\r\n");

                                        }
                                    }
                                } else if (dir.contains(testDate.DateMonthDay1())) {

                                    if (!dir.contains("1" + testDate.DateMonthDay1())) {
                                        String[] split = dir.split("/");
                                        String factory = split[4];
                                        String region = split[5];
                                        String machine = split[8];

                                        if (factory.equals("GuCheng")) {
                                            String factory_test = factory.replace("GuCheng", "古城");
                                            String[] split1 = machine.split("-");
                                            buf.append(factory_test + "、" + region + "、" + split1[0] + "-" + split1[1] +"、"+EQcode.getPath().getName()+"、"+result.getPath().getName());
                                            buf.append("\r\n");

                                        } else if (factory.equals("TaiHong")) {
                                            String factory_test = factory.replace("TaiHong", "台虹");
                                            String[] split1 = machine.split("-");
                                            buf.append(factory_test + "、" + region + "、" + split1[0] +"、"+EQcode.getPath().getName()+"、"+result.getPath().getName());
                                            buf.append("\r\n");


                                        } else {
                                            String factory_test = factory.replace("ChengBei", "汉浦");
                                            String[] split1 = machine.split("-");

                                            buf.append(factory_test + "、" + region + "、" + split1[0] +"、"+EQcode.getPath().getName()+"、"+result.getPath().getName());
                                            buf.append("\r\n");

                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            //关闭hdfs资源
            fileSystem.close();
            return buf.toString();
        }


        public static void InfoSum() throws IOException {
            int qiQu = 0;
            int LiuQu = 0;
            int SanQu = 0;
            int yiQu = 0;
            int wuQu = 0;

            int qiQuLotError = 0;
            int qiQueqError = 0;

            int LiuQuLotError = 0;
            int LiuQueqError = 0;

            int SanQuLotError = 0;
            int SanQueqError = 0;

            int yiQuLotError = 0;
            int yiQueqError = 0;

            int wuQuLotError = 0;
            int wuQueqError = 0;

            String[] line = OutputComlianceFile_mail().split("\r\n");

            for (String data : line){
                String[] split = data.split("、");

                if (split[1].equalsIgnoreCase("cob1")){

                    yiQu++;

                    if (split[3].length() > 16){
                        yiQueqError++;
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            yiQuLotError++;
                        }
                    }else{
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            yiQuLotError++;
                        }
                    }
                }else if (split[1].equalsIgnoreCase("cob5")){

                    wuQu++;

                    if (split[3].length() > 16){
                        wuQueqError++;
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            wuQuLotError++;
                        }
                    }else{
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            wuQuLotError++;
                        }
                    }
                }else if(split[1].equalsIgnoreCase("cob3")){

                    SanQu++;
                    if (split[3].length() > 16){
                        SanQueqError++;
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            SanQuLotError++;
                        }
                    }else{
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            SanQuLotError++;
                        }
                    }
                }else if (split[1].equalsIgnoreCase("cob6")){

                    LiuQu++;
                    if (split[3].length() > 16){
                        LiuQueqError++;
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            LiuQuLotError++;
                        }
                    }else{
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            LiuQuLotError++;
                        }
                    }
                }else if (split[1].equalsIgnoreCase("cob7")){
                    qiQu++;
                    if (split[3].length() > 16){
                        qiQueqError++;
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            qiQuLotError++;
                        }
                    }else{
                        String[] split1 = split[4].split("-");
                        if (split1[0].length() > 4 ){
                            qiQuLotError++;
                        }
                    }
                }
            }



            StringBuilder content = new StringBuilder();
//            content.append("今日上传数据机台共"); //第一列
//            content.append(qiQu); //第二列
//            content.append(LiuQu); //第三列
//            content.append(SanQu); //第四列
//            content.append(yiQu); //第五列
//            content.append(wuQu); //第六列

//            content.append("EQ错误机台共"); //第一列
//            content.append(qiQueqError ); //第二列
//            content.append(LiuQueqError); //第三列
//            content.append(SanQueqError); //第四列
//            content.append(yiQueqError); //第五列
//            content.append(wuQueqError); //第六列
//
//            content.append("Lot文件名错误机台共"); //第一列
//            content.append(qiQuLotError ); //第二列
//            content.append(LiuQuLotError); //第三列
//            content.append(SanQuLotError); //第四列
//            content.append(yiQuLotError); //第五列
//            content.append(wuQuLotError); //第六列

            for (String data: line) {

                String[] split = data.split("、");

                if (split[3].length() > 16){

                    String s = "EQ码不正确";
                    String[] split1 = split[4].split("-");

                    if (split1[0].length() > 4 ){

                        content.append(split[0]+"、"); //第一列
                        content.append(split[1]+"、"); //第二列
                        content.append(split[2]+"、"); //第三列
                        content.append(split[3]+"、"); //第四列
                        content.append(split[4]+"、"); //第五列
                        content.append(s+"、");        //第六列
                        content.append("Lot文件名不正确");        //第七列
                        content.append("\r\n");


                    }else{

                        content.append(split[0]+"、"); //第一列
                        content.append(split[1]+"、"); //第二列
                        content.append(split[2]+"、"); //第三列
                        content.append(split[3]+"、"); //第四列
                        content.append(split[4]+"、"); //第五列
                        content.append(s+"、" );        //第六列
                        content.append("Lot文件名正确");        //第七列
                        content.append("\r\n");

                    }

                }else{
                    String s = "EQ码正确";
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4){
                        content.append(split[0]+"、"); //第一列
                        content.append(split[1]+"、"); //第二列
                        content.append(split[2]+"、"); //第三列
                        content.append(split[3]+"、"); //第四列
                        content.append(split[4]+"、"); //第五列
                        content.append(s+"、");        //第六列
                        content.append("Lot文件名不正确" );        //第七列
                        content.append("\r\n");
                    }else{
                        content.append(split[0]+"、"); //第一列
                        content.append(split[1]+"、"); //第二列
                        content.append(split[2]+"、"); //第三列
                        content.append(split[3]+"、"); //第四列
                        content.append(split[4]+"、"); //第五列
                        content.append(s+"、");        //第六列
                        content.append("Lot文件名正确");        //第七列
                        content.append("\r\n");
                    }
                }
            }

            Connection con = null ;
            String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
            String CONNECTION_URL = "jdbc:impala://10.170.3.14:21050/ods_machine_extract;UseSasl=0;AuthMech=3;UID=qtkj;PWD=;characterEncoding=utf-8";
            PreparedStatement ps = null;

            String sql = "insert into aa_mes.ADS_AA_STATE_LIST (id,CREATAE_TIME,FACTORY_NAME,WORKSHOP_CODE,MACHINE_NO,DEVICE_E_ID,LOT_FILE_NAME) values (cast(? as string),cast(? as string),cast(? as string)" +
                    ",cast(? as string),cast(? as string),cast(? as string),cast(? as string))";
            String[] jdbc = content.toString().split("、");
            try {

                Class.forName(JDBC_DRIVER);
                con = (Connection) DriverManager.getConnection(CONNECTION_URL);
                for (int i = 0; i < jdbc.length; i++) {
                    ps = con.prepareStatement(sql);
                    String s = jdbc[0];
                    String s1 = jdbc[1];
                    String s2 = jdbc[2];
                    String s3 = jdbc[3];
                    String s4 = jdbc[4];
                    String s5 = jdbc[5];
                    String s6 = jdbc[6];

//                    ps.setString(1, uuid);
//                    ps.setString(2, date.toString());
                    ps.setString(3, s);
                    ps.setString(4, s1);
                    ps.setString(5, s2);
                    ps.setString(6, s3);
                    ps.setString(7, s4);
                    ps.execute();

                }
            }catch (ClassNotFoundException e) {

                e.printStackTrace();
            } catch (SQLException e) {

                e.printStackTrace();
            }finally {
                try {
                    con.close();
//                ps.close();

                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
     }

}
