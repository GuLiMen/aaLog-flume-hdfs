package com.qtech.bigdata.start.increase;

import com.qtech.bigdata.comm.SendEMailWarning_bak2;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * AA数采异常邮件
 */
import static com.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;
import static com.qtech.bigdata.comm.AppConstants.RECEIVE_MAIL_EQUIPMENT;
public class abnormalMail {
    //主程序运行
    public static void main(String[] args) throws IOException {

        if(!jdbcData().isEmpty()){
            sendail();
        }else{
            System.out.println("没有匹配的数据");
        }
//        System.out.println(Date_D());


    }

    public static void sendail() {
        try {
            //计算总和
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

            int erQuLotError = 0;
            int erQueqError = 0;
            for (String datatest : OutputComlianceFile_mail_new.aaList_nd()) {
                String[] split = datatest.split("、");
                String factory = split[0];
                String machine = split[1];
                String region = split[2];
                String eid = split[3];
                String subpath = split[4];

                if (factory.equalsIgnoreCase("古城")) {
                    if (machine.equalsIgnoreCase("COB1")) {
                        yiQu++;
                    }
                } else if (factory.equalsIgnoreCase("台虹")) {

                    if (machine.equalsIgnoreCase("cob3")) {
                        SanQu++;
                    } else if (machine.equalsIgnoreCase("cob6")) {
                        LiuQu++;
                    } else if (machine.equalsIgnoreCase("cob7")) {
                        qiQu++;
                    }
                } else if (factory.equalsIgnoreCase("古二")) {
                    wuQu++;
                }
            }

            for (String datas :jdbcData()){
                String[] data = datas.split(",");
                String factory_new = data[0];
                String cob_new = data[1];
                String is_network_new = data[5];
                String remarks_new = data[7];

                if(factory_new.equals("古一") && cob_new.equals("COB1") && remarks_new.equals("Lot错误")){
                    yiQuLotError++;
                }else if(factory_new.equals("古二") && cob_new.equals("COB2") && remarks_new.equals("Lot错误")){
                    erQuLotError++;
                }else if(factory_new.equals("台虹") && cob_new.equals("COB7") && remarks_new.equals("Lot错误")){
                    qiQuLotError++;
                }else if(factory_new.equals("台虹") && cob_new.equals("COB6") && remarks_new.equals("Lot错误")){
                    LiuQuLotError++;
                }else if(factory_new.equals("台虹") && cob_new.equals("COB3") && remarks_new.equals("Lot错误")){
                    SanQuLotError++;
                }
            }

            StringBuilder content = new StringBuilder("<html><head></head><body><h2>各厂区昨日上传机台汇总</h2>");
            content.append("<table border=\"5\" style=\"border:solid 1px #E8F2F9;font-size=14px;;font-size:18px;\">");
            content.append("<tr style=\"background-color: #428BCA; color:#ffffff\"><th>各厂区机台上传汇总</th><th>台虹七区</th><th>台虹六区</th><th>台虹三区</th><th>古城一厂</th><th>古城二厂</th></tr>");
            content.append("</tr>");
            content.append("<tr>");
            content.append("<td>" + "各厂区AA机台总数" + "</td>"); //第一列
            content.append("<td>" + "38" + "</td>"); //第二列
            content.append("<td>" + "14" + "</td>"); //第三列
            content.append("<td>" + "10" + "</td>"); //第四列
            content.append("<td>" + "64" + "</td>"); //第五列
            content.append("<td>" + "25" + "</td>"); //第六列
            content.append("</tr>");
            content.append("<tr>");
            content.append("<td>" + "昨日AA上传数据机台共" + "</td>"); //第一列
            content.append("<td>" + qiQu + "</td>"); //第二列
            content.append("<td>" + LiuQu + "</td>"); //第三列
            content.append("<td>" + SanQu + "</td>"); //第四列
            content.append("<td>" + yiQu + "</td>"); //第五列
            content.append("<td>" + wuQu + "</td>"); //第六列
//            content.append("<tr>");
//            content.append("<td>" + "EQ错误机台共" + "</td>"); //第一列
//            content.append("<td>" + qiQueqError + "</td>"); //第二列
//            content.append("<td>" + LiuQueqError + "</td>"); //第三列
//            content.append("<td>" + SanQueqError + "</td>"); //第四列
//            content.append("<td>" + yiQueqError + "</td>"); //第五列
//            content.append("<td>" + wuQueqError + "</td>"); //第六列
//            content.append("</tr>");
            content.append("<tr>");
            content.append("<td>" + "Lot文件名错误机台共" + "</td>"); //第一列
            content.append("<td>" + qiQuLotError + "</td>"); //第二列
            content.append("<td>" + LiuQuLotError + "</td>"); //第三列
            content.append("<td>" + SanQuLotError + "</td>"); //第四列
            content.append("<td>" + yiQuLotError + "</td>"); //第五列
            content.append("<td>" + erQuLotError + "</td>"); //第六列
            content.append("</tr>");
            content.append("<tr style=\"background-color: #428BCA; color:#ffffff\"><th>厂</th><th>区</th><th>机台号</th><th>EQ码</th><th>Lot文件名</th><th>网络</th><th>是否生产</th><th>备注</th></tr>");
            for (String datas : jdbcData()) {
                String[] split = datas.split(",");
                String factory_new = split[0];
                String cob_new = split[1];
                String device_num_new = split[2];
                String eid_new = split[3];
                String lot_new = split[4];
                String is_network_new = split[5];
                String is_production_new = split[6];
                String remarks_new = split[7];
                content.append("<tr>");
                content.append("<td>" + factory_new + "</td>");
                content.append("<td>" + cob_new + "</td>");
                content.append("<td>" + device_num_new + "</td>");
                content.append("<td>" + eid_new + "</td>");

                if(!remarks_new.equals("Lot正确") && !remarks_new.equals("null")){
                    content.append("<td style=\"background-color: #EA0000; color:#ffffff\">" + lot_new + "</td>");
                }else{
                    content.append("<td>" + lot_new + "</td>");
                }

                if (is_network_new.equals("0")) {
                    is_network_new = "网络未连接";
                    is_production_new = "未知";

                    content.append("<td style=\"background-color: #EA0000; color:#ffffff\">" + is_network_new + "</td>");
                } else {
                    if (is_production_new.equals("0")) {
                        is_production_new = "未生产";
                    }else{
                        is_production_new = "已生产";
                    }
                    is_network_new = "网络已连接";
                    content.append("<td>" + is_network_new + "</td>");
                }

                content.append("<td>" + is_production_new + "</td>");

                if (!remarks_new.equals("Lot正确") && !remarks_new.equals("null") ) {
                    content.append("<td style=\"background-color: #EA0000; color:#ffffff\">" + remarks_new + "</td>");
                } else {
                    content.append("<td>" + remarks_new + "</td>");
                }

                content.append("</tr>");

            }

            content.append("</table>");
            content.append("<h3>end</h3>");
            content.append("</body></html>");
            File affix = new File("/data/workspace/project/file/采集软件安装文档及EQ错误纠正.pptx");
//            File affix = new File("./resources/采集软件安装文档及EQ错误纠正.pptx");

            if (!content.toString().isEmpty()) {
//                SendEMailWarning_bak2.sendMail(RECEIVE_EMAIL, "各厂区今日数采状况", content.toString(), affix);
            SendEMailWarning_bak2.sendMail(RECEIVE_MAIL_EQUIPMENT, "各厂区/昨日数采状况", content.toString(), affix);
            } else {
                System.out.println("昨天可能在换班，没有数据");
            }

        } catch (IndexOutOfBoundsException e) {
            System.out.println("Exception thrown  :" + e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //通过jdbc拉取数据
    public static List<String> jdbcData() {

        String time = Date_D();
        String createTime = time+" 08:00:00";
        String endTime = time+" 23:00:00";


        Connection conn = null;

        //创建集合，返货结果集
        List<String> resultList = new ArrayList<>();
        try {

            //指定连接类型
            Class.forName("com.cloudera.impala.jdbc41.Driver");
            //获取连接
            conn = DriverManager.getConnection("jdbc:impala://10.170.3.13:21050/ods_baseinfo;UseSasl=0;AuthMech=3;characterEncoding=utf-8", "qtkj", "qt_qt");

            try {

                //用statement 来执行sql语句
                Statement statement = conn.createStatement();

                //先查出网络OK部分数据，is_network = '1'，在join配置表里其他机台信息，拿到无网络机台的数据。
                //1代表有网络，0代表无网络
                String sql = "select * from(\n" +
                        "select b.area,b.cob,b.eid,b.device_num,uploadtime,\n" +
                        "case when is_network  is null then '0' else is_network end as is_network\n" +
                        ",case when is_production is null then '0' else is_production end as is_production,\n" +
                        "Lot from (\n" +
                        "select * from (\n" +
                        "select *,row_number()over(partition by eqid order by uploadtime desc)as num\n" +
                        "from aa_mes.dws_aa_is_nwtwork where is_network = '1' and uploadtime < '"+endTime+"' and uploadtime >= '"+createTime+"'\n" +
                        ") d where num = 1\n" +
//                        "and uploadtime < '"+createTime+"'\n" +
                        ")g right join aa_mes.device_mapping as b\n" +
                        "on b.eid = g.eqid )k where  device_num not like '%S' order by area,cob,eid;";

                //用于返回结果
                ResultSet rs = statement.executeQuery(sql);

                String area = null;
                String cob = null;
                String eid = null;
                String device_num = null;
                String is_network = null;
                String is_production = null;
                String Lot = null;
                String remarks = "null";

                //遍历结果集
                while (rs.next()) {

                    if (rs.getString("area") == null || rs.getString("area").isEmpty()) {
                        area = "null";
                    } else {
                        area = rs.getString("area");
                    }

                    if (rs.getString("cob") == null || rs.getString("cob").isEmpty()) {
                        cob = "null";
                    } else {
                        cob = rs.getString("cob");
                    }

                    if (rs.getString("eid") == null || rs.getString("eid").isEmpty()) {
                        eid = "null";
                    } else {
                        eid = rs.getString("eid");
                    }

                    if (rs.getString("device_num") == null || rs.getString("device_num").isEmpty()) {
                        device_num = "null";
                    } else {
                        device_num = rs.getString("device_num");
                    }

                    if (rs.getString("is_network") == null || rs.getString("is_network").isEmpty()) {
                        is_network = "null";
                    } else {
                        is_network = rs.getString("is_network");
                    }

                    if (rs.getString("is_production") == null || rs.getString("is_production").isEmpty()) {
                        is_production = "null";
                    } else {
                        is_production = rs.getString("is_production");
                    }

                    if (rs.getString("Lot") == null || rs.getString("Lot").isEmpty()) {
                        Lot = "null";
                    } else {
                        Lot = rs.getString("Lot");
                    }
                    //将表里代码转换成汉字，以便于邮件端展示
                    if (area.startsWith("GT")) {
                        area = "古二";
                    } else if (area.startsWith("GC")) {
                        area = "古一";
                    } else {
                        area = "台虹";
                    }

                    //当Lot不为空时判断Lot规则。
                    remarks = lotResule(Lot, area, Integer.parseInt(is_network), Integer.parseInt(is_production));
                    //当备注Lot错误时 or is_network为0时添加数据
                    if (remarks.equals("Lot错误") || is_network.equals("0")) {
                        resultList.add(area + "," + cob + "," + device_num + "," + eid + "," + Lot + "," + is_network + "," + is_production + "," + remarks);
                    }
                }

                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("数据库数据读取成功！" + "\n");
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return resultList;
    }


    //查看Lot文件夹是否符合规则
    public static String lotResule(String lot, String area, int is_network, int is_production) {
        String[] dataSplit = lot.split("-");
        if (area.startsWith("古二") || area.startsWith("古一")) {
            if (!lot.equals("null") && (!dataSplit[0].matches("-?[0-9]+.?[0-9]*") || !dataSplit[1].matches("-?[0-9]+.?[0-9]*") || lot.contains("--") || lot.contains(" ") || lot.contains("#") || dataSplit.length > 8 || dataSplit.length < 6)) {
                lot = "Lot错误";
            } else if (is_network == 1 && is_production == 1) {
                lot = "Lot正确";
            }
        } else if (area.startsWith("台虹")) {
            if (!lot.equals("null") && (!dataSplit[0].matches("-?[0-9]+.?[0-9]*") || lot.contains("--") || lot.contains(" ") || lot.contains("#") || dataSplit.length > 7 || dataSplit.length < 5)) {
                lot = "Lot错误";
            } else if (is_network == 1 && is_production == 1) {
                lot = "Lot正确";
            }
        }

        return lot;
    }

    public static String Date_D(){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,-1) ;
        String yesterday = new SimpleDateFormat( "yyyy-MM-dd").format(cal.getTime());

        return yesterday;
    }


}
