package com.qtech.bigdata.test;

import com.qtech.bigdata.comm.SendEMailWarning_bak1;
import com.qtech.bigdata.util.FileSystemManager;
import com.qtech.bigdata.util.testDate;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

import static com.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;

public class OutputComlianceFile {

    public static void main(String[] args) throws IOException {

        InfoSum();

    }

    //拉取今日各厂机台上传数据信息
    public static String OutputComlianceFile_mail() throws IOException {

        //获取文件配置信息
        FileSystem fileSystem = FileSystemManager.getFileSystem();

        //创建StringBuilder对象，添加各厂机台信息
        StringBuilder buf = new StringBuilder();

        //指定目录
        for (FileStatus Factory : fileSystem.listStatus(new Path("/flume"))) {

            for (FileStatus cob : fileSystem.listStatus(new Path(Factory.getPath().toString()))) {

                for (FileStatus EQcode : fileSystem.listStatus(new Path(cob.getPath().toString()))) {

                    for (FileStatus Lot : fileSystem.listStatus(new Path(EQcode.getPath().toString()))) {

                        for (FileStatus result : fileSystem.listStatus(new Path(Lot.getPath().toString()))) {

                            //获取Lot文件名
                            String dir = result.getPath().toString();

                            //按照-切割
                            String[] split2 = dir.split("-");

                            String[] split3 = dir.split("/");

                            String[] split4 = split3[8].split("-");

                            //判断Lot第一位是否数字，若不是则Lot文件名异常
                            if (!split4[0].matches("-?[0-9]+.?[0-9]*")) {
                                try {

                                    //截取月与日

                                    //Lot文件名可能不规范
                                    String errorMonth = split2[1];
                                    String errorDays = split2[2];
                                    //拼接月日，因月日中间可能是.或者-
                                    String datesError = errorMonth + "-" + errorDays;
                                    String date1sError = errorMonth + "." + errorDays;

                                    //判断Lot文件名是否符合今日数据规则
                                    if (dir.contains(testDate.DateMonthDay()) && (datesError.equalsIgnoreCase(testDate.DateMonthDay()))) {

                                        //判断Lot文件名不包含1+月与日
                                        //如12-12、2-12.
                                        if (!dir.contains("1" + testDate.DateMonthDay())) {
                                            //按照斜杠切割，
                                            String[] split = dir.split("/");
                                            //得到厂
                                            String factory = split[4];
                                            //得到区
                                            String region = split[5];

                                            //得到机台号
                                            String machine = split[8];

                                            if (factory.equals("GuCheng")) {
                                                //因hdfs里是英文，转换成中文
                                                String factory_test = factory.replace("GuCheng", "古城");
                                                //按照-切割，得到线-机台号
                                                String[] split1 = machine.split("-");

                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "-" + split1[1] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");
                                            } else if (factory.equals("TaiHong")) {
                                                String factory_test = factory.replace("TaiHong", "台虹");
                                                String[] split1 = machine.split("-");
                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            } else {
                                                String factory_test = factory.replace("ChengBei", "汉浦");
                                                String[] split1 = machine.split("-");
                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            }
                                        }
                                    } else if (dir.contains(testDate.DateMonthDay1()) && (date1sError.equalsIgnoreCase(testDate.DateMonthDay()))) {

                                        if (!dir.contains("1" + testDate.DateMonthDay1())) {
                                            String[] split = dir.split("/");
                                            String factory = split[4];
                                            String region = split[5];
                                            String machine = split[8];

                                            if (factory.equals("GuCheng")) {
                                                String factory_test = factory.replace("GuCheng", "古城");
                                                String[] split1 = machine.split("-");
                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "-" + split1[1] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            } else if (factory.equals("TaiHong")) {
                                                String factory_test = factory.replace("TaiHong", "台虹");
                                                String[] split1 = machine.split("-");
                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            } else {
                                                String factory_test = factory.replace("ChengBei", "汉浦");
                                                String[] split1 = machine.split("-");

                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            }
                                        }
                                    }
                                } catch (IndexOutOfBoundsException e) {
                                    System.out.println("Exception thrown  :" + e);
                                }
                            } else {
                                try {
                                    //因古城多一位，故取3，4
                                    String months = split2[3];
                                    String days = split2[4];

                                    String dates = months + "-" + days;
                                    String date1s = months + "." + days;


                                    //台虹汉普取2，3
                                    String month = split2[2];
                                    String day = split2[3];

                                    String date = month + "-" + day;
                                    String date1 = month + "." + day;

                                    //判断Lot文件名是否符合今日数据规则
                                    if (dir.contains(testDate.DateMonthDay()) && (date.equalsIgnoreCase(testDate.DateMonthDay()) || dates.equalsIgnoreCase(testDate.DateMonthDay()))) {

                                        //判断Lot文件名不包含1+月与日
                                        //如12-12、2-12.
                                        if (!dir.contains("1" + testDate.DateMonthDay())) {
                                            //按照斜杠切割，
                                            String[] split = dir.split("/");
                                            //得到厂
                                            String factory = split[4];
                                            //得到区
                                            String region = split[5];

                                            //得到机台号
                                            String machine = split[8];

                                            if (factory.equals("GuCheng")) {
                                                String factory_test = factory.replace("GuCheng", "古城");
                                                //按照-切割，得到线-机台号
                                                String[] split1 = machine.split("-");


                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "-" + split1[1] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");
                                            } else if (factory.equals("TaiHong")) {
                                                String factory_test = factory.replace("TaiHong", "台虹");
                                                String[] split1 = machine.split("-");
                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            } else {
                                                String factory_test = factory.replace("ChengBei", "汉浦");
                                                String[] split1 = machine.split("-");
                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            }
                                        }
                                    } else if (dir.contains(testDate.DateMonthDay1()) && (date1.equalsIgnoreCase(testDate.DateMonthDay()) || date1s.equalsIgnoreCase(testDate.DateMonthDay()))) {

                                        if (!dir.contains("1" + testDate.DateMonthDay1())) {
                                            String[] split = dir.split("/");
                                            String factory = split[4];
                                            String region = split[5];
                                            String machine = split[8];

                                            if (factory.equals("GuCheng")) {
                                                String factory_test = factory.replace("GuCheng", "古城");
                                                String[] split1 = machine.split("-");
                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "-" + split1[1] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            } else if (factory.equals("TaiHong")) {
                                                String factory_test = factory.replace("TaiHong", "台虹");
                                                String[] split1 = machine.split("-");
                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            } else {
                                                String factory_test = factory.replace("ChengBei", "汉浦");
                                                String[] split1 = machine.split("-");

                                                buf.append(factory_test + "、" + region + "、" + split1[0] + "、" + EQcode.getPath().getName() + "、" + result.getPath().getName());
                                                buf.append("\r\n");

                                            }
                                        }
                                    }
                                } catch (ArrayIndexOutOfBoundsException e) {
                                    System.out.println(e+"、"+result.getPath().getName());
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

        int wuQuLotError = 0;
        int wuQueqError = 0;


        //按照换行切割
        String[] line = OutputComlianceFile_mail().split("\r\n");
//            System.out.println(OutputComlianceFile_mail());

        //打印字符串
        for (String data : line) {
            //按照点切割
            String[] split = data.split("、");

            //判断是否相等
            if (split[1].equalsIgnoreCase("cob1")) {

                yiQu++;

                //判断EQ是否错误
                if (split[3].length() > 16) {
                    yiQueqError++;
                    String[] split1 = split[4].split("-");

                    System.out.println(split1[1]);

                    //判断Lot文件名是否错误
                    if (split1[0].length() > 4 && !split1[1].matches("-?[0-9]+.?[0-9]*") && split1[2].matches("-?[0-9]+.?[0-9]*")) {
                        yiQuLotError++;
                    }
                } else {
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4 && !split1[1].matches("-?[0-9]+.?[0-9]*") && split1[2].matches("-?[0-9]+.?[0-9]*")) {
                        yiQuLotError++;
                    }
                }
            } else if (split[1].equalsIgnoreCase("cob5")) {

                wuQu++;

                if (split[3].length() > 16) {
                    wuQueqError++;
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4) {
                        wuQuLotError++;
                    }
                } else {
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4) {
                        wuQuLotError++;
                    }
                }
            } else if (split[1].equalsIgnoreCase("cob3")) {

                SanQu++;
                if (split[3].length() > 16) {
                    SanQueqError++;
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4) {
                        SanQuLotError++;
                    }
                } else {
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4) {
                        SanQuLotError++;
                    }
                }
            } else if (split[1].equalsIgnoreCase("cob6")) {

                LiuQu++;
                if (split[3].length() > 16) {
                    LiuQueqError++;
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4) {
                        LiuQuLotError++;
                    }
                } else {
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4) {
                        LiuQuLotError++;
                    }
                }
            } else if (split[1].equalsIgnoreCase("cob7")) {
                qiQu++;
                if (split[3].length() > 16) {
                    qiQueqError++;
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4) {
                        qiQuLotError++;
                    }
                } else {
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4) {
                        qiQuLotError++;
                    }
                }
            }
        }


        StringBuilder content = new StringBuilder("<html><head></head><body><h2>各厂区今日数采状况</h2>");
        content.append("<table border=\"5\" style=\"border:solid 1px #E8F2F9;font-size=14px;;font-size:18px;\">");
        content.append("<tr style=\"background-color: #428BCA; color:#ffffff\"><th>各厂区机台上传汇总</th><th>台虹七区</th><th>台虹六区</th><th>台虹三区</th><th>古城一区</th><th>汉浦五区</th></tr>");

        content.append("</tr>");
        content.append("<tr>");
        content.append("<td>" + "各厂区机台总数" + "</td>"); //第一列
        content.append("<td>" + "38" + "</td>"); //第二列
        content.append("<td>" + "16" + "</td>"); //第三列
        content.append("<td>" + "10" + "</td>"); //第四列
        content.append("<td>" + "61" + "</td>"); //第五列
        content.append("<td>" + "16" + "</td>"); //第六列
        content.append("</tr>");

        content.append("<tr>");
        content.append("<td>" + "昨日上传数据机台共" + "</td>"); //第一列
        content.append("<td>" + qiQu + "</td>"); //第二列
        content.append("<td>" + LiuQu + "</td>"); //第三列
        content.append("<td>" + SanQu + "</td>"); //第四列
        content.append("<td>" + yiQu + "</td>"); //第五列
        content.append("<td>" + wuQu + "</td>"); //第六列

        content.append("<tr>");
        content.append("<td>" + "EQ错误机台共" + "</td>"); //第一列
        content.append("<td>" + qiQueqError + "</td>"); //第二列
        content.append("<td>" + LiuQueqError + "</td>"); //第三列
        content.append("<td>" + SanQueqError + "</td>"); //第四列
        content.append("<td>" + yiQueqError + "</td>"); //第五列
        content.append("<td>" + wuQueqError + "</td>"); //第六列
        content.append("</tr>");
        content.append("<tr>");
        content.append("<td>" + "Lot文件名错误机台共" + "</td>"); //第一列
        content.append("<td>" + qiQuLotError + "</td>"); //第二列
        content.append("<td>" + LiuQuLotError + "</td>"); //第三列
        content.append("<td>" + SanQuLotError + "</td>"); //第四列
        content.append("<td>" + yiQuLotError + "</td>"); //第五列
        content.append("<td>" + wuQuLotError + "</td>"); //第六列
        content.append("</tr>");


        content.append("<tr style=\"background-color: #428BCA; color:#ffffff\"><th>厂</th><th>区</th><th>机台号</th><th>EQ码</th><th>Lot文件名</th><th>EQ码是否正确</th><th>Lot文件名是否正确</th></tr>");

        for (String data : line) {

            String[] split = data.split("、");

            if (split[3].length() > 16) {

                String s = "EQ码不正确";
                String[] split1 = split[4].split("-");
                if (split1[0].length() > 4) {
                    content.append("<tr>");
                    content.append("<td>" + split[0] + "</td>"); //第一列
                    content.append("<td>" + split[1] + "</td>"); //第二列
                    content.append("<td>" + split[2] + "</td>"); //第三列
                    content.append("<td>" + split[3] + "</td>"); //第四列
                    content.append("<td>" + split[4] + "</td>"); //第五列
                    content.append("<td>" + s + "</td>");        //第六列
                    content.append("<td style=\"background-color: #EA0000; color:#ffffff\">" + "Lot文件名不正确" + "</td>");        //第七列
                    content.append("</tr>");
                } else {
                    content.append("<tr>");
                    content.append("<td>" + split[0] + "</td>"); //第一列
                    content.append("<td>" + split[1] + "</td>"); //第二列
                    content.append("<td>" + split[2] + "</td>"); //第三列
                    content.append("<td>" + split[3] + "</td>"); //第四列
                    content.append("<td>" + split[4] + "</td>"); //第五列
                    content.append("<td style=\"background-color: #EA0000; color:#ffffff\">" + s + "</td>");        //第六列
                    content.append("<td>" + "Lot文件名正确" + "</td>");        //第七列
                    content.append("</tr>");
                }

            } else {
                String s = "EQ码正确";
                String[] split1 = split[4].split("-");
                if (split1[0].length() > 4) {
                    content.append("<tr>");
                    content.append("<td>" + split[0] + "</td>"); //第一列
                    content.append("<td>" + split[1] + "</td>"); //第二列
                    content.append("<td>" + split[2] + "</td>"); //第三列
                    content.append("<td>" + split[3] + "</td>"); //第四列
                    content.append("<td>" + split[4] + "</td>"); //第五列
                    content.append("<td>" + s + "</td>");        //第六列
                    content.append("<td style=\"background-color: #EA0000; color:#ffffff\">" + "Lot文件名不正确" + "</td>");        //第七列
                    content.append("</tr>");
                } else {
                    content.append("<tr>");
                    content.append("<td>" + split[0] + "</td>"); //第一列
                    content.append("<td>" + split[1] + "</td>"); //第二列
                    content.append("<td>" + split[2] + "</td>"); //第三列
                    content.append("<td>" + split[3] + "</td>"); //第四列
                    content.append("<td>" + split[4] + "</td>"); //第五列
                    content.append("<td>" + s + "</td>");        //第六列
                    content.append("<td>" + "Lot文件名正确" + "</td>");        //第七列
                    content.append("</tr>");
                }
            }
//            System.out.println(split[0]);

        }

        content.append("</table>");
        content.append("<h3>description</h3>");
        content.append("</body></html>");

//        File affix = new File("/data/workspace/project/file/采集软件安装文档及EQ错误纠正.pptx");

        File affix = new File("./resources/采集软件安装文档及EQ错误纠正.pptx");
//        SendEMailWarning_bak1.sendMail(RECEIVE_EMAIL, "各厂区今日数采状况", content.toString(), affix);
    }

}
