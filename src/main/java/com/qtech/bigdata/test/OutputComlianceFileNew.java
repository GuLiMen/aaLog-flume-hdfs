package com.qtech.bigdata.test;

import com.qtech.bigdata.comm.SendEMailWarning_bak1;
import com.qtech.bigdata.util.FileSystemManager;
import com.qtech.bigdata.util.testDate;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;

public class OutputComlianceFileNew {

    public static void main(String[] args) throws IOException {

        InfoSum();
    }

    //拉取今日各厂机台上传数据信息
    public static String OutputComlianceFile_mail_new() throws IOException {

        //获取文件配置信息
        FileSystem fileSystem = FileSystemManager.getFileSystem();

        //创建StringBuilder对象，添加各厂机台信息
        StringBuilder buf = new StringBuilder();

        //厂
        for (FileStatus Factory : fileSystem.listStatus(new Path("/flume"))) {

            //区
            for (FileStatus cob : fileSystem.listStatus(new Path(Factory.getPath().toString()))) {

                //唯一码
                for (FileStatus EQcode : fileSystem.listStatus(new Path(cob.getPath().toString()))) {

                    //Lot
                    for (FileStatus Lot : fileSystem.listStatus(new Path(EQcode.getPath().toString()))) {

                        //Lot文件名
                        for (FileStatus result : fileSystem.listStatus(new Path(Lot.getPath().toString()))) {

                            //获取Lot文件名
                            String dir = result.getPath().getName();

                            try{
                                //定义厂，区，唯一码
                                String factory_a = Factory.getPath().getName();
                                String eid_a = EQcode.getPath().getName();
                                String region;
                                String machine = cob.getPath().getName();
                                //得到今天的Lot文件名，对其进行切割
                                if(dir.contains(testDate.DateMonthDay())){
                                    if (factory_a.equalsIgnoreCase("GuCheng")){
                                        //因hdfs里是英文，转换成中文
                                        factory_a = factory_a.replace("GuCheng", "古城");
                                        //按照dir目录切割，如1-2-C9MA02-5-12-D。因三厂规则不一样，故分开写
                                        String[] split = dir.split("-");
                                        //得到线体-机台号
                                        region = split[0]+"-"+split[1];

                                        buf.append(factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + result.getPath().getName());
                                        buf.append("\r\n");
                                    }else if(factory_a.equalsIgnoreCase("TaiHong")){
                                        factory_a = factory_a.replace("TaiHong", "台虹");
                                        //按照dir目录切割，如1-C0LA24-2-20-D。因三厂规则不一样，故分开写
                                        String[] split = dir.split("-");
                                        //得到线体-机台号
                                        region = split[0];

                                        buf.append(factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + result.getPath().getName());
                                        buf.append("\r\n");
                                    }else{
                                        factory_a = factory_a.replace("ChengBei", "汉浦");
                                        //按照dir目录切割，如1-C0LA24-2-20-D。因三厂规则不一样，故分开写
                                        String[] split = dir.split("-");
                                        //得到线体-机台号
                                        region = split[0];

                                        buf.append(factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + result.getPath().getName());
                                        buf.append("\r\n");
                                    }
                                }

                            }catch (IndexOutOfBoundsException e) {
                                System.out.println("Exception thrown  :" + e);
                            }



                        }
                    }
                }
            }
        }
        //关闭hdfs资源
        fileSystem.close();
        //返回buf
        return buf.toString();
    }

    public static void  InfoSum() throws IOException {
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
        String[] line = OutputComlianceFile_mail_new().split("\r\n");

        //打印字符串
        for (String data : line) {
            //按照点切割,得到例如{汉浦、COB5、1、EQ01000003300058、1-C0LA24-5-12----D}
            String[] split = data.split("、");

            //因三厂规则
            if (split[1].equalsIgnoreCase("cob1")) {

                yiQu++;

                //判断EQ是否错误
                if (split[3].length() > 16) {
                    //如果错误就加1
                    yiQueqError++;
                    //按照Lot文件名切割，查看Lot文件名是否错误
                    String[] split1 = split[4].split("-");

                    //判断Lot文件名是否错误
                    if (split1[0].length() > 2 | !split1[1].matches("-?[0-9]+.?[0-9]*") | !split1[2].matches("-?[0-9]+.?[0-9]*") | split[4].contains("--")) {
                        yiQuLotError++;
                    }
                }
                //若EQ无错误
                else {
                    String[] split1 = split[4].split("-");
                    if (split1[0].length() > 4 | !split1[1].matches("-?[0-9]+.?[0-9]*") | !split1[2].matches("-?[0-9]+.?[0-9]*") | split[4].contains("--")) {
                        yiQuLotError++;

//                        if (yiQuLotError == 1){
//                            System.out.println(split1[4]);
//                        }
                    }
                }
                //判断除古城外，其他两厂
            }else if (split[1].equalsIgnoreCase("cob5")) {

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
        content.append("<td>" + "今日上传数据机台共" + "</td>"); //第一列
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

        //加深颜色
        for (String data : line) {

            //汉浦、COB5、1、EQ01000003300058、1-C0LA24-5-12-D
            String[] split = data.split("、");
            if (split[0].equalsIgnoreCase("古城")){

                if (split[3].length() > 16) {

                    String s = "EQ码不正确";
                    String[] split1 = split[4].split("-");
                    String pattern = "([\\-])\\1";

                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(split[4]);
                    if (split1[0].length() > 4 | !split1[1].matches("-?[0-9]+.?[0-9]*") | split1[2].matches("-?[0-9]+.?[0-9]*") | split[4].contains("--") | m.matches() == true) {
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

//                    Pattern pattern;
//                    pattern = compile("([\\-])\\1{1}");
                    String pattern = "([\\-])\\1";

                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(split[4]);

                    if (split1[0].length() > 4 || !split1[1].matches("-?[0-9]+.?[0-9]*") || split1[2].matches("-?[0-9]+.?[0-9]*") ||  m.matches() == true) {
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
            }else {
                if (split[3].length() > 16) {

                    String s = "EQ码不正确";
                    String[] split1 = split[4].split("-");
                    String pattern = "([\\-])\\1";

                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(split[4]);
                    if (split1[0].length() > 4|| split1[1].matches("-?[0-9]+.?[0-9]*") ||  m.matches() == true) {
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
                    String pattern = "([\\-])\\1";

                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(split[4]);
                    if (split1[0].length() > 4|| split1[1].matches("-?[0-9]+.?[0-9]*") ||  m.matches() == true) {
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
            }
        }

        content.append("</table>");
        content.append("<h3>description</h3>");
        content.append("</body></html>");

//        File affix = new File("/data/workspace/project/file/采集软件安装文档及EQ错误纠正.pptx");

        File affix = new File("./resources/采集软件安装文档及EQ错误纠正.pptx");

//        SendEMailWarning_bak1.sendMail(RECEIVE_EMAIL, "各厂区今日数采状况", content.toString(), affix);
    }


}
