package com.qtech.bigdata.start.increase;

import com.qtech.bigdata.util.FileSystemManager;
import com.qtech.bigdata.util.date_D_N;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

import static com.qtech.bigdata.util.Factor.areaNew;
import static com.qtech.bigdata.util.Factor.cobNew;

//数采状况邮件数据准备
public class OutputComlianceFile_mail_new {

    public static List<String> aaList_nd() throws IOException {

        //获取文件配置信息
        FileSystem fileSystem = FileSystemManager.getFileSystem();

        //创建StringBuilder对象，添加各厂机台信息
        List<String> info = new ArrayList<>();

        Set<String> set = new HashSet<String>();
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

                            try {
                                //定义厂，区，唯一码
                                String factory_a = Factory.getPath().getName();
                                String eid_a = EQcode.getPath().getName();
                                String region;
                                String machine = cob.getPath().getName();
                                //得到今天的Lot文件名，对其进行切割
                                if (dir.contains(date_D_N.Date_D()) | dir.contains(date_D_N.Date_N())) {
                                    if (factory_a.equalsIgnoreCase("GuCheng")) {
                                        //因hdfs里是英文，转换成中文
                                        factory_a = factory_a.replace("GuCheng", "古城");
                                        //按照dir目录切割，如1-2-C9MA02-5-12-D。因三厂规则不一样，故分开写
                                        String[] split = dir.split("-");
                                        //得到线体-机台号
                                        region = split[0] + "-" + split[1];

                                            if (set.add(eid_a)) {
                                                info.add(factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + result.getPath().getName());
                                            }

//                                        buf.append(factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + result.getPath().getName());
//                                        buf.append("\r\n");
                                    } else if (factory_a.equalsIgnoreCase("TaiHong")) {
                                        factory_a = factory_a.replace("TaiHong", "台虹");
                                        //按照dir目录切割，如1-C0LA24-2-20-D。因三厂规则不一样，故分开写
                                        String[] split = dir.split("-");
                                        //得到线体-机台号
                                        region = split[0];
                                        if(set.add(eid_a)){
                                            info.add(factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + result.getPath().getName());
                                        }
//                                        buf.append(factory_a + "、" + machine + "、" + region + "、" + eid_a + "、" + result.getPath().getName());
//                                        buf.append("\r\n");
                                    } else if (factory_a.equalsIgnoreCase("GuCheng2")){
                                        factory_a = factory_a.replace("GuCheng2", "古二");
                                        //按照dir目录切割，如1-C0LA24-2-20-D。因三厂规则不一样，故分开写
                                        String[] split = dir.split("-");
                                        //得到线体-机台号
                                        //得到线体-机台号
                                        region = split[0] + "-" + split[1];
                                        if(set.add(eid_a)){
                                            info.add(areaNew(factory_a) + "、" + cobNew(machine) + "、" + region + "、" + eid_a + "、" + result.getPath().getName());
                                        }
                                    }
                                }
                            } catch (IndexOutOfBoundsException e) {
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
        return info;
    }
}
