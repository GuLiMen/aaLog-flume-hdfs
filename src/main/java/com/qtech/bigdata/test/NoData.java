package com.qtech.bigdata.test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.qtech.bigdata.comm.SendEMailWarning_bak1;
import com.qtech.bigdata.util.FileSystemManager;
import com.qtech.bigdata.util.testDate;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.qtech.bigdata.util.testDate.noDataEQ;

public class NoData {

    public static void main(String[] args) throws IOException {
        NoDataEQ();

    }

    public static void NoDataEQ() {

        Set<String> EQ = new HashSet<>();


        FileSystem fileSystem = FileSystemManager.getFileSystem();
        try {
            for (FileStatus Factory : fileSystem.listStatus(new Path("/flume"))) {

                for (FileStatus cob : fileSystem.listStatus(new Path(Factory.getPath().toString()))) {

                    for (FileStatus EQcode : fileSystem.listStatus(new Path(cob.getPath().toString()))) {

                        for (FileStatus Lot : fileSystem.listStatus(new Path(EQcode.getPath().toString()))) {

                            for (FileStatus result : fileSystem.listStatus(new Path(Lot.getPath().toString()))) {

                                String dir = result.getPath().getName();

                                String[] factory_GuCheng = dir.split("-");

                                if (Factory.getPath().getName().equalsIgnoreCase("GuCheng")) {
                                    if (dir.contains(noDataEQ())) {
                                        EQ.add(EQcode.getPath().getName());
                                    }
                                }

                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String s : EQ) {
            System.out.println(s);
        }
    }
}
