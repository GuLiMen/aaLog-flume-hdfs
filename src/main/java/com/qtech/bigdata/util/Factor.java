package com.qtech.bigdata.util;

public class Factor {
    public static String areaNew(String area){
        String areanew = "";
        if(area.equals("汉浦")){
            areanew  = "古城";
        }else{
            areanew = area;
        }
        return areanew;
    }

    public static String cobNew(String cob){
        String cobnew= "";
        if(cob.equals("COB5")){
            cobnew  = "COB2";
        }else{
            cobnew =cob;
        }
        return cobnew;
    }
}
