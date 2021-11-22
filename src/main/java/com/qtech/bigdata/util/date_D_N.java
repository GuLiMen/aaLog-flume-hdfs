package com.qtech.bigdata.util;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class date_D_N {

    @Test
    public static String Date_D(){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,-1) ;
        String yesterday = new SimpleDateFormat( "MM-dd").format(cal.getTime());
        String[] split = yesterday.split("-");

        int month = Integer.parseInt(split[0]);
        int date = Integer.parseInt(split[1]);

        String monthNew = "";
        String dateNew = "";

        if(month < 10){
            monthNew = Integer.toString(month).replace("0","");
        }else{
            monthNew = Integer.toString(month);
        }
        if(date < 10){
            dateNew = Integer.toString(date).replace("0","");
        }else{
            dateNew = Integer.toString(date);
        }
        return monthNew+"-"+dateNew+"-D";
    }

    @Test
    public static String Date_N(){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,-1) ;
        String yesterday = new SimpleDateFormat( "MM-dd").format(cal.getTime());
        String[] split = yesterday.split("-");

        int month = Integer.parseInt(split[0]);
        int date = Integer.parseInt(split[1]);

        String monthNew = "";
        String dateNew = "";

        if(month < 10){
            monthNew = Integer.toString(month).replace("0","");
        }else{
            monthNew = Integer.toString(month);
        }
        if(date < 10){
            dateNew = Integer.toString(date).replace("0","");
        }else{
            dateNew = Integer.toString(date);
        }
        return monthNew+"-"+dateNew+"-N";
    }



}
