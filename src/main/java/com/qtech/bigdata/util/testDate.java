package com.qtech.bigdata.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class testDate {

    public static void main(String[] args) {
        System.out.println(Date_D());
    }

    public static String Date_D(){
        Date dNow = new Date( );
        String yesterday = new SimpleDateFormat( "MM-dd").format(dNow);
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

    public static String DateMonthDay() {

//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM-dd");
//        Date date = new Date();
//        String dateStr = simpleDateFormat.format(date);
//        String strDate = dateStr.substring(1);
//        return strDate;

        Calendar cal = Calendar.getInstance();
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DATE);
        return month+"-"+day+"-D";

    }

    public static String DateMonthDay1() {

//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM.dd");
//        Date date = new Date();
//        String dateStr = simpleDateFormat.format(date);
//        String strDate = dateStr.substring(1);
//        return strDate;

        Calendar cal = Calendar.getInstance();
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DATE);
        return month+"."+day+"-D";

    }


    public static String noDataEQ() {
        Calendar cal = Calendar.getInstance();
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DATE);
        return month+"-"+day+"-D";

    }

//    @Test
//    public String DateMonthDayError() {
//        Calendar cal = Calendar.getInstance();
//        int month = cal.get(Calendar.MONTH) + 1;
//        int day = cal.get(Calendar.DATE);
//        return month+"."+day;
//
//    }

//    @Test
//    public void test(){
//        Calendar cal = Calendar.getInstance();
//        int month = cal.get(Calendar.MONTH) + 1;
//        int day = cal.get(Calendar.DATE);
//        System.out.println(month+"-"+day+"-D");
//    }

}