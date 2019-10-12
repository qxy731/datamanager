package com.myboot.dataprocess.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommonTool {
	
	/**
	 * 计算两个字符串日期之间的天数差
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public static Integer betweenDay(String startDate, String endDate) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");// 自定义时间格式
        Calendar calendar_a = Calendar.getInstance();// 获取日历对象
        Calendar calendar_b = Calendar.getInstance();
        Date date_a = null;
        Date date_b = null;
        try {
            date_a = simpleDateFormat.parse(startDate);//字符串转Date
            date_b = simpleDateFormat.parse(endDate);
            calendar_a.setTime(date_a);// 设置日历
            calendar_b.setTime(date_b);
        } catch (ParseException e) {
            e.printStackTrace();//格式化异常
        }
        long time_a = calendar_a.getTimeInMillis();
        long time_b = calendar_b.getTimeInMillis();
        Long between_days = (time_b - time_a) / (1000 * 3600 * 24);//计算相差天数
        return between_days.intValue();
    }
	
	/**
	 * 字符串日期加1天
	 * @param startDate
	 * @return
	 */
	public static String addDay(String startDate,int days) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");// 自定义时间格式
        Calendar calendar = Calendar.getInstance();// 获取日历对象
        Date date = null;
        try {
            date = simpleDateFormat.parse(startDate);//字符串转Date
            calendar.setTime(date);// 设置日历
            calendar.add(Calendar.DAY_OF_YEAR, days);
        } catch (ParseException e) {
            e.printStackTrace();//格式化异常
        }
        String y = calendar.get(Calendar.YEAR)+"";
        int month = calendar.get(Calendar.MONTH)+1;
        String m = String.valueOf(month);
        if(month < 10) {
        	m = "0" + m;
        }
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        String d = String.valueOf(day);
        if(day < 10) {
        	d = "0" + d;
        }
        String ret = y + "-" + m + "-" + d;
        return ret;
    }
	
	public static String getCurrentDate() {
		 Calendar calendar = Calendar.getInstance();// 获取日历对象
		 String y = calendar.get(Calendar.YEAR)+"";
	        int month = calendar.get(Calendar.MONTH)+1;
	        String m = String.valueOf(month);
	        if(month < 10) {
	        	m = "0" + m;
	        }
	        int day = calendar.get(Calendar.DAY_OF_MONTH);
	        String d = String.valueOf(day);
	        if(day < 10) {
	        	d = "0" + d;
	        }
	        String ret = y + "-" + m + "-" + d;
	        return ret;
	}
	
	public static String getCurrentTime() {
		Calendar calendar = Calendar.getInstance();// 获取日历对象
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		String h = String.valueOf(hour);
        if(hour < 10) {
        	h = "0" + h;
        }
        int month = calendar.get(Calendar.MINUTE);
        String m = String.valueOf(month);
        if(month < 10) {
        	m = "0" + m;
        }
        int second = calendar.get(Calendar.SECOND);
        String s = String.valueOf(second);
        if(second < 10) {
        	s = "0" + s;
        }
        String ret = h +":"+ m +":"+ s;
        return ret;
	}
	
	public static String getBirthByCertNo(String certNo) {
		String birthday = certNo.substring(6, 10) + "-"
                 + certNo.substring(10, 12) + "-"
                 + certNo.substring(12, 14);
		return birthday;
	}
	
	public static void main(String[] args) {
		System.out.println(getBirthByCertNo("370826198410252516"));
		log.info("");
	}

}
