package com.myboot.dataprocess.common;

public class MyConstants {
	
    //传入akka的时间戳字段（必传）当时的系统时间	
    public final static String FLAT_TRAD_DATE_TIME_NAME = "FLAT_TRAD_DATE_TIME";
    
    public final static String FLAT_TRAD_DATE_TIME_DEFAULT = "2019-10-13 00:00:00";
    
    public final static String MY_APP_DATA_TYPE_NAME = "MyAppDataType";
    
    public final static String MY_APP_DATA_TYPE_DEFAULT = "1";
    
    /**1-历史；2-实时*/
    public final static String MY_DATA_FLAG_NAME = "MyDataFlag";
    
    public final static String MY_DATA_FLAG_HIS = "1";
    
    public final static String MY_DATA_FLAG_REAL = "2";
    
    public final static String MY_START_DATE_NAME = "MyStartDate";
    
    public final static String MY_START_DATE_DEFAULT = "20190313";
    
    public final static String MY_END_DATE_NAME = "MyEndDate";
    
    public final static String MY_END_DATE_DEFAULT = "20190929";
    
    public final static String MY_APPLICATION_DATE_NAME = "ApplicationDate";
    		
    public final static String MY_APPLICATION_DATE_DEFAULT = "2019-10-13";
    
    /**AKKAHTTP,HTTPCLIENT*/
   //public final static String HTTP_TYPE = "AKKAHTTP";
   public final static String HTTP_TYPE = "HTTPCLIENT";
	
    
    //public final static String HTTP_SERVER_IP = "127.0.0.1";
    public final static int HTTP_SERVER_PORT = 13551;
    //public final static int HTTP_SERVER_PORT = 8090;
    
    //public final static String HTTP_SERVER_IP = "182.180.115.236"; 
    public final static String HTTP_SERVER_IP = "182.180.183.12"; 
    public final static String HTTP_SERVER_PROJECT = "";
    //public final static String HTTP_SERVER_PROJECT = "/datamanager";
    public final static String HTTP_SERVER_METHOD = "/call";
    
    /**
     * HTTPCLIENT:"http://182.180.115.236:13551/call"
     * HTTPCLIENT:"http://127.0.0.1:8090/datamanager/call"
     * AKKAHTTP:"http://127.0.0.1:13551/datamanager/call"
     */
    public final static String HTTP_URL = "http://"+HTTP_SERVER_IP+":"+HTTP_SERVER_PORT+HTTP_SERVER_PROJECT+HTTP_SERVER_METHOD;
    //public final static String HTTP_URL = "http://182.180.115.236:13551/call";
    //public final static String HTTP_URL = "http://127.0.0.1:8090/datamanager/call";
    
    public final static String IGNITE_CACHE_KEY_DEFAULT = "dinst-realtime";
    
    public final static String IGNITE_SERVER_PORT = "47500..47509";

    public final static String IGNITE_SERVER_IP = HTTP_SERVER_IP+":"+IGNITE_SERVER_PORT;
    
    public final static long IGNITE_CACHE_SIZE_DEFAULT = 5L * 1024 * 1024 * 1024;
    
    public final static String IGNITE_ATOMIC_SEQUENCE = "myStreamSequence";
    
}