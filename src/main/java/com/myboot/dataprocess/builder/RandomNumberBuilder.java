package com.myboot.dataprocess.builder;

import java.security.SecureRandom;
import java.util.Random;

public class RandomNumberBuilder {
	
	public static String getRangeRandom2Str(int min,int max) {
        Random random = new Random();
        int s = random.nextInt(max)%(max-min+1) + min;
        return String.valueOf(s);
	}
	
	public static int getRangeRandom2Int(int min,int max) {
        Random random = new Random();
        int s = random.nextInt(max)%(max-min+1) + min;
        return s;
	}
	
	 /**
     * 生成16位不重复的随机数，含数字+大小写
     * @return
     */
    public static String getRandomGuid(int num) {
        StringBuilder uid = new StringBuilder();
        //产生16位的强随机数
        Random rd = new SecureRandom();
        for (int i = 0; i < num; i++) {
            //产生0-2的3位随机数
            int type = rd.nextInt(3);
            switch (type){
                case 0:
                    //0-9的随机数
                    uid.append(rd.nextInt(10));
                    break;
                case 1:
                    //ASCII在65-90之间为大写,获取大写随机
                    uid.append((char)(rd.nextInt(25)+65));
                    break;
                case 2:
                    //ASCII在97-122之间为小写，获取小写随机
                    uid.append((char)(rd.nextInt(25)+97));
                    break;
                default:
                    break;
            }
        }
        return uid.toString();
    }
	
	public static String getRangeRandomHexStr() {
		int min = 1;
		int max = 16;
        Random random = new Random();
        int s = random.nextInt(max)%(max-min+1) + min;
        return Integer.toHexString(s).toUpperCase();
	}
	
	public static void main(String[] args) {
		System.out.println(getRandomGuid(16));
	}
	
	

}
