package com.myboot.dataprocess.builder;

import java.io.UnsupportedEncodingException;
import java.util.Random;

public class RandomCharacterBuilder {
	
	public static String getRandomStr(int num) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<num;i++) {
			sb.append(String.valueOf(getRandomChar()));
		}
		return sb.toString();
	}
	
	private static char getRandomChar() {
		String str = "";
		int hightPos; //
		int lowPos;
		Random random = new Random();
		hightPos = (176 + Math.abs(random.nextInt(39)));
		lowPos = (161 + Math.abs(random.nextInt(93)));
		byte[] b = new byte[2];
		b[0] = (Integer.valueOf(hightPos)).byteValue();
		b[1] = (Integer.valueOf(lowPos)).byteValue();
		try {
		  str = new String(b, "GBK");
		} catch (UnsupportedEncodingException e) {
		  e.printStackTrace();
		  System.out.println("错误");
		}
		    return str.charAt(0);
	}
	
	/**
	 * 随机获取一串汉字
	 * @return
	 */
			
	public static String getRandomString(int num) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<num;i++) {
			char c = (char)(0x4e00 + (int) (Math.random() * (0x9fa5 - 0x4e00 + 1)));
			sb.append(String.valueOf(c));
		}
		return sb.toString();
        //return (char) (0x4e00 + (int) (Math.random() * (0x9fa5 - 0x4e00 + 1)));
    }
	
	//生成随机数字和字母,
    public static String getRandomNumOrChar(int length) {
        
        String val = "";
        Random random = new Random();
        
        //参数length，表示生成几位随机数
        for(int i = 0; i < length; i++) {
            
            String charOrNum = random.nextInt(2) % 2 == 0 ? "char" : "num";
            //输出字母还是数字
            if( "char".equalsIgnoreCase(charOrNum) ) {
                //输出是大写字母还是小写字母
                int temp = random.nextInt(2) % 2 == 0 ? 65 : 97;
            	val += (char)(random.nextInt(26) + temp);
            } else if( "num".equalsIgnoreCase(charOrNum) ) {
                val += String.valueOf(random.nextInt(10));
            }
        }
        return val.toUpperCase();
    }
	public static void main(String[] args) {
		/*System.out.println(getRandomStr(24));
		System.out.println(getRandomString(24));*/
		System.out.println(getRandomNumOrChar(1));
	}

}
