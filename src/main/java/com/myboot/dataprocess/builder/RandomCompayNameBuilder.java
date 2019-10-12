package com.myboot.dataprocess.builder;

import java.util.Random;

public class RandomCompayNameBuilder {
	
	private static String[] name = {"毕博诚网络有限公司","合联电子信息有限公司","科技有限公司","股份有限公司"};
	
	
	public static String getRandomCompayName() {
		Random r = new Random();
		int index = r.nextInt(name.length);
		String company = name[index];
		int num = r.nextInt(5);
		String str = RandomCharacterBuilder.getRandomStr(num);
		company = str + company;
		return company;
	 }
	
	  public static void main(String[] args) {
	    	System.out.println(getRandomCompayName());
	    }

}
