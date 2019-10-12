package com.myboot.dataprocess.builder.enums;

public enum ApplicationType {
	
	CARD("CARD","信用卡");
	
	public String text;
	
    public String testStr;

    ApplicationType(String text, String testStr) {
        this.text = text;
        this.testStr = testStr;
    }
    
    public static ApplicationType randomType(ApplicationType[] values){
        return values[(int)(Math.random()*values.length)];
    }


}
