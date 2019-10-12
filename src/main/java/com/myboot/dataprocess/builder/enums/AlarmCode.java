package com.myboot.dataprocess.builder.enums;

public enum AlarmCode {
	
	//H,F,C,空
	H("H","H"),F("F","F"),C("C","C"),NONE(" "," ");
	
	public String code;
	
    public String text;

    AlarmCode(String code, String text) {
        this.code = code;
        this.text = text;
    }
    
    public static AlarmCode randomType(){
    	AlarmCode[] values = AlarmCode.values();
        return values[(int)(Math.random()*values.length)];
    }

}
