package com.myboot.dataprocess.builder.enums;

public enum Sex {
	
	FEMALE("F","女"),MAN("M","男");
	
	
	public String code;
	
    public String text;

    Sex(String code, String text) {
        this.code = code;
        this.text = text;
    }
    
    public static Sex randomType(){
    	Sex[] values = Sex.values();
        return values[(int)(Math.random()*values.length)];
    }

}
