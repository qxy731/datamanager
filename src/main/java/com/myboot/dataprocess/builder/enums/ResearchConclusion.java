package com.myboot.dataprocess.builder.enums;

public enum ResearchConclusion {
	//K,F,S,空
	K("K","K"),F("F","F"),S("S","S"),NONE(" "," ");
	
	public String code;
	
    public String text;

    ResearchConclusion(String code, String text) {
        this.code = code;
        this.text = text;
    }
    
    public static ResearchConclusion randomType(){
    	ResearchConclusion[] values = ResearchConclusion.values();
        return values[(int)(Math.random()*values.length)];
    }

}
