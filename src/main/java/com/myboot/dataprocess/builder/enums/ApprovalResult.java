package com.myboot.dataprocess.builder.enums;

public enum ApprovalResult {
	
	AGREE("通过","通过"),REFUSE("不通过","不通过");
	
	public String code;
	
    public String text;

    ApprovalResult(String code, String text) {
        this.code = code;
        this.text = text;
    }
    
    public static ApprovalResult randomType(){
    	ApprovalResult[] values = ApprovalResult.values();
        return values[(int)(Math.random()*values.length)];
    }

}
