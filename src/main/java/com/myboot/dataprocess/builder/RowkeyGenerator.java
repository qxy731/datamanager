package com.myboot.dataprocess.builder;

public class RowkeyGenerator {
	
	/**
	 * 申请编号自增长序列的起始值，7位
	 */
	private static long sequence = 1000000L;
	
	private static String rowkey = "20191001"+1000000+"|"+System.currentTimeMillis();
	
	private static RowkeyGenerator instance  = new RowkeyGenerator();
	
	private RowkeyGenerator(){}
	
	public long getSequence() {
		return sequence;
	}
	
	public String getRowkey() {
		return rowkey;
	}
	
	public static RowkeyGenerator getInstance(String currentDate) {
		sequence++;
    	rowkey = currentDate.replaceAll("-", "")+sequence+"|"+String.valueOf(System.currentTimeMillis());
		return instance;
	}
	
    public static void main(String[] args) {
    	String currentDate = "20191007";
    	for(int i=0;i<10;i++) {
	    	RowkeyGenerator generator = RowkeyGenerator.getInstance(currentDate);
	        System.out.println("Sequence:"+generator.getSequence()+"&rowkey:"+generator.getRowkey());
    	}
    }

}
