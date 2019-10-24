package com.myboot.dataprocess.process.ignite;

import java.io.Serializable;

/**
 * Ignite文件对象类
 * @author 2zhouliang2
 *
 */
public class IgniteFilePojo implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private String fileNo;
	
	private int dimension;
	
	/**
	 * 维度1
	 */
	private String key1;
	/**
	 * 维度2
	 */
	private String key2;
	
	private int key1Value;
	
	private int key2Value;
	/**
	 * 维度相关的衍生变量名数组
	 */
	private String[] arr;
	
	public String getFileNo() {
		return fileNo;
	}

	public void setFileNo(String fileNo) {
		this.fileNo = fileNo;
	}

	public String getKey1() {
		return key1;
	}

	public void setKey1(String key1) {
		this.key1 = key1;
	}

	public String getKey2() {
		return key2;
	}

	public void setKey2(String key2) {
		this.key2 = key2;
	}
	
	public int getKey1Value() {
		return key1Value;
	}

	public void setKey1Value(int key1Value) {
		this.key1Value = key1Value;
	}

	public int getKey2Value() {
		return key2Value;
	}

	public void setKey2Value(int key2Value) {
		this.key2Value = key2Value;
	}

	public int getDimension() {
		return dimension;
	}

	public void setDimension(int dimension) {
		this.dimension = dimension;
	}

	public String[] getArr() {
		return arr;
	}

	public void setArr(String[] arr) {
		this.arr = arr;
	}

}
