package com.myboot.dataprocess.common;

public class StatusInfo<T> {
	
    private String message;

    private String status;
    
	private T data;
	
    public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	public StatusInfo() {
        this.status = ErrorMessage.m0.getMsgCode();
        this.message = ErrorMessage.m0.getMsg();
    }
    
    public StatusInfo(String message,String status) {
    	this.status = status;
        this.message = message;
        this.data = null;
    }
    
	public StatusInfo(ErrorMessage errMsg) {
		this.message = errMsg.getMsg();
		this.status = errMsg.getMsgCode();
		this.data = null;
	}

    public StatusInfo(T data) {
    	this.message = ErrorMessage.m0.getMsg();
    	this.status = ErrorMessage.m0.getMsgCode();
        this.data = data;
    }
    
    public StatusInfo(String message,String status,T data) {
    	this.message = message;
    	this.status = status;
        this.data = data;
    }
    
}