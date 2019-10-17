package com.myboot.dataprocess.model.result;

public class ApiResponse<T extends ApiDto> extends ApiDto {
  private static final long serialVersionUID = 1L;
  private StatusEnum status;
  private String code;
  private String message;
  private T payload;

  public ApiResponse(StatusEnum status, String code) {
    super();
    this.status = status;
    this.code = code;
  }

  public ApiResponse(StatusEnum status, String code, String message) {
    super();
    this.status = status;
    this.code = code;
    this.message = message;
  }

  public ApiResponse(StatusEnum status, String code, String message, T payload) {
    super();
    this.status = status;
    this.code = code;
    this.message = message;
    this.payload = payload;
  }

  public StatusEnum getStatus() {
    return status;
  }

  public void setStatus(StatusEnum status) {
    this.status = status;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public T getPayload() {
    return payload;
  }

  public void setPayload(T payload) {
    this.payload = payload;
  }

  ApiResponse() {
  }

}
