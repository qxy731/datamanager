package com.myboot.dataprocess.model.result;

/**
 * 通用状态
 * 
 * @author mengjie
 */
public enum StatusEnum {
  /** 初始(等待处理) */
  I,

  /** 处理中 */
  P,

  /** 成功(终态) */
  S,

  /** 失败(终态) */
  F;
}
