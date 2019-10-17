package com.myboot.dataprocess.model.result;

import java.util.Map;

public class RuleEvalResultDto extends ApiDto {
  private static final long serialVersionUID = 1L;
  /**
   * 规则编号
   */
  private String id;
  /**
   * 规则编号
   */
  private String code;
  /**
   * 规则名称
   */
  private String name;
  /**
   * 表达式
   */
  private String exp;

  /** 本rule的评估结果 */
  private Boolean evalResult;

  /** 是否命中 */
  private Boolean hit;
  /** 事件槽信息 */
  private Map<String, Object> eventSlotInfo;
  /** 调试信息 */
  private Map<String, Object> traceInfo;
  private String disableReason;

  public RuleEvalResultDto(String ruleId, String ruleCode, String ruleName) {
    super();
    this.id = ruleId;
    this.code = ruleCode;
    this.name = ruleName;
  }

  public RuleEvalResultDto(String ruleId, String ruleCode, String ruleName, String ruleExp) {
    this(ruleId, ruleCode, ruleName);
    this.exp = ruleExp;
  }

  public RuleEvalResultDto(Boolean evalResult, String ruleId, String ruleCode, String ruleName, String ruleExp) {
    this(ruleId, ruleCode, ruleName);
    this.evalResult = evalResult;
    this.exp = ruleExp;
  }

  public RuleEvalResultDto(Boolean evalResult, String ruleId, String ruleCode, String ruleName, String ruleExp,
      Map<String, Object> traceInfo) {
    this(evalResult, ruleId, ruleCode, ruleName, ruleExp);
    this.traceInfo = traceInfo;
  }

  public RuleEvalResultDto(Boolean evalResult, String ruleId, String ruleCode, String ruleName, String ruleExp,
      Map<String, Object> traceInfo, String disableReason) {
    this(evalResult, ruleId, ruleCode, ruleName, ruleExp, traceInfo);
    this.disableReason = disableReason;
  }

  public String getId() {
    return id;
  }

  public void setId(String ruleId) {
    this.id = ruleId;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String ruleCode) {
    this.code = ruleCode;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getExp() {
    return exp;
  }

  public void setExp(String ruleExp) {
    this.exp = ruleExp;
  }

  public Boolean getEvalResult() {
    return evalResult;
  }

  public void setEvalResult(Boolean evalResult) {
    this.evalResult = evalResult;
  }

  public Map<String, Object> getTraceInfo() {
    return traceInfo;
  }

  public void setTraceInfo(Map<String, Object> traceInfo) {
    this.traceInfo = traceInfo;
  }

  public String getDisableReason() {
    return disableReason;
  }

  public void setDisableReason(String disableReason) {
    this.disableReason = disableReason;
  }

  public Boolean getHit() {
    return hit;
  }

  public void setHit(Boolean hit) {
    this.hit = hit;
  }

  public RuleEvalResultDto() {
  }

  public Map<String, Object> getEventSlotInfo() {
    return eventSlotInfo;
  }

  public void setEventSlotInfo(Map<String, Object> eventSlotInfo) {
    this.eventSlotInfo = eventSlotInfo;
  }
}
