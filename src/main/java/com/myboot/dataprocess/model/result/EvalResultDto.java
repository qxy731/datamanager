package com.myboot.dataprocess.model.result;

import java.util.List;
import java.util.Map;

public class EvalResultDto extends ApiDto {
	private static final long serialVersionUID = 1L;

	/** (外部)请求号 */
	private String reqNo;

	/** (总体)评估结果 */
	private boolean result;

	/** 命中规则的表示 */
	private String hitRuleExp;

	/** 返回的计算结果 */
	private Map<String, Object> resultMap;

	/** 所有rules的评估结果明细 */
	private List<RuleEvalResultDto> rules;

	public Map<String, Object> getResultMap() {
		return resultMap;
	}

	public void setResultMap(Map<String, Object> resultMap2) {
		this.resultMap = resultMap2;
	}

	public EvalResultDto(String reqNo) {
		super();
		this.reqNo = reqNo;
	}

	public EvalResultDto(String reqNo, boolean result) {
		super();
		this.reqNo = reqNo;
		this.result = result;
	}

	public EvalResultDto(boolean result) {
		super();
		this.result = result;
	}

	public EvalResultDto(boolean result, List<RuleEvalResultDto> rules) {
		super();
		this.result = result;
		this.rules = rules;
	}

	public String getReqNo() {
		return reqNo;
	}

	public void setReqNo(String reqNo) {
		this.reqNo = reqNo;
	}

	public boolean getResult() {
		return result;
	}

	public void setResult(boolean result) {
		this.result = result;
	}

	public EvalResultDto(List<RuleEvalResultDto> rules) {
		this.rules = rules;
	}

	public EvalResultDto() {
	}

	public String getHitRuleExp() {
		return hitRuleExp;
	}

	public void setHitRuleExp(String hitRuleExp) {
		this.hitRuleExp = hitRuleExp;
	}

	public List<RuleEvalResultDto> getRules() {
		return rules;
	}

	public void setRules(List<RuleEvalResultDto> rules) {
		this.rules = rules;
	}
}
