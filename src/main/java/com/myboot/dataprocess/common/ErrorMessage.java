package com.myboot.dataprocess.common;

import org.springframework.util.StringUtils;

public enum ErrorMessage {

	m0("0", "成功"),
	m1("1", "文件不存在"),
	m2("2", "文件格式错误，后缀名有误"),
	m_excel("3", "sheet %s 第%s 行%s数据格式有误，导入失败"),
	m_excel_bk("4", "系统在sheet %s 第 %s 行 %s发生异常，请重试或联系管理员帮助"),
	m5("5", "系统数据对象初始化异常，请联系管理员"),
	m_cell_null("6", "系统在sheet %s 第 %s 行 %s发生异常，该字段不能为空值"),
	m_sys("7", "系统在csv文件 %s 第 %s 行发生异常，请重试或联系管理员"),
	m_date_form("8", "系统在csv文件 %s 第 %s 行 %s 处发生异常，日期格式为 %s"),
	m_zip_err("9", "部分压缩文件读取失败，请重试"),
	m_rar_err("10", "部分RAR压缩文件读取失败，请重试"),
	m_rar5_err("11", "WINRAR版本5.0以上不支持解压，请使用其他版本"),
	m_rest_sys_err("12", "系统异常请重试或联系管理员"),
	msg_hdfs_up_fail("13", "文件上传失败，请稍后重试"),
	msg_hdfs_up_null("14", "文件上传错误，必须选择至少一个文件"),
	msg_hdfs_file_repeat("15", "指定类型的文件名称已存在，请重新命名后上传"),
	msg_mysql_file_repeat("16", "文件上传失败，上传文件列表可能存在同名文件"),
    msg_img_file_unsupport("17", "不支持的文件格式"),
	msg_hdfs_file_dir_unauth("18", "系统异常，文件服务拒绝上传"),
	msg_img_abridge_error("19", "图片上传失败，请重试或联系管理员"),
    msg_hql_check_error("20", "表达式语法错误，验证不通过"),
	msg_access_2_much_warn("21", "访问过于频繁"),
	msg_hr_user_update_fail("22", "部分或全部数据更新失败"),
	msg_mail_fail("23", "邮件发送失败"),
	msg_sql_check_fail("24", "标签表达式验证失败"),
	msg_task_fail("25", "群体画像任务创建失败"),
	msg_opt_fail("26","操作失败"),
	msg_sys_fail("27","系统服务异常，请稍后再试，或尝试联系管理员"),
	msg_params_need_fail("28","参数缺失"),
	msg_params_type_fail("29","参数类型错误"),
	msg_params_fail("30","参数值有误"),
	msg_cas_redirect("31","单点登录超时重定向"),
	msg_img_file_toobig("32", "文件大小超限, 文件大小不应超过 %s MB"),
	msg_tmp_file("33", "文件解析时发生临时文件操作异常，请联系管理员"),
	msg_auth("34", "当前用户没有数据权限的，请联系管理员配置数据权限"),
    msg_convert("35", "文件内容转换出错");
	

	private String msgCode; 
	
	private String msg;
	
	ErrorMessage(String msgCode, String msg) {
		this.msgCode = msgCode;
		this.msg = msg;
	}
	
	public String getMsgCode() {
		return this.msgCode;
	}
	
	public String getMsg() {
		return this.msg;
	}

	public static ErrorMessage getByMsgCode(String code) {
		for (ErrorMessage value : ErrorMessage.values()) {
			if(StringUtils.isEmpty(code)) {
				return msg_hdfs_up_fail;
			}
			if(value.getMsgCode().equals(code)) {
				return value;
			}
		}
		return m0;
	}
}