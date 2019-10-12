package com.myboot.dataprocess.model;


import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * HBase的进件流水单
 */
@Data
@ApiModel(value = "HBase的进件流水单", description = "HBase的进件流水单")
public class ApplyCardEntity {
	
	@ApiModelProperty(value = "申请编号", required = true)
    private String applicationNumber;	//申请编号
	@ApiModelProperty(value = "申请编号", required = true)
    private String captureDate; //获取数据日期	Date
    private String captureTime; //获取数据时间	Numeric
    private String expiryDate; //中止日期	Date
    private String applicationDate; //申请日	Date
    private String applicationType; //申请类型	Text
    private String amount; //信用额度	Numeric
    private String branch;  //分行号	Text
    private String decision;  //处理结果	Text
    private String decisionReason;  //处理原因	Text
    private String decisionDate;   //处理日期	Date
    private String certificateID;  //Id Num 证件号码	Text
    private String certificateType;   //Id Type 证件种类	Text
    private String surname;  //	Full Name 主卡姓名	Text
    private String firstName;  //	NameonCard 压花名	Text
    private String middleName;   //	中间名	Text
    private String sex;     //	Sex 性别	Text
    private String dateofBirth;    //	Date of Birth 出生日期	Date
    private String homeAddress;   //	Hm Addr1 住宅地址1	Text
    private String inhabitCity;   //	Hm City 住宅城市	Text
    private String homePostcode;   //	HmPcode 住宅邮编	Text
    private String homePhoneNumber;  //	HmPhNum 住宅电话	Text
    private String mobilePhoneNumber;   //	Mobile 手机号码	Text	32
    private String companyName;   ///	Co Name 单位名称	Text	70
    private String officeAddress1;  //	Co Addr1 单位地址1	Text	40
    private String officeAddress2;   //	Co Addr2 单位地址2	Text	40
    private String companyAddress3;   //	Co City 单位城市	Text	40
    private String companyCity;   //	Co Pcode 单位邮编	Text	10
    private String companyPhoneNumber;   //	Co Ph Num 单位电话	Text	32
    private String salesCode;   //	Channel 营销代码	Text	21
    private String sourceCode;   //	Src of App 申请来源	Text	21
    private String trackcode;    //	Trackcode	Text	70	　
    private String cimCode;    //	CIM码	Text	40	　
    private String cookie;   //	COOKIE地址	Text	40	　
    private String idvalue;   //	证件号码(公安)	Text	25
    private String idtype;  //	证件类型(公安)	Text	25
    private String spouseidnumber;   //	配偶证件号码	Text	25
    private String appID;    //	app设备Id	Text	70	　
    private String appCode;   //	app手机设备标示码	Text	70	　
    private String app_uuid;   //	app渠道uuid	Text	70
    private String ip;   //	IP地址	Text	40
    private String approvalResult;  //	审批结果	Text	10	通过，不通过
    private String researchConclusion;  //	调查结论	Text	2	值：K,F,S,空
    private String alarmCode;   //	报警代码	Text	2	值：H,S,C
    
}