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
    private String ApplicationNumber;	//申请编号
	@ApiModelProperty(value = "申请编号", required = true)
    private String CaptureDate; //获取数据日期	Date
    private String CaptureTime; //获取数据时间	Numeric
    private String ExpiryDate; //中止日期	Date
    private String ApplicationDate; //申请日	Date
    private String ApplicationType; //申请类型	Text
    private String Amount; //信用额度	Numeric
    private String Branch;  //分行号	Text
    private String Decision;  //处理结果	Text
    private String DecisionReason;  //处理原因	Text
    private String DecisionDate;   //处理日期	Date
    private String CertificateID;  //Id Num 证件号码	Text
    private String CertificateType;   //Id Type 证件种类	Text
    private String Surname;  //	Full Name 主卡姓名	Text
    private String FirstName;  //	NameonCard 压花名	Text
    private String MiddleName;   //	中间名	Text
    private String Sex;     //	Sex 性别	Text
    private String DateofBirth;    //	Date of Birth 出生日期	Date
    private String HomeAddress;   //	Hm Addr1 住宅地址1	Text
    private String InhabitCity;   //	Hm City 住宅城市	Text
    private String HomePostcode;   //	HmPcode 住宅邮编	Text
    private String HomePhoneNumber;  //	HmPhNum 住宅电话	Text
    private String MobilePhoneNumber;   //	Mobile 手机号码	Text	32
    private String CompanyName;   ///	Co Name 单位名称	Text	70
    private String OfficeAddress1;  //	Co Addr1 单位地址1	Text	40
    private String OfficeAddress2;   //	Co Addr2 单位地址2	Text	40
    private String CompanyAddress3;   //	Co City 单位城市	Text	40
    private String CompanyCity;   //	Co Pcode 单位邮编	Text	10
    private String CompanyPhoneNumber;   //	Co Ph Num 单位电话	Text	32
    private String SalesCode;   //	Channel 营销代码	Text	21
    private String SourceCode;   //	Src of App 申请来源	Text	21
    private String Trackcode;    //	Trackcode	Text	70	　
    private String CimCode;    //	CIM码	Text	40	　
    private String Cookie;   //	COOKIE地址	Text	40	　
    private String IDTYPE;   //	证件号码(公安)	Text	25
    private String IDVALUE;  //	证件类型(公安)	Text	25
    private String SPOUSEIDNUMBER;   //	配偶证件号码	Text	25
    private String AppID;    //	app设备Id	Text	70	　
    private String AppCode;   //	app手机设备标示码	Text	70	　
    private String App_uuid;   //	app渠道uuid	Text	70
    private String IP;   //	IP地址	Text	40
    private String ApprovalResult;  //	审批结果	Text	10	通过，不通过
    private String ResearchConclusion;  //	调查结论	Text	2	值：K,F,S,空
    private String AlarmCode;   //	报警代码	Text	2	值：H,S,C
    private String flag;
    
}