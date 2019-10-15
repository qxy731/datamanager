package com.myboot.dataprocess.process.kafka.common;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.myboot.dataprocess.builder.RandomDataModelBuilder;
import com.myboot.dataprocess.builder.RowkeyGenerator;
import com.myboot.dataprocess.model.ApplyCardEntity;
import com.myboot.dataprocess.model.KafkaApplyCardEntity;
import com.myboot.dataprocess.model.ProtocolEntity;
import com.myboot.dataprocess.model.SchemaEntity;

@Component
public class KafkaDataModelProcess {
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	public KafkaApplyCardEntity assembleKafkaData(String currentDate) {
		KafkaApplyCardEntity apply  = new KafkaApplyCardEntity();
		ProtocolEntity protocol = new ProtocolEntity();
		protocol.setType(myKafkaConfiguration.getOtherParameter("protocol.type"));
		protocol.setVersion(myKafkaConfiguration.getOtherParameter("protocol.version"));
		SchemaEntity schema = new SchemaEntity();
		schema.setNamespace(myKafkaConfiguration.getOtherParameter("source.topic"));
		schema.setTableName(myKafkaConfiguration.getOtherParameter("phoenix_table_name"));
		apply.setProtocol(protocol);
		apply.setSchema(schema);
		apply.setTimestamp(System.currentTimeMillis());
		RowkeyGenerator generator = RowkeyGenerator.getInstance(currentDate);
		long sequence = generator.getSequence();
		ApplyCardEntity data = RandomDataModelBuilder.getRandomDataModel(sequence, currentDate);
		apply.setData(data);
		return apply;
	}
	
	public KafkaApplyCardEntity processHisKafkaData(Map<String,String> map) {
		KafkaApplyCardEntity apply  = new KafkaApplyCardEntity();
		ProtocolEntity protocol = new ProtocolEntity();
		protocol.setType(myKafkaConfiguration.getOtherParameter("protocol.type"));
		protocol.setVersion(myKafkaConfiguration.getOtherParameter("protocol.version"));
		SchemaEntity schema = new SchemaEntity();
		schema.setNamespace(myKafkaConfiguration.getOtherParameter("source.topic"));
		schema.setTableName(myKafkaConfiguration.getOtherParameter("phoenix_table_name"));
		apply.setProtocol(protocol);
		apply.setSchema(schema);
		apply.setTimestamp(System.currentTimeMillis());
		ApplyCardEntity entity = new ApplyCardEntity();
		//申请编号
	    entity.setApplicationNumber(map.get("APPLICATIONNUMBER")==null?"":map.get("APPLICATIONNUMBER").toString());
	    //获取数据日期	Date
	    entity.setCaptureDate(map.get("CAPTUREDATE")==null?"":map.get("CAPTUREDATE").toString());
	    //获取数据时间	Numeric
	    entity.setCaptureTime(map.get("CAPTURETIME")==null?"":map.get("CAPTURETIME").toString());
	    //中止日期	Date
	    entity.setExpiryDate(map.get("EXPIRYDATE")==null?"":map.get("EXPIRYDATE").toString());
	    //申请日	Date
	    entity.setApplicationDate(map.get("APPLICATIONDATE")==null?"":map.get("APPLICATIONDATE").toString());
	    //申请类型 Text
	    entity.setApplicationType(map.get("APPLICATIONTYPE")==null?"":map.get("APPLICATIONTYPE").toString());
	    //信用额度	Numeric
	    entity.setAmount(map.get("AMOUNT")==null?"":map.get("AMOUNT").toString());
	    //分行号	Text
	    entity.setBranch(map.get("BRANCH")==null?"":map.get("BRANCH").toString());
	    //处理结果	Text
	    entity.setDecision(map.get("DECISION")==null?"":map.get("DECISION").toString());
	    //处理原因	Text
	    entity.setDecisionReason(map.get("DECISIONREASON")==null?"":map.get("DECISIONREASON").toString());
	    //处理日期	Date
	    entity.setDecisionDate(map.get("DECISIONDATE")==null?"":map.get("DECISIONDATE").toString());
	    //Id Num 证件号码	Text
	    entity.setCertificateID(map.get("CERTIFICATEID")==null?"":map.get("CERTIFICATEID").toString());
	    //Id Type 证件种类	Text
	    entity.setCertificateType(map.get("CERTIFICATETYPE")==null?"":map.get("CERTIFICATETYPE").toString());
	    //Full Name 主卡姓名	Text
	    entity.setSurname(map.get("SURNAME")==null?"":map.get("SURNAME").toString());
	    //NameonCard 压花名	Text
	    entity.setFirstName(map.get("FIRSTNAME")==null?"":map.get("FIRSTNAME").toString());
	    //中间名	Text
	    entity.setMiddleName(map.get("MIDDLENAME")==null?"":map.get("MIDDLENAME").toString());
	    //Sex 性别	Text
	    entity.setSex(map.get("SEX")==null?"":map.get("SEX").toString());
	    //Date of Birth 出生日期	Date
	    entity.setDateofBirth(map.get("DATEOFBIRTH")==null?"":map.get("DATEOFBIRTH").toString());
	    //Hm Addr1 住宅地址1	Text
	    entity.setHomeAddress(map.get("HOMEADDRESS")==null?"":map.get("HOMEADDRESS").toString());
	    //Hm City 住宅城市	Text
	    entity.setInhabitCity(map.get("INHABITCITY")==null?"":map.get("INHABITCITY").toString());  
	    //HmPcode 住宅邮编	Text
	    entity.setHomePostcode(map.get("HOMEPOSTCODE")==null?"":map.get("HOMEPOSTCODE").toString());
	    //HmPhNum 住宅电话	Text
	    entity.setHomePhoneNumber(map.get("HOMEPHONENUMBER")==null?"":map.get("HOMEPHONENUMBER").toString());
	    //Mobile 手机号码	Text	32
	    entity.setMobilePhoneNumber(map.get("MOBILEPHONENUMBER")==null?"":map.get("MOBILEPHONENUMBER").toString());
	    //Co Name 单位名称	Text	70
	    entity.setCompanyName(map.get("COMPANYNAME")==null?"":map.get("COMPANYNAME").toString());
	    //Co Addr1 单位地址1	Text	40
	    entity.setOfficeAddress1(map.get("OFFICEADDRESS1")==null?"":map.get("OFFICEADDRESS1").toString());
	    //Co Addr2 单位地址2	Text	40
	    entity.setOfficeAddress2(map.get("OFFICEADDRESS2")==null?"":map.get("OFFICEADDRESS2").toString());
	    //Co City 单位城市	Text	40
	    entity.setCompanyAddress3(map.get("COMPANYADDRESS3")==null?"":map.get("COMPANYADDRESS3").toString());
	    //Co Pcode 单位邮编	Text	10
	    entity.setCompanyCity(map.get("COMPANYCITY")==null?"":map.get("COMPANYCITY").toString());
	    //Co Ph Num 单位电话	Text	32
	    entity.setCompanyPhoneNumber(map.get("COMPANYPHONENUMBER")==null?"":map.get("COMPANYPHONENUMBER").toString());
	    //Channel 营销代码	Text	21
	    entity.setSalesCode(map.get("SALESCODE")==null?"":map.get("SALESCODE").toString());
	    //Src of App 申请来源	Text	21
	    entity.setSourceCode(map.get("SOURCECODE")==null?"":map.get("SOURCECODE").toString());
	    //Trackcode	Text	70
	    entity.setTrackcode(map.get("TRACKCODE")==null?"":map.get("TRACKCODE").toString());
	    //CIM码	Text	40
	    entity.setCimCode(map.get("CIMCODE")==null?"":map.get("CIMCODE").toString());
	    //COOKIE地址	Text	40
	    entity.setCookie(map.get("COOKIE")==null?"":map.get("COOKIE").toString());
	    //证件号码(公安)	Text	25
	    entity.setIDVALUE(map.get("IDVALUE")==null?"":map.get("IDVALUE").toString());
	    //证件类型(公安)	Text	25
	    entity.setIDTYPE(map.get("IDTYPE")==null?"":map.get("IDTYPE").toString());
	    //配偶证件号码	Text	25
	    entity.setSPOUSEIDNUMBER(map.get("SPOUSEIDNUMBER")==null?"":map.get("SPOUSEIDNUMBER").toString());
	    //app设备Id	Text	70
	    entity.setAppID(map.get("APPID")==null?"":map.get("APPID").toString());
	    //app手机设备标示码	Text	70
	    entity.setAppCode(map.get("APPCODE")==null?"":map.get("APPCODE").toString());
	    //app渠道uuid	Text	70
	    entity.setApp_uuid(map.get("APP_UUID")==null?"":map.get("APP_UUID").toString());
	    //IP地址	Text	40
	    entity.setIP(map.get("IP")==null?"":map.get("IP").toString());
	    //审批结果	Text	10	通过，不通过
	    entity.setApprovalResult(map.get("APPROVALRESULT")==null?"":map.get("APPROVALRESULT").toString());
	    //调查结论	Text	2	值：K,F,S,空
	    entity.setResearchConclusion(map.get("RESEARCHCONCLUSION")==null?"":map.get("RESEARCHCONCLUSION").toString());
	    //报警代码	Text	2	值：H,S,C
	    entity.setAlarmCode(map.get("ALARMCODE")==null?"":map.get("ALARMCODE").toString());
	    //标识
	    entity.setFlag(map.get("FLAG")==null?"":map.get("FLAG").toString());
		apply.setData(entity);
		return apply;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
