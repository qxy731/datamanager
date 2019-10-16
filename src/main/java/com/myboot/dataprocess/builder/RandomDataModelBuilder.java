package com.myboot.dataprocess.builder;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.myboot.dataprocess.builder.enums.AlarmCode;
import com.myboot.dataprocess.builder.enums.ApprovalResult;
import com.myboot.dataprocess.builder.enums.ResearchConclusion;
import com.myboot.dataprocess.builder.enums.Sex;
import com.myboot.dataprocess.model.KafkaApplyCardEntity;
import com.myboot.dataprocess.model.ProtocolEntity;
import com.myboot.dataprocess.model.SchemaEntity;
import com.myboot.dataprocess.tools.CommonTool;

public class RandomDataModelBuilder {
	
	public static Map<String,Object> getRandomDataModel(long sequence,String currentDate) {
		Map<String,Object> map = new LinkedHashMap<String,Object>();
		//申请编号
	    map.put("ApplicationNumber",currentDate.replaceAll("-", "")+sequence);
	    //获取数据日期	Date
	    map.put("CaptureDate",CommonTool.getCurrentDate());
	    //获取数据时间	Numeric
	    map.put("CaptureTime",CommonTool.getCurrentTime());
	    //中止日期	Date
	    map.put("ExpiryDate",CommonTool.getCurrentDate());
	    //申请日	Date
	    map.put("ApplicationDate",currentDate);
	    //申请类型 Text
	    map.put("ApplicationType","CARD");
	    map.put("Amount",RandomNumberBuilder.getRangeRandom2Str(3000,100000));
	    //分行号	Text
	    map.put("Branch",RandomNumberBuilder.getRangeRandom2Str(10,100));
	    //处理结果	Text
	    String decision = RandomNumberBuilder.getRangeRandom2Str(1,4);
	    map.put("Decision",decision);
	    //处理原因	Text //处理日期	Date
	    if("3".equals(decision)) {
	    	map.put("DecisionReason",RandomCharacterBuilder.getRandomStr(10));
	    	map.put("DecisionDate",CommonTool.addDay(currentDate,RandomNumberBuilder.getRangeRandom2Int(1,4)));
	    }else {
	    	map.put("DecisionReason","");
	    	map.put("DecisionDate","");
	    }
	    //Id Num 证件号码	Text
	    String certNo = RandomCertNoBuilder.getRandomCertNo();
	    map.put("CertificateID",certNo);
	    //Id Type 证件种类	Text
	    map.put("CertificateType","10");
	    //Full Name 主卡姓名	Text
	    map.put("Surname",RandomNameBuilder.build());
	    //NameonCard 压花名	Text
	    map.put("FirstName",RandomNameBuilder.getFirstName());
	    //中间名	Text
	    map.put("MiddleName",RandomNameBuilder.getMiddleName());
	    //Sex 性别	Text
	    map.put("Sex",Sex.randomType().code);
	    //Date of Birth 出生日期	Date
	    int index = RandomAddressBuilder.getRandomProvinceIndex();
    	String province = RandomAddressBuilder.getRandomProvince(index);
    	String city = RandomAddressBuilder.getRandomCity(index);
    	String address = RandomAddressBuilder.getRandomAddress(province,city);
	    map.put("DateofBirth",CommonTool.getBirthByCertNo(certNo));
	    //Hm Addr1 住宅地址1	Text
	    map.put("HomeAddress",address);
	    //Hm City 住宅城市	Text
	    map.put("InhabitCity",city);  
	    //HmPcode 住宅邮编	Text
	    map.put("HomePostcode",RandomNumberBuilder.getRangeRandom2Str(1000000,9000000));
	    //HmPhNum 住宅电话	Text
	    map.put("HomePhoneNumber",RandomPhoneBuilder.getRandomTel());
	    //Mobile 手机号码	Text	32
	    map.put("MobilePhoneNumber",RandomPhoneBuilder.getRandomTel());
	    //Co Name 单位名称	Text	70
	    map.put("CompanyName",RandomCompayNameBuilder.getRandomCompayName());
	    //Co Addr1 单位地址1	Text	40
	    map.put("OfficeAddress1",RandomAddressBuilder.getRandomAddress(province,city));
	    //Co Addr2 单位地址2	Text	40
	    map.put("OfficeAddress2","");
	    //Co City 单位城市	Text	40
	    map.put("CompanyAddress3","");
	    //Co Pcode 单位邮编	Text	10
	    map.put("CompanyCity",city);
	    //Co Ph Num 单位电话	Text	32
	    map.put("CompanyPhoneNumber",RandomPhoneBuilder.getRandomTel());
	    //Channel 营销代码	Text	21
	    map.put("SalesCode",RandomNumberBuilder.getRangeRandom2Str(1000,9999));
	    //Src of App 申请来源	Text	21
	    map.put("SourceCode",RandomCharacterBuilder.getRandomNumOrChar(1));
	    //Trackcode	Text	70
	    map.put("Trackcode",RandomNumberBuilder.getRangeRandom2Str(1000,9999));
	    //CIM码	Text	40
	    map.put("CimCode",RandomNumberBuilder.getRandomGuid(16));
	    //COOKIE地址	Text	40
	    map.put("Cookie",RandomIpBuilder.getRandomIp());
	    //证件号码(公安)	Text	25
	    map.put("IDVALUE",certNo);
	    //证件类型(公安)	Text	25
	    map.put("IDTYPE",RandomNumberBuilder.getRangeRandom2Str(10,30));
	    //配偶证件号码	Text	25
	    map.put("SPOUSEIDNUMBER",RandomCertNoBuilder.getRandomCertNo());
	    //app设备Id	Text	70
	    map.put("AppID",RandomNumberBuilder.getRandomGuid(17));
	    //app手机设备标示码	Text	70
	    map.put("AppCode",RandomNumberBuilder.getRandomGuid(14));
	    //app渠道uuid	Text	70
	    map.put("App_uuid",RandomNumberBuilder.getRandomGuid(16));
	    //IP地址	Text	40
	    map.put("IP",RandomIpBuilder.getRandomIp());
	    //审批结果	Text	10	通过，不通过
	    map.put("ApprovalResult",ApprovalResult.randomType().code);
	    //调查结论	Text	2	值：K,F,S,空
	    map.put("ResearchConclusion",ResearchConclusion.randomType().code);
	    //报警代码	Text	2	值：H,S,C
	    map.put("AlarmCode",AlarmCode.randomType().code);
	    //标识
	    map.put("flag","");
	    return map;
	}
	
	public static KafkaApplyCardEntity processHisKafkaData(Map<String,Object> map) {
		KafkaApplyCardEntity apply  = new KafkaApplyCardEntity();
		ProtocolEntity protocol = new ProtocolEntity();
		protocol.setType("protocol.type");
		protocol.setVersion("protocol.version");
		SchemaEntity schema = new SchemaEntity();
		schema.setNamespace("source.topic");
		schema.setTableName("phoenix_table_name");
		apply.setProtocol(protocol);
		apply.setSchema(schema);
		apply.setTimestamp(System.currentTimeMillis());
		//Map<String,Object> retMap = new LinkedHashMap<String,Object>();
		//申请编号
	    map.put("ApplicationNumber",map.get("APPLICATIONNUMBER")==null?"":map.get("APPLICATIONNUMBER").toString());
	    map.remove("APPLICATIONNUMBER");
	    //获取数据日期	Date
	    map.put("CaptureDate",map.get("CAPTUREDATE")==null?"":map.get("CAPTUREDATE").toString());
	    map.remove("CAPTUREDATE");
	    //获取数据时间	Numeric
	    map.put("CaptureTime",map.get("CAPTURETIME")==null?"":map.get("CAPTURETIME").toString());
	    map.remove("CAPTURETIME");
	    //中止日期	Date
	    map.put("ExpiryDate",map.get("EXPIRYDATE")==null?"":map.get("EXPIRYDATE").toString());
	    map.remove("ExpiryDate");
	    //申请日	Date
	    map.put("ApplicationDate",map.get("APPLICATIONDATE")==null?"":map.get("APPLICATIONDATE").toString());
	    map.remove("APPLICATIONDATE");
	    //申请类型 Text
	    map.put("ApplicationType",map.get("APPLICATIONTYPE")==null?"":map.get("APPLICATIONTYPE").toString());
	    map.remove("APPLICATIONTYPE");
	    //信用额度	Numeric
	    map.put("Amount",map.get("AMOUNT")==null?"":map.get("AMOUNT").toString());
	    map.remove("AMOUNT");
	    //分行号	Text
	    map.put("Branch",map.get("BRANCH")==null?"":map.get("BRANCH").toString());
	    map.remove("BRANCH");
	    //处理结果	Text
	    map.put("Decision",map.get("DECISION")==null?"":map.get("DECISION").toString());
	    map.remove("DECISION");
	    //处理原因	Text
	    map.put("DecisionReason",map.get("DECISIONREASON")==null?"":map.get("DECISIONREASON").toString());
	    map.remove("DECISIONREASON");
	    //处理日期	Date
	    map.put("DecisionDate",map.get("DECISIONDATE")==null?"":map.get("DECISIONDATE").toString());
	    map.remove("DecisionDate");
	    //Id Num 证件号码	Text
	    map.put("CertificateID",map.get("CERTIFICATEID")==null?"":map.get("CERTIFICATEID").toString());
	    map.remove("CERTIFICATEID");
	    //Id Type 证件种类	Text
	    map.put("CertificateType",map.get("CERTIFICATETYPE")==null?"":map.get("CERTIFICATETYPE").toString());
	    map.remove("CERTIFICATETYPE");
	    //Full Name 主卡姓名	Text
	    map.put("Surname",map.get("SURNAME")==null?"":map.get("SURNAME").toString());
	    map.remove("SURNAME");
	    //NameonCard 压花名	Text
	    map.put("FirstName",map.get("FIRSTNAME")==null?"":map.get("FIRSTNAME").toString());
	    map.remove("FIRSTNAME");
	    //中间名	Text
	    map.put("MiddleName",map.get("MIDDLENAME")==null?"":map.get("MIDDLENAME").toString());
	    map.remove("MIDDLENAME");
	    //Sex 性别	Text
	    map.put("Sex",map.get("SEX")==null?"":map.get("SEX").toString());
	    map.remove("SEX");
	    //Date of Birth 出生日期	Date
	    map.put("DateofBirth",map.get("DATEOFBIRTH")==null?"":map.get("DATEOFBIRTH").toString());
	    map.remove("DATEOFBIRTH");
	    //Hm Addr1 住宅地址1	Text
	    map.put("HomeAddress",map.get("HOMEADDRESS")==null?"":map.get("HOMEADDRESS").toString());
	    map.remove("HOMEADDRESS");
	    //Hm City 住宅城市	Text
	    map.put("InhabitCity",map.get("INHABITCITY")==null?"":map.get("INHABITCITY").toString());  
	    map.remove("INHABITCITY");
	    //HmPcode 住宅邮编	Text
	    map.put("HomePostcode",map.get("HOMEPOSTCODE")==null?"":map.get("HOMEPOSTCODE").toString());
	    map.remove("HOMEPOSTCODE");
	    //HmPhNum 住宅电话	Text
	    map.put("HomePhoneNumber",map.get("HOMEPHONENUMBER")==null?"":map.get("HOMEPHONENUMBER").toString());
	    map.remove("HOMEPHONENUMBER");
	    //Mobile 手机号码	Text	32
	    map.put("MobilePhoneNumber",map.get("MOBILEPHONENUMBER")==null?"":map.get("MOBILEPHONENUMBER").toString());
	    map.remove("MOBILEPHONENUMBER");
	    //Co Name 单位名称	Text	70
	    map.put("CompanyName",map.get("COMPANYNAME")==null?"":map.get("COMPANYNAME").toString());
	    map.remove("COMPANYNAME");
	    //Co Addr1 单位地址1	Text	40
	    map.put("OfficeAddress1",map.get("OFFICEADDRESS1")==null?"":map.get("OFFICEADDRESS1").toString());
	    map.remove("OFFICEADDRESS1");
	    //Co Addr2 单位地址2	Text	40
	    map.put("OfficeAddress2",map.get("OFFICEADDRESS2")==null?"":map.get("OFFICEADDRESS2").toString());
	    map.remove("OfficeAddress2");
	    //Co City 单位城市	Text	40
	    map.put("CompanyAddress3",map.get("COMPANYADDRESS3")==null?"":map.get("COMPANYADDRESS3").toString());
	    map.remove("CompanyAddress3");
	    //Co Pcode 单位邮编	Text	10
	    map.put("CompanyCity",map.get("COMPANYCITY")==null?"":map.get("COMPANYCITY").toString());
	    map.remove("COMPANYCITY");
	    //Co Ph Num 单位电话	Text	32
	    map.put("CompanyPhoneNumber",map.get("COMPANYPHONENUMBER")==null?"":map.get("COMPANYPHONENUMBER").toString());
	    map.remove("COMPANYPHONENUMBER");
	    //Channel 营销代码	Text	21
	    map.put("SalesCode",map.get("SALESCODE")==null?"":map.get("SALESCODE").toString());
	    map.remove("SALESCODE");
	    //Src of App 申请来源	Text	21
	    map.put("SourceCode",map.get("SOURCECODE")==null?"":map.get("SOURCECODE").toString());
	    map.remove("SOURCECODE");
	    //Trackcode	Text	70
	    map.put("Trackcode",map.get("TRACKCODE")==null?"":map.get("TRACKCODE").toString());
	    map.remove("TRACKCODE");
	    //CIM码	Text	40
	    map.put("CimCode",map.get("CIMCODE")==null?"":map.get("CIMCODE").toString());
	    map.remove("CIMCODE");
	    //COOKIE地址	Text	40
	    map.put("Cookie",map.get("COOKIE")==null?"":map.get("COOKIE").toString());
	    map.remove("COOKIE");
	    //证件号码(公安)	Text	25
	    //map.put("IDVALUE",map.get("IDVALUE")==null?"":map.get("IDVALUE").toString());
	    //证件类型(公安)	Text	25
	   // map.put("IDTYPE",map.get("IDTYPE")==null?"":map.get("IDTYPE").toString());
	    //配偶证件号码	Text	25
	    //map.put("SPOUSEIDNUMBER",map.get("SPOUSEIDNUMBER")==null?"":map.get("SPOUSEIDNUMBER").toString());
	    //app设备Id	Text	70
	    map.put("AppID",map.get("APPID")==null?"":map.get("APPID").toString());
	    map.remove("APPID");
	    //app手机设备标示码	Text	70
	    map.put("AppCode",map.get("APPCODE")==null?"":map.get("APPCODE").toString());
	    map.remove("APPCODE");
	    //app渠道uuid	Text	70
	    map.put("App_uuid",map.get("APP_UUID")==null?"":map.get("APP_UUID").toString());
	    map.remove("APP_UUID");
	    //IP地址	Text	40
	    //map.put("IP",map.get("IP")==null?"":map.get("IP").toString());
	    //审批结果	Text	10	通过，不通过
	    map.put("ApprovalResult",map.get("APPROVALRESULT")==null?"":map.get("APPROVALRESULT").toString());
	    map.remove("APPROVALRESULT");
	    //调查结论	Text	2	值：K,F,S,空
	    map.put("ResearchConclusion",map.get("RESEARCHCONCLUSION")==null?"":map.get("RESEARCHCONCLUSION").toString());
	    map.remove("RESEARCHCONCLUSION");
	    //报警代码	Text	2	值：H,S,C
	    map.put("AlarmCode",map.get("ALARMCODE")==null?"":map.get("ALARMCODE").toString());
	    map.remove("ALARMCODE");
	    //标识
	    map.put("flag",map.get("FLAG")==null?"":map.get("FLAG").toString());
	    map.remove("flag");
		apply.setData(map);
		return apply;
	}
	
	public static void main(String[] args) {
		Map<String,Object> map = RandomDataModelBuilder.getRandomDataModel(1234566L, "2019-10-10");
		System.out.println(map);
		Map<String,Object> retMap = new LinkedHashMap<String,Object>();
		for(Map.Entry<String, Object> ss : map.entrySet()) {
			String key = ss.getKey();
			Object value = ss.getValue();
			retMap.put(key.toUpperCase(), value);
			//map.remove(key);
		}
		System.out.println(retMap);
		retMap.put("MyDataType", "1");
		KafkaApplyCardEntity entity = processHisKafkaData(retMap);
        Gson gson = new Gson();
    	String jsonStr = gson.toJson(entity);
		System.out.println(jsonStr);
	}

}