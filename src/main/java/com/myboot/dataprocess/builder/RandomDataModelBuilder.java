package com.myboot.dataprocess.builder;

import java.util.LinkedHashMap;
import java.util.Map;

import com.myboot.dataprocess.builder.enums.AlarmCode;
import com.myboot.dataprocess.builder.enums.ApprovalResult;
import com.myboot.dataprocess.builder.enums.ResearchConclusion;
import com.myboot.dataprocess.builder.enums.Sex;
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
	
	/*private String[] str = {"SalesCode:5556","ResearchConclusion:{'K','F'}","ApprovalResult:通过",
			"SourceCode:{8,P}","AlarmCode:('H','S')",""
			};*/
	
	/*public static ApplyCardEntity getSpecialDataModel(long sequence,String currentDate) {
		ApplyCardEntity entity = new ApplyCardEntity();
		//申请编号
	    entity.setApplicationNumber(currentDate.replaceAll("-", "")+sequence);
	    //获取数据日期	Date
	    entity.setCaptureDate(CommonTool.getCurrentDate());
	    //获取数据时间	Numeric
	    entity.setCaptureTime(CommonTool.getCurrentTime());
	    //中止日期	Date
	    entity.setExpiryDate(CommonTool.getCurrentDate());
	    //申请日	Date
	    entity.setApplicationDate(currentDate);
	    //申请类型 Text
	    entity.setApplicationType("CARD");
	    //信用额度	Numeric
	    entity.setAmount(RandomNumberBuilder.getRangeRandom2Str(3000,100000));
	    //分行号	Text
	    entity.setBranch(RandomNumberBuilder.getRangeRandom2Str(10,100));
	    //处理结果	Text
	    String decision = RandomNumberBuilder.getRangeRandom2Str(1,4);
	    entity.setDecision(decision);
	    //处理原因	Text
	    if("3".equals(decision)) {
	    	entity.setDecisionReason(RandomCharacterBuilder.getRandomStr(10));
	    }else {
	    	entity.setDecisionReason("");
	    }
	    //处理日期	Date
	    if("3".equals(decision)) {
	    	 entity.setDecisionDate(CommonTool.addDay(currentDate,RandomNumberBuilder.getRangeRandom2Int(1,4)));
	    }else {
	    	entity.setDecisionDate("");
	    }
	    //Id Num 证件号码	Text
	    String certNo = RandomCertNoBuilder.getRandomCertNo();
	    entity.setCertificateID(certNo);
	    //Id Type 证件种类	Text
	    entity.setCertificateType("10");
	    //Full Name 主卡姓名	Text
	    entity.setSurname(RandomNameBuilder.build());
	    //NameonCard 压花名	Text
	    entity.setFirstName(RandomNameBuilder.getFirstName());
	    //中间名	Text
	    entity.setMiddleName(RandomNameBuilder.getMiddleName());
	    //Sex 性别	Text
	    entity.setSex(Sex.randomType().code);
	    //Date of Birth 出生日期	Date
	    entity.setDateofBirth(CommonTool.getBirthByCertNo(certNo));
	    int index = RandomAddressBuilder.getRandomProvinceIndex();
    	String province = RandomAddressBuilder.getRandomProvince(index);
    	String city = RandomAddressBuilder.getRandomCity(index);
    	String address = RandomAddressBuilder.getRandomAddress(province,city);
	    //Hm Addr1 住宅地址1	Text
	    entity.setHomeAddress(address);
	    //Hm City 住宅城市	Text
	    entity.setInhabitCity(city);  
	    //HmPcode 住宅邮编	Text
	    entity.setHomePostcode(RandomNumberBuilder.getRangeRandom2Str(1000000,9000000));
	    //HmPhNum 住宅电话	Text
	    entity.setHomePhoneNumber(RandomPhoneBuilder.getRandomTel());
	    //Mobile 手机号码	Text	32
	    entity.setMobilePhoneNumber(RandomPhoneBuilder.getRandomTel());
	    //Co Name 单位名称	Text	70
	    entity.setCompanyName(RandomCompayNameBuilder.getRandomCompayName());
	    //Co Addr1 单位地址1	Text	40
	    entity.setOfficeAddress1(RandomAddressBuilder.getRandomAddress(province,city));
	    //Co Addr2 单位地址2	Text	40
	    entity.setOfficeAddress2(" ");
	    //Co City 单位城市	Text	40
	    entity.setCompanyAddress3(" ");
	    //Co Pcode 单位邮编	Text	10
	    entity.setCompanyCity(city);
	    //Co Ph Num 单位电话	Text	32
	    entity.setCompanyPhoneNumber(RandomPhoneBuilder.getRandomTel());
	    //Channel 营销代码	Text	21
	    entity.setSalesCode("5556");
	    //Src of App 申请来源	Text	21
	    entity.setSourceCode(RandomCharacterBuilder.getRandomNumOrChar(1));
	    //Trackcode	Text	70
	    entity.setTrackcode(RandomNumberBuilder.getRangeRandom2Str(1000,9999));
	    //CIM码	Text	40
	    entity.setCimCode(RandomNumberBuilder.getRandomGuid(16));
	    //COOKIE地址	Text	40
	    entity.setCookie(RandomIpBuilder.getRandomIp());
	    //证件号码(公安)	Text	25
	    entity.setIdvalue(certNo);
	    //证件类型(公安)	Text	25
	    entity.setIdtype(RandomNumberBuilder.getRangeRandom2Str(10,30));
	    //配偶证件号码	Text	25
	    entity.setSpouseidnumber(RandomCertNoBuilder.getRandomCertNo());
	    //app设备Id	Text	70
	    entity.setAppID(RandomNumberBuilder.getRandomGuid(17));
	    //app手机设备标示码	Text	70
	    entity.setAppCode(RandomNumberBuilder.getRandomGuid(14));
	    //app渠道uuid	Text	70
	    entity.setApp_uuid(RandomNumberBuilder.getRandomGuid(16));
	    //IP地址	Text	40
	    entity.setIp(RandomIpBuilder.getRandomIp());
	    //审批结果	Text	10	通过，不通过
	    entity.setApprovalResult(ApprovalResult.randomType().code);
	    //调查结论	Text	2	值：K,F,S,空
	    entity.setResearchConclusion(ResearchConclusion.randomType().code);
	    //报警代码	Text	2	值：H,S,C
	    entity.setAlarmCode(AlarmCode.randomType().code);
	    return entity;
	}*/

}