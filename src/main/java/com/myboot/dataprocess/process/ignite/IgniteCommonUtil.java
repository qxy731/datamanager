package com.myboot.dataprocess.process.ignite;

import java.util.LinkedHashMap;
import java.util.Map;

public class IgniteCommonUtil {
	
	/**
	 * 根据文件编号封装数据
	 * 
	 * @param fileNo 文件编号
	 * @return
	 */
	public static IgniteFilePojo getDerivedVariableKey(String fileNo) {
		IgniteFilePojo pojo = new IgniteFilePojo();
		pojo.setFileNo(fileNo);
		switch (fileNo) {
		case "10":
			pojo.setKey1("SALESCODE");
			pojo.setDimension(1);
			String[] arr10 = { "sSMOneMthEnterCounts-count-his-SalesCode:",
					"sSMThreeMthEnterCounts-count-his-SalesCode:", "sSMSixMthEnterCounts-count-his-SalesCode:" };
			pojo.setArr(arr10);
			break;
		case "11":
			pojo.setKey1("SALESCODE");
			pojo.setDimension(1);
			String[] arr11 = { "sSMOneMthProCounts-count-his-SalesCode:", "sSMThreeMthProCounts-count-his-SalesCode:",
					"sSMSixMthProCounts-count-his-SalesCode:" };
			pojo.setArr(arr11);
			break;
		case "12":
			pojo.setKey1("SALESCODE");
			pojo.setDimension(1);
			String[] arr12 = { "sSMOneMthWarningRate1-count-his-SalesCode:",
					"sSMThreeMthWarningRate1-count-his-SalesCode:", "sSMSixMthWarningRate1-count-his-SalesCode:" };
			pojo.setArr(arr12);
			break;
		case "13":
			pojo.setKey1("SALESCODE");
			pojo.setDimension(1);
			String[] arr13 = { "sSMOneMthHitRate1-count-his-SalesCode:", "sSMThreeMthHitRate1-count-his-SalesCode:",
					"sSMSixMthHitRate1-count-his-SalesCode:" };
			pojo.setArr(arr13);
			break;
		case "14":
			pojo.setKey1("SALESCODE");
			pojo.setDimension(1);
			String[] arr14 = { "sSMOneMthHitRate2-count-his-SalesCode:", "sSMThreeMthHitRate2-count-his-SalesCode:",
					"sSMSixMthHitRate2-count-his-SalesCode:" };
			pojo.setArr(arr14);
			break;
		case "20":
//			pojo.setKey1("SALESCODE");
			pojo.setDimension(0);
			String[] arr20 = { "sTOneMthEnterCounts-count-his-MyAppDataType:1",
					"sTThreeMthEnterCounts-count-his-MyAppDataType:1",
					"sTSixMthEnterCounts-count-his-MyAppDataType:1" };
			pojo.setArr(arr20);
			break;
		case "21":
//			pojo.setKey1("SALESCODE");
			pojo.setDimension(0);
			String[] arr21 = { "sTOneMthProCounts-count-his-MyAppDataType:1",
					"sTThreeMthProCounts-count-his-MyAppDataType:1", "sTSixMthProCounts-count-his-MyAppDataType:1" };
			pojo.setArr(arr21);
			break;
		case "22":
//			pojo.setKey1("SALESCODE");
			pojo.setDimension(0);
			String[] arr22 = { "sTOneMthWarningRate1-count-his-MyAppDataType:1",
					"sTThreeMthWarningRate1-count-his-MyAppDataType:1",
					"sTSixMthWarningRate1-count-his-MyAppDataType:1" };
			pojo.setArr(arr22);
			break;
		case "23":
//			pojo.setKey1("SALESCODE");
			pojo.setDimension(0);
			String[] arr23 = { "sTOneMthHitRate1-count-his-MyAppDataType:1",
					"sTThreeMthHitRate1-count-his-MyAppDataType:1", "sTSixMthHitRate1-count-his-MyAppDataType:1" };
			pojo.setArr(arr23);
			break;
		case "24":
//			pojo.setKey1("SALESCODE");
			pojo.setDimension(0);
			String[] arr24 = { "sTOneMthHitRate2-count-his-MyAppDataType:1",
					"sTThreeMthHitRate2-count-his-MyAppDataType:1", "sTSixMthHitRate2-count-his-MyAppDataType:1" };
			pojo.setArr(arr24);
			break;
		case "30":
			pojo.setKey1("COMPANYPHONENUMBER");
			pojo.setDimension(1);
			String[] arr30 = { "sCPFiveEnterCounts-count-his-CompanyPhoneNumber:",
					"sCPFifteenEnterCounts-count-his-CompanyPhoneNumber:",
					"sCPThirtyEnterCounts-count-his-CompanyPhoneNumber:" };
			pojo.setArr(arr30);
			break;
		case "31":
			pojo.setKey1("COMPANYPHONENUMBER");
			pojo.setDimension(1);
			String[] arr31 = { "sCPFiveFraudCounts-count-his-CompanyPhoneNumber:",
					"sCPFifteenFraudCounts-count-his-CompanyPhoneNumber:",
					"sCPThirtyFraudCounts-count-his-CompanyPhoneNumber:" };
			pojo.setArr(arr31);
			break;
		case "32":
			pojo.setKey1("COMPANYPHONENUMBER");
			pojo.setKey2("COMPANYNAME");
			pojo.setDimension(2);
			String[] arr32 = { "sCPFiveDCEnterCounts1-count-his-CompanyPhoneNumber:#key1#CompanyName:",
					"sCPFifteenDCEnterCounts1-count-his-CompanyPhoneNumber:#key1#CompanyName:",
					"sCPThirtyDCEnterCounts1-count-his-CompanyPhoneNumber:#key1#CompanyName:" };
			pojo.setArr(arr32);
			break;
		case "40":
			pojo.setKey1("COMPANYNAME");
			pojo.setDimension(1);
			String[] arr40 = { "sCFiveEnterCounts-count-his-CompanyName:",
					"sCFifteenEnterCounts-count-his-CompanyName:", "sCThirtyEnterCounts-count-his-CompanyName:" };
			pojo.setArr(arr40);
			break;
		case "41":
			pojo.setKey1("COMPANYNAME");
			pojo.setDimension(1);
			String[] arr41 = { "sCFiveFraudCounts-count-his-CompanyName:",
					"sCFifteenFraudCounts-count-his-CompanyName:", "sCThirtyFraudCounts-count-his-CompanyName:" };
			pojo.setArr(arr41);
			break;
		case "50":
			pojo.setKey1("IP");
			pojo.setDimension(1);
			String[] arr50 = { "sIPFiveEnterCounts-count-his-IP:", "sIPFifteenEnterCounts-count-his-IP:",
					"sIPThirtyEnterCounts-count-his-IP:" };
			pojo.setArr(arr50);
			break;
		case "51":
			pojo.setKey1("IP");
			pojo.setDimension(1);
			String[] arr51 = { "sIPFiveFraudCounts-count-his-IP:", "sIPFifteenFraudCounts-count-his-IP:",
					"sIPThirtyFraudCounts-count-his-IP:" };
			pojo.setArr(arr51);
			break;
		case "52":
			pojo.setKey1("IP");
			pojo.setKey2("COMPANYPHONENUMBER");
			pojo.setDimension(2);
			String[] arr52 = { "sIPFiveDCEnterCounts1-count-his-IP:#key1#CompanyPhoneNumber:",
					"sIPFifteenDCEnterCounts1-count-his-IP:#key1#CompanyPhoneNumber:",
					"sIPThirtyDCEnterCounts1-count-his-IP:#key1#CompanyPhoneNumber:" };
			pojo.setArr(arr52);
			break;
		case "60":
			pojo.setKey1("APPID");
			pojo.setDimension(1);
			String[] arr60 = { "sDFiveEnterCounts-count-his-AppID:", "sDFifteenEnterCounts-count-his-AppID:",
					"sDThirtyEnterCounts-count-his-AppID:" };
			pojo.setArr(arr60);
			break;
		case "61":
			pojo.setKey1("APPID");
			pojo.setDimension(1);
			String[] arr61 = { "sDFiveFraudCounts-count-his-AppID:", "sDFifteenFraudCounts-count-his-AppID:",
					"sDThirtyFraudCounts-count-his-AppID:" };
			pojo.setArr(arr61);
			break;
		case "70":
			pojo.setKey1("COOKIE");
			pojo.setDimension(1);
			String[] arr70 = { "sCkFiveEnterCounts-count-his-Cookie:", "sCkFifteenEnterCounts-count-his-Cookie:",
					"sCkThirtyEnterCounts-count-his-Cookie:" };
			pojo.setArr(arr70);
			break;
		case "71":
			pojo.setKey1("COOKIE");
			pojo.setDimension(1);
			String[] arr71 = { "sCkFiveFraudCounts-count-his-Cookie:", "sCkFifteenFraudCounts-count-his-Cookie:",
					"sCkThirtyFraudCounts-count-his-AppCookieID:" };
			pojo.setArr(arr71);
			break;

		default:
			return null;
		}
		return pojo;
	}
	
	/**
	 * 根据文件名称获取文件编号
	 * 
	 * @param fileName 文件名称 如：file10、file11
	 * @return
	 */
	public static String getFileNo(String fileName) {
		return fileName.replaceAll("file", "");
	}
	
	public static Map<String,String> getDataQuerySQL(String[] fileNames) {
		Map<String,String> sqlMap = new LinkedHashMap<String,String>();
		sqlMap.put("10", "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,SALESCODE");
		sqlMap.put("11", "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE APPROVALRESULT='通过' GROUP BY APPLICATIONDATE,SALESCODE");
		sqlMap.put("12", "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE ALARMCODE IN ('H','S') GROUP BY APPLICATIONDATE,SALESCODE");
		sqlMap.put("13", "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,SALESCODE");
		sqlMap.put("14", "SELECT APPLICATIONDATE,SALESCODE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION IN('K','F') GROUP BY APPLICATIONDATE,SALESCODE");
		sqlMap.put("20", "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE SOURCECODE IN ('8','P')  GROUP BY APPLICATIONDATE");
		sqlMap.put("21", "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE SOURCECODE IN ('8','P') AND APPROVALRESULT='通过' GROUP BY APPLICATIONDATE");
		sqlMap.put("22", "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE SOURCECODE IN ('8','P') AND ALARMCODE IN ('H','S') GROUP BY APPLICATIONDATE ");
		sqlMap.put("23", "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE SOURCECODE IN ('8','P') AND RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE ");
		sqlMap.put("24", "SELECT APPLICATIONDATE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE SOURCECODE IN ('8','P') AND RESEARCHCONCLUSION IN('K','F') GROUP BY APPLICATIONDATE ");
		sqlMap.put("30", "SELECT APPLICATIONDATE,COMPANYPHONENUMBER,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,COMPANYPHONENUMBER");
		sqlMap.put("31", "SELECT APPLICATIONDATE,COMPANYPHONENUMBER,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,COMPANYPHONENUMBER");
		sqlMap.put("32", "SELECT APPLICATIONDATE,COMPANYPHONENUMBER,COMPANYNAME,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,COMPANYPHONENUMBER,COMPANYNAME");
		sqlMap.put("40", "SELECT APPLICATIONDATE,COMPANYNAME,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,COMPANYNAME");
		sqlMap.put("41", "SELECT APPLICATIONDATE,COMPANYNAME,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,COMPANYNAME");
		sqlMap.put("50", "SELECT APPLICATIONDATE,IP,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,IP");
		sqlMap.put("51", "SELECT APPLICATIONDATE,IP,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,IP");
		sqlMap.put("52", "SELECT APPLICATIONDATE,IP,COMPANYPHONENUMBER,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,IP,COMPANYPHONENUMBER");
		sqlMap.put("60", "SELECT APPLICATIONDATE,APPID,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,APPID");
		sqlMap.put("61", "SELECT APPLICATIONDATE,APPID,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,APPID");
		sqlMap.put("70", "SELECT APPLICATIONDATE,COOKIE,COUNT(1) AS COUNT FROM POCTEST3_DEST GROUP BY APPLICATIONDATE,COOKIE");
		sqlMap.put("71", "SELECT APPLICATIONDATE,COOKIE,COUNT(1) AS COUNT FROM POCTEST3_DEST WHERE RESEARCHCONCLUSION='K' GROUP BY APPLICATIONDATE,COOKIE");		
		if(fileNames == null ) {
			return sqlMap;
		}
		Map<String,String> retMap = new LinkedHashMap<String,String>();
		for(int i=0;i<fileNames.length;i++) {
			String fileNo = getFileNo(fileNames[i]);
			retMap.put(fileNo,sqlMap.get(fileNo));
		}
		return retMap;
	}

}
