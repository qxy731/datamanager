package com.myboot.dataprocess.process.ignite;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * IgniteProcess 
 * 将文件中的数据加载到ignite中
 * @author 2zhouliang2
 *
 */
public class IgniteProcess {
	
	private static final Logger LOG = LoggerFactory.getLogger(IgniteProcess.class);
	
	private static Gson gson = new Gson();
	// 扫描cache的key-value
	//private static IgniteCache<String, Object> region = GridEngine.regionRealtime();

	public static void main(String[] args) throws ParseException {
		
//		writeFile("F:\\poc\\file\\file20.txt","20");

	}
	
	/**
	 * 根据文件路径和文件编号解析文件
	 * @param filePath
	 * @param fileNo
	 */
	public static void writeFile(String filePath,String fileNo) {
		BufferedReader bfReader = null;
		try {
			File file = new File(filePath);
			if (!file.isFile()) {
				return;
			}
			//根据文件编号获取维度信息
			IgniteFilePojo pojo = beforeWrite(fileNo);
			if (pojo == null) {
				//文件编号不存在
				return;
			}
			IgniteFilePojo newPojo = new IgniteFilePojo();
			newPojo.setArr(pojo.getArr());
			String key1 = pojo.getKey1();//主维度
			String key2 = pojo.getKey2();//第二维度
			String value1 = "";//主维度对应的值
			String value2 = "";//第二维度对应的值
			List<Map<String,String>> mapList = new ArrayList<Map<String,String>>();
			bfReader = new BufferedReader(new FileReader(file));
			String line = "";
			boolean flag = true;
			while ((line = bfReader.readLine()) != null) {
				@SuppressWarnings("unchecked")
				Map<String,String> map = gson.fromJson(line, Map.class);
				if (StringUtils.isNotEmpty(key1)) {
					String value1_0 = map.get(key1);
					if (StringUtils.isNotEmpty(key2)) {
						String value2_0 = map.get(key2);
						if (StringUtils.isEmpty(value1)) {
							//第一次进
							mapList.add(map);
							value1 = value1_0;
						} else {
							//32 52
							if (value1.equals(value1_0) && value2.equals(value2_0)) {
								//继续读取下一行
								mapList.add(map);
							} else {
								//保存之前读取的行数据
								insertIgnite(mapList,newPojo,filePath,fileNo);
								//重置参数，继续读取下一行
								mapList = new ArrayList<Map<String,String>>();
								mapList.add(map);
								value1 = value1_0;
								value2 = value2_0;
								flag = true;
							}
						}
						if (flag) {
							//将ignite存储的key拼装完整
							String[] oldArr = pojo.getArr();
							String str0 = oldArr[0];
							str0 = str0.replaceAll("#key1#", value1_0) + value2_0;
							String str1 = oldArr[1];
							str1 = str1.replaceAll("#key1#", value1_0) + value2_0;
							String str2 = oldArr[2];
							str2 = str2.replaceAll("#key1#", value1_0) + value2_0;
							String[] newArr = {str0,str1,str2}; 
							newPojo.setArr(newArr);
							flag = false;
						}
					} else {
						if (StringUtils.isEmpty(value1)) {
							//第一次进
							mapList.add(map);
							value1 = value1_0;
						} else {
							//10 11 12 13 14 30 31 40 41 50 51 60 61 70 71
							if (value1.equals(value1_0)) {
								//继续读取下一行
								mapList.add(map);
							}else {
								//保存之前读取的行数据
								insertIgnite(mapList,newPojo,filePath,fileNo);
								//重置参数，继续读取下一行
								mapList = new ArrayList<Map<String,String>>();
								mapList.add(map);
								value1 = value1_0;
								flag = true;
							}
						}
						if (flag) {
							//将ignite存储的key拼装完整
							String[] oldArr = pojo.getArr();
							String str0 = oldArr[0];
							str0 = str0 + value1_0;
							String str1 = oldArr[1];
							str1 = str1 + value1_0;							
							String str2 = oldArr[2];
							str2 = str2 + value1_0;
							String[] newArr = {str0,str1,str2}; 
							newPojo.setArr(newArr);
							flag = false;
						}
					}
				} else {
					//20 21 22 23 24
					//整体营销员，全部读完再存
					mapList.add(map);
				}
			}
			//保存数据
			insertIgnite(mapList,newPojo,filePath,fileNo);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bfReader != null) {
				try {
					bfReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 解析数据存入ignite
	 * @param list
	 * @param pojo
	 */
	public static void insertIgnite(List<Map<String, String>> list,IgniteFilePojo pojo,String filePath,String fileNo) {
		List<ImmutablePair<String,Object>> dataList = new ArrayList<ImmutablePair<String,Object>>();
		String[] arr = pojo.getArr();//ignite中存储的key
		for (int i = 0; i < arr.length; i++) {
			Map<String, Object> igniteMap = new HashMap<String, Object>();
			Map<String, Integer> data = new HashMap<String, Integer>();
			int ver = 0;
			for (Map<String, String> dataMap : list) {
				//获取日期
				String applicationdate = dataMap.get("APPLICATIONDATE");
				applicationdate = applicationdate.replaceAll("-", "").substring(0, 8);
				//获取统计数
				String countStr = dataMap.get("COUNT");
				Integer count = Integer.valueOf(countStr);
				data.put(applicationdate, count);
				ver = ver + count;
			}
			igniteMap.put("ver", ver);
			igniteMap.put("data", data);
			ImmutablePair<String,Object> itp = new ImmutablePair<String,Object>(arr[i],igniteMap);
			dataList.add(itp);
		}
		
		writeDataTofile(dataList,filePath,fileNo);
//		System.out.println(dataList);
//		region.removeAll();
		// 遍历放入cache中
//		dataList.forEach(immutablePair -> {
//			region.putAsync((String) immutablePair.getLeft(), immutablePair.getRight());
//		});
	}
	
	/**
	 * 将数据写入文件中
	 * @param list
	 */
	public static void writeDataTofile(List<ImmutablePair<String,Object>> list,String filePath,String fileNo) {
//		String filePath1 = "/hdata/poctest3/file/newFile" + fileNo + ".txt";
		String filePath1 = "F:\\poc\\file\\newFile" + fileNo + ".txt";
		FileWriter fw = null;
		try {
			fw = new FileWriter(filePath1, true);
			for (ImmutablePair<String, Object> immutablePair : list) {
				String json = gson.toJson(immutablePair);
				fw.write(json + "\r\n");
				fw.flush();
			}
		} catch (Exception e) {
			LOG.error("写入文件出错！", e);
		} finally {
			try {
				if (fw != null) {
					fw.close();
				}
			} catch (IOException e) {
				LOG.error("流关闭异常！", e);
			}
		}
	}
	
	/**
	 * 读取文件中的数据存入ignite
	 * @param file
	 */
	public static void readerFileDataToIgnite(String filePath) {
		BufferedReader reader = null;
		try {
			File file = new File(filePath);
			if (!file.isFile()) {
				LOG.info("file is null");
				return;
			}
			reader = new BufferedReader(new FileReader(file));
			List<ImmutablePair<String,Object>> dataList = new ArrayList<ImmutablePair<String,Object>>();
			int count = 1;
			String line = "";
			while ((line = reader.readLine()) != null) {
				@SuppressWarnings("unchecked")
				ImmutablePair<String,Object> immutablePair = gson.fromJson(line, ImmutablePair.class);
				dataList.add(immutablePair);
				if (dataList.size() > 10000) {
					// 遍历放入cache中
					dataList.forEach(immutablePair1 -> {
						//region.putAsync((String) immutablePair1.getLeft(), immutablePair.getRight());
					});
					dataList.clear();
					System.out.println("已经存入： " + (count*10000) + " 条数据");
					count++;
				}
			}
			// 遍历放入cache中
			dataList.forEach(immutablePair -> {
				//region.putAsync((String) immutablePair.getLeft(), immutablePair.getRight());
			});
			
		} catch (Exception e) {
			LOG.error("读取文件中的数据存入ignite出错", e);
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				LOG.error("流关闭出错！", e);
			}
		}
	}
	
	/**
	 * 根据文件编号封装数据
	 * @param fileNo 文件编号
	 * @return
	 */
	public static IgniteFilePojo beforeWrite (String fileNo) {
		
		IgniteFilePojo pojo = new IgniteFilePojo();
		switch (fileNo) {
		case "10":
			pojo.setKey1("SALESCODE");
			String[] arr10 = {"sSMOneMthEnterCounts-count-his-SalesCode:","sSMThreeMthEnterCounts-count-his-SalesCode:","sSMSixMthEnterCounts-count-his-SalesCode:"};
			pojo.setArr(arr10);
			break;
		case "11":
			pojo.setKey1("SALESCODE");
			String[] arr11 = {"sSMOneMthProCounts-count-his-SalesCode:","sSMThreeMthProCounts-count-his-SalesCode:","sSMSixMthProCounts-count-his-SalesCode:"};
			pojo.setArr(arr11);
			break;
		case "12":
			pojo.setKey1("SALESCODE");
			String[] arr12 = {"sSMOneMthWarningRate1-count-his-SalesCode:","sSMThreeMthWarningRate1-count-his-SalesCode:","sSMSixMthWarningRate1-count-his-SalesCode:"};
			pojo.setArr(arr12);
			break;
		case "13":
			pojo.setKey1("SALESCODE");
			String[] arr13 = {"sSMOneMthHitRate1-count-his-SalesCode:","sSMThreeMthHitRate1-count-his-SalesCode:","sSMSixMthHitRate1-count-his-SalesCode:"};
			pojo.setArr(arr13);
			break;
		case "14":
			pojo.setKey1("SALESCODE");
			String[] arr14 = {"sSMOneMthHitRate2-count-his-SalesCode:","sSMThreeMthHitRate2-count-his-SalesCode:","sSMSixMthHitRate2-count-his-SalesCode:"};
			pojo.setArr(arr14);
			break;
		case "20":
//			pojo.setKey1("SALESCODE");
			String[] arr20 = {"sTOneMthEnterCounts-count-his-MyAppDataType:1","sTThreeMthEnterCounts-count-his-MyAppDataType:1","sTSixMthEnterCounts-count-his-MyAppDataType:1"};
			pojo.setArr(arr20);
			break;
		case "21":
//			pojo.setKey1("SALESCODE");
			String[] arr21 = {"sTOneMthProCounts-count-his-MyAppDataType:1","sTThreeMthProCounts-count-his-MyAppDataType:1","sTSixMthProCounts-count-his-MyAppDataType:1"};
			pojo.setArr(arr21);
			break;
		case "22":
//			pojo.setKey1("SALESCODE");
			String[] arr22 = {"sTOneMthWarningRate1-count-his-MyAppDataType:1","sTThreeMthWarningRate1-count-his-MyAppDataType:1","sTSixMthWarningRate1-count-his-MyAppDataType:1"};
			pojo.setArr(arr22);
			break;
		case "23":
//			pojo.setKey1("SALESCODE");
			String[] arr23 = {"sTOneMthHitRate1-count-his-MyAppDataType:1","sTThreeMthHitRate1-count-his-MyAppDataType:1","sTSixMthHitRate1-count-his-MyAppDataType:1"};
			pojo.setArr(arr23);
			break;
		case "24":
//			pojo.setKey1("SALESCODE");
			String[] arr24 = {"sTOneMthHitRate2-count-his-MyAppDataType:1","sTThreeMthHitRate2-count-his-MyAppDataType:1","sTSixMthHitRate2-count-his-MyAppDataType:1"};
			pojo.setArr(arr24);
			break;
		case "30":
			pojo.setKey1("COMPANYPHONENUMBER");
			String[] arr30 = {"sCPFiveEnterCounts-count-his-CompanyPhoneNumber:","sCPFifteenEnterCounts-count-his-CompanyPhoneNumber:","sCPThirtyEnterCounts-count-his-CompanyPhoneNumber:"};
			pojo.setArr(arr30);
			break;
		case "31":
			pojo.setKey1("COMPANYPHONENUMBER");
			String[] arr31 = {"sCPFiveFraudCounts-count-his-CompanyPhoneNumber:","sCPFifteenFraudCounts-count-his-CompanyPhoneNumber:","sCPThirtyFraudCounts-count-his-CompanyPhoneNumber:"};
			pojo.setArr(arr31);
			break;
		case "32":
			pojo.setKey1("COMPANYPHONENUMBER");
			pojo.setKey2("COMPANYNAME");
			String[] arr32 = {"sCPFiveDCEnterCounts1-count-his-CompanyPhoneNumber:#key1#CompanyName:","sCPFifteenDCEnterCounts1-count-his-CompanyPhoneNumber:#key1#CompanyName:","sCPThirtyDCEnterCounts1-count-his-CompanyPhoneNumber:#key1#CompanyName:"};
			pojo.setArr(arr32);
			break;
		case "40":
			pojo.setKey1("COMPANYNAME");
			String[] arr40 = {"sCFiveEnterCounts-count-his-CompanyName:","sCFifteenEnterCounts-count-his-CompanyName:","sCThirtyEnterCounts-count-his-CompanyName:"};
			pojo.setArr(arr40);
			break;
		case "41":
			pojo.setKey1("COMPANYNAME");
			String[] arr41 = {"sCFiveFraudCounts-count-his-CompanyName:","sCFifteenFraudCounts-count-his-CompanyName:","sCThirtyFraudCounts-count-his-CompanyName:"};
			pojo.setArr(arr41);
			break;
		case "50":
			pojo.setKey1("IP");
			String[] arr50 = {"sIPFiveEnterCounts-count-his-IP:","sIPFifteenEnterCounts-count-his-IP:","sIPThirtyEnterCounts-count-his-IP:"};
			pojo.setArr(arr50);
			break;
		case "51":
			pojo.setKey1("IP");
			String[] arr51 = {"sIPFiveFraudCounts-count-his-IP:","sIPFifteenFraudCounts-count-his-IP:","sIPThirtyFraudCounts-count-his-IP:"};
			pojo.setArr(arr51);
			break;
		case "52":
			pojo.setKey1("IP");
			pojo.setKey2("COMPANYPHONENUMBER");
			String[] arr52 = {"sIPFiveDCEnterCounts1-count-his-IP:#key1#CompanyPhoneNumber:","sIPFifteenDCEnterCounts1-count-his-IP:#key1#CompanyPhoneNumber:","sIPThirtyDCEnterCounts1-count-his-IP:#key1#CompanyPhoneNumber:"};
			pojo.setArr(arr52);
			break;
		case "60":
			pojo.setKey1("APPID");
			String[] arr60 = {"sDFiveEnterCounts-count-his-AppID:","sDFifteenEnterCounts-count-his-AppID:","sDThirtyEnterCounts-count-his-AppID:"};
			pojo.setArr(arr60);
			break;
		case "61":
			pojo.setKey1("APPID");
			String[] arr61 = {"sDFiveFraudCounts-count-his-AppID:","sDFifteenFraudCounts-count-his-AppID:","sDThirtyFraudCounts-count-his-AppID:"};
			pojo.setArr(arr61);
			break;
		case "70":
			pojo.setKey1("COOKIE");
			String[] arr70 = {"sCkFiveEnterCounts-count-his-Cookie:","sCkFifteenEnterCounts-count-his-Cookie:","sCkThirtyEnterCounts-count-his-Cookie:"};
			pojo.setArr(arr70);
			break;
		case "71":
			pojo.setKey1("COOKIE");
			String[] arr71 = {"sCkFiveFraudCounts-count-his-Cookie:","sCkFifteenFraudCounts-count-his-Cookie:","sCkThirtyFraudCounts-count-his-AppCookieID:"};
			pojo.setArr(arr71);
			break;

		default:
			return null;
		}
		return pojo;
	}
	
	
	
	/**
	 * 返回两个日期之间时间 按天 (格式：yyyyMMdd)
	 * @param starttime 开始日期  (格式：yyyyMMdd)
	 * @param endtime   结束日期  (格式：yyyyMMdd)
	 * @return
	 */
	public static List<String> getBetweenTime(String starttime,String endtime)
    {
    	List<String> betweenTime = new ArrayList<String>();
    	try
    	{
	    	Date sdate= new SimpleDateFormat("yyyyMMdd").parse(starttime);
	    	Date edate= new SimpleDateFormat("yyyyMMdd").parse(endtime);
	    	
	    	SimpleDateFormat outformat = new SimpleDateFormat("yyyyMMdd");
	        
	    	Calendar sCalendar = Calendar.getInstance();
	    	sCalendar.setTime(sdate);
	    	int year = sCalendar.get(Calendar.YEAR);
	        int month = sCalendar.get(Calendar.MONTH);
	        int day = sCalendar.get(Calendar.DATE);
	        sCalendar.set(year, month, day, 0, 0, 0);
	        
	        Calendar eCalendar = Calendar.getInstance();
	        eCalendar.setTime(edate);
	        year = eCalendar.get(Calendar.YEAR);
	        month = eCalendar.get(Calendar.MONTH);
	        day = eCalendar.get(Calendar.DATE);
	        eCalendar.set(year, month, day, 0, 0, 0);
	        
	        while (sCalendar.before(eCalendar))
	        {
	        	betweenTime.add(outformat.format(sCalendar.getTime()));
	        	sCalendar.add(Calendar.DAY_OF_YEAR, 1);
	        }
	        betweenTime.add(outformat.format(eCalendar.getTime()));
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
        return betweenTime;
    }
	
	
}
