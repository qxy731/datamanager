package com.myboot.dataprocess.process.httpclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.myboot.dataprocess.tools.CommonTool;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyHttpClientProcess {
	
	public static String post(Map<String,Object> map) {
		// 获得Http客户端(可以理解为:你得先有一个浏览器;注意:实际上HttpClient与浏览器是不一样的)
		CloseableHttpClient httpClient = HttpClientUtil.getHttpClient();
		Map<String,Object> newMap = new LinkedHashMap<String,Object>();
		newMap.putAll(map);
		Gson gson = new Gson();
		// 创建Post请求
		HttpPost httpPost = new HttpPost("http://182.180.115.236:13551/call");
		RequestConfig requestConfig = RequestConfig.custom()
			      .setSocketTimeout(2000)//数据传输过程中数据包之间间隔的最大时间
			      .setConnectTimeout(2000)//连接建立时间，三次握手完成时间
			      .setExpectContinueEnabled(true)//重点参数 
			      .setConnectionRequestTimeout(2000)
			      .build();
		httpPost.setConfig(requestConfig);
		String reqNo = UUID.randomUUID().toString();
		newMap.put("reqNo",reqNo);
		String applicationDate = map.get("ApplicationDate")==null?CommonTool.getCurrentDate():map.get("ApplicationDate").toString();
		newMap.put("FLAT_TRAD_DATE_TIME", applicationDate + " 00:00:00");
		String jsonString = gson.toJson(map);
		StringEntity entity = new StringEntity(jsonString, "UTF-8");
		// post请求是将参数放在请求体里面传过去的;这里将entity放入post请求体中
		httpPost.setEntity(entity);
		httpPost.setHeader("Content-Type", "application/json;charset=utf8");
		//httpPost.setHeader("reqTimeMs",System.currentTimeMillis()+"");
		httpPost.setHeader("api","tradErm02");
		httpPost.setHeader("reqMode", "slow");
		httpPost.setHeader("dinstMode", "his");
		httpPost.setHeader("dataMode", "real_data");
		// 响应模型
		CloseableHttpResponse response = null;
		try {
			//由客户端执行(发送)Post请求
			response = httpClient.execute(httpPost);
			if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				log.info("httpclient request is fail ...");
				return  null;
			}
			//从响应模型中获取响应实体
			HttpEntity resEntity =  response.getEntity();
		    if(resEntity==null){
		        return  null;
		    }
		    String result=EntityUtils.toString(resEntity, "UTF-8");
		    log.info("response status:"+ response.getStatusLine() + "response content:" + EntityUtils.toString(resEntity));
			return result;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 释放资源
			if (httpClient != null) {
				try {
					httpClient.close();
				} catch (IOException e) {
			         //System.out.println("关闭response失败:"+ e);
			    }
			}
			if (response != null) {
				try {
					//此处调优重点，多线程模式下可提高性能。
					//此处高能，通过源码分析，由EntityUtils是否回收HttpEntity
			        EntityUtils.consume(response.getEntity());
			        response.close();
			     } catch (IOException e) {
			         //System.out.println("关闭response失败:"+ e);
			     }
			}
		}
		return null;
	}
	
	
	public static Map<String, Object> processResultContent(String resultJsonStr){
		Gson gson = new Gson();
		Map<String,Object> map = gson.fromJson(resultJsonStr, new TypeToken<HashMap<String,Object>>(){}.getType());
        if(map==null)return null;
		@SuppressWarnings("unchecked")
		Map<String,Object> payload = (Map<String,Object>)map.get("payload");
		if(payload==null)return null;
		@SuppressWarnings("unchecked")
		Map<String,Object> resultMap = (Map<String,Object>)payload.get("resultMap");
		if(resultMap==null)return null;
		return resultMap;
	}

}