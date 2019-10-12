package com.myboot.dataprocess.process.httpclient;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.myboot.dataprocess.tools.CommonTool;

import jline.internal.Log;

public class MyHttpClientProcess {
	
	public static String post(Map<String,Object> map) {
		// 获得Http客户端(可以理解为:你得先有一个浏览器;注意:实际上HttpClient与浏览器是不一样的)
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		Map<String,Object> newMap = new LinkedHashMap<String,Object>();
		newMap.putAll(map);
		Gson gson = new Gson();
		// 创建Post请求
		HttpPost httpPost = new HttpPost("http://182.180.115.236:13551/call");
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
			//从响应模型中获取响应实体
			HttpEntity responseEntity = response.getEntity();
			Log.info("response status:"+ response.getStatusLine() + "response content:" + EntityUtils.toString(responseEntity));
			return EntityUtils.toString(responseEntity);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				// 释放资源
				if (httpClient != null) {
					httpClient.close();
				}
				if (response != null) {
					response.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

}