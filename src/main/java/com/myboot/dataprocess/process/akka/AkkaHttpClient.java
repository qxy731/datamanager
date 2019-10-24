package com.myboot.dataprocess.process.akka;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.myboot.dataprocess.common.MyConstants;

import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.ResponseEntity;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("deprecation")
public class AkkaHttpClient {
	
	  private static Http http;
	  private static Materializer materializer;
	  
	  static {
	    ActorSystem system = ActorSystem.create("http");
	    materializer = ActorMaterializer.create(system);
	    http = Http.get(system);
	  }
	  
	  public static String post(Map<String,Object> map) {
		  Gson gson = new Gson();
		  map.put("reqNo",UUID.randomUUID().toString());
		  String jsonStr = gson.toJson(map);
		  HttpRequest request = HttpRequest.POST(MyConstants.HTTP_URL).withEntity(jsonStr);
		  request.addHeader(HttpHeader.parse("Content-Type", "application/json;charset=utf8"));
		  request.addHeader(HttpHeader.parse("reqTimeMs", System.currentTimeMillis()+""));
		  request.addHeader(HttpHeader.parse("api", "tradErm02"));
		  request.addHeader(HttpHeader.parse("reqMode", "slow"));
		  request.addHeader(HttpHeader.parse("dinstMode", "his"));
		  request.addHeader(HttpHeader.parse("dataMode", "real_data"));
		  try {
			      HttpResponse response = http.singleRequest(request, materializer).toCompletableFuture().get(15, TimeUnit.SECONDS);
			      if(response == null) {
			    	  return null;
			      }
			      ResponseEntity resEntity =  response.entity();
			      if(resEntity == null) {
			    	  return null;
			      }
			      HttpEntity.Strict s = resEntity.toStrict(5000, materializer).toCompletableFuture().get(5, TimeUnit.SECONDS); 
			      String result = s.getData().utf8String();
			      log.info("http response result:"+ result);
				  return result;
		    } catch (Exception e) {
		      // 1.客户端记录异常
		    	log.error(e.getMessage());
		    }
		  return null;
	  }
	  
	  public static void main(String[] args) throws Exception {
		  Map<String,Object> mapMessage = new HashMap<String,Object>();
		  mapMessage.put(MyConstants.MY_START_DATE_NAME,"");
		  mapMessage.put(MyConstants.MY_END_DATE_NAME,"");
		  mapMessage.put(MyConstants.MY_DATA_FLAG_NAME, MyConstants.MY_DATA_FLAG_REAL);
		  mapMessage.put(MyConstants.FLAT_TRAD_DATE_TIME_NAME,MyConstants.FLAT_TRAD_DATE_TIME_DEFAULT);
		  mapMessage.put(MyConstants.MY_APP_DATA_TYPE_NAME,MyConstants.MY_APP_DATA_TYPE_DEFAULT);
		  post(mapMessage);
		  
	  }
	  
	  
	  
}