package com.myboot.dataprocess.process.akka.server;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.myboot.dataprocess.common.MyConstants;
import com.myboot.dataprocess.model.result.ApiResponse;
import com.myboot.dataprocess.model.result.EvalResultDto;
import com.myboot.dataprocess.model.result.StatusEnum;

import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpResponse;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("deprecation")
public class MyAkkaHttpServerApi {
	
	private static Http http;
	private static Materializer materializer;
	  
	static {
	    ActorSystem system = ActorSystem.create("http");
	    materializer = ActorMaterializer.create(system);
	    http = Http.get(system);
	}

	public static void start() {
		    //CompletionStage<ServerBinding> binding = 
		    		http.bindAndHandleSync((req) -> {
			HttpEntity.Strict ss = null;
			try {
			 	ss = req.entity().toStrict(5000, materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);
			 	String result = ss.getData().utf8String();
			    log.info("http response result:"+ result);
				long startTime = System.currentTimeMillis();
				Gson gson = new Gson();
				Map<String,Object> map = gson.fromJson(result, new TypeToken<HashMap<String,Object>>(){}.getType());
		        if(map==null) {
		        	return null;
		        }
		        Optional<HttpHeader> reqApiOpt = req.getHeader("api");
				Optional<HttpHeader> reqModeOpt = req.getHeader("reqMode");
				Optional<HttpHeader> dinstModeOpt = req.getHeader("dinstMode");
				//Optional<HttpHeader> dataModeOpt = req.getHeader("dataMode");
				log.info("reqApiOpt:"+reqApiOpt.toString()+"&reqModeOpt:"+reqModeOpt.toString()+"&dinstModeOpt:"+dinstModeOpt.toString());
		        String reqNo = map.get("reqNo")==null?"":map.get("reqNo").toString();
				EvalResultDto payload = new EvalResultDto(reqNo);
				payload.setResultMap(map);
				ApiResponse<EvalResultDto> api = new ApiResponse<EvalResultDto>(StatusEnum.S, "1", "成功", payload);
				log.info("MyAkkaProcessController#call:receive content:"+ api);
				log.info("dinst call cost： "+(System.currentTimeMillis() -startTime )+"ms");
				return  HttpResponse.create().withEntity(gson.toJson(api));
			} catch (Exception e) {
				e.printStackTrace();
			}
			return HttpResponse.create().withEntity("");
		   }, ConnectHttp.toHost(MyConstants.HTTP_URL), materializer);
		   //ConnectHttp.toHost(MyConstants.HTTP_SERVER_IP, MyConstants.HTTP_SERVER_PORT)
	}

}
