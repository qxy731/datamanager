package com.myboot.dataprocess.process.akka.server;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.myboot.dataprocess.model.result.ApiResponse;
import com.myboot.dataprocess.model.result.EvalResultDto;
import com.myboot.dataprocess.model.result.StatusEnum;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

@Api
@Slf4j
@RestController
public class MyHttpClientServerApi {
	
	@ApiOperation(value="接收kafka推送数据", notes="接收kafka推送数据")
	@RequestMapping(value = "/call", method = {RequestMethod.POST,RequestMethod.GET})
	public String call(@RequestBody String result,HttpServletResponse response) {
	    log.info("MyAkkaProcessController#call:receive content:"+ result);
		long startTime = System.currentTimeMillis();
		Gson gson = new Gson();
		Map<String,Object> map = gson.fromJson(result, new TypeToken<HashMap<String,Object>>(){}.getType());
        if(map==null) {
        	return null;
        }
        String reqNo = map.get("reqNo")==null?"":map.get("reqNo").toString();
		EvalResultDto payload = new EvalResultDto(reqNo);
		payload.setResultMap(map);
		ApiResponse<EvalResultDto> api = new ApiResponse<EvalResultDto>(StatusEnum.S, "1", "成功", payload);
		log.info("MyAkkaProcessController#call:receive content:"+ api);
		log.info("dinst call cost： "+(System.currentTimeMillis() -startTime )+"ms");
		response.setStatus(200);
        return gson.toJson(api);
	}

}