package com.myboot.dataprocess.process.akka;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
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
public class MyAkkaProcessController {
	
	@ApiOperation(value="Kafka推送数据", notes="Kafka推送数据")
	@RequestMapping(value = "/call", method = {RequestMethod.POST,RequestMethod.GET})
	public String call(HttpServletRequest request, HttpServletResponse response, @RequestBody String requestBody) {
		response.setStatus(200);
		long startTime = System.currentTimeMillis();
		Gson gson = new Gson();
		Map<String,Object> map = gson.fromJson(requestBody, new TypeToken<HashMap<String,Object>>(){}.getType());
        if(map==null) {
        	return null;
        }
        String reqNo = map.get("reqNo")==null?"":map.get("reqNo").toString();
		EvalResultDto payload = new EvalResultDto(reqNo);
		Map<String,Object> resultMap = new HashMap<String,Object>();
		payload.setResultMap(resultMap);
		ApiResponse<EvalResultDto> api = new ApiResponse<EvalResultDto>(StatusEnum.S, "1", "成功", payload);
		log.info("call cost"+(System.currentTimeMillis() -startTime )+"ms");
        return gson.toJson(api);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
