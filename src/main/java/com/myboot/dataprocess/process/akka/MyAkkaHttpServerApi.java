package com.myboot.dataprocess.process.akka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
// import com.myboot.dataprocess.common.MyConstants;
// import com.myboot.dataprocess.model.result.ApiResponse;
// import com.myboot.dataprocess.model.result.EvalResultDto;
// import com.myboot.dataprocess.model.result.StatusEnum;
import com.myboot.dataprocess.model.result.EvalResultDto;

import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpEntity;
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

  public static void main(String[] args) {
    start();
  }

  public static void start() {
    http.bindAndHandleSync((req) -> {
      HttpEntity.Strict ss = null;
      try {
        ss = req.entity().toStrict(5000, materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);
        String result = ss.getData().utf8String();
        System.out.println(result);
//        log.info("http response result:" + result);
        //long startTime = System.currentTimeMillis();
        Gson gson = new Gson();
        Map<String, Object> map = gson.fromJson(result, new TypeToken<HashMap<String, Object>>() {
        }.getType());
        if (map == null) {
          return null;
        }
        String reqNo = map.get("reqNo") == null ? "" : map.get("reqNo").toString();
        EvalResultDto payload = new EvalResultDto(reqNo);
        payload.setResultMap(map);
//        ApiResponse<EvalResultDto> api = new ApiResponse<EvalResultDto>(StatusEnum.S, "1", "成功", payload);
//        log.info("MyAkkaProcessController#call:receive content:" + api);
//        log.info("dinst call cost： " + (System.currentTimeMillis() - startTime) + "ms");
        return HttpResponse.create().withEntity(gson.toJson(payload));
      } catch (Exception e) {
        e.printStackTrace();
        log.error(e.getMessage());
      }
      return HttpResponse.create().withEntity("");
    }, ConnectHttp.toHost("127.0.0.1", 8080), materializer);
  }

}
