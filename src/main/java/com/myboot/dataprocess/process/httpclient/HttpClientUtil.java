package com.myboot.dataprocess.process.httpclient;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class HttpClientUtil {
	
    private static PoolingHttpClientConnectionManager cm = null;

    static {
        /*LayeredConnectionSocketFactory sslsf = null;
        try {
            sslsf = new SSLConnectionSocketFactory(SSLContext.getDefault());
        } catch (NoSuchAlgorithmException e) {
            log.error("creat SSL http connection is fail ...");
        }*/
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                //.register("https", sslsf)
                .register("http", new PlainConnectionSocketFactory())
                .build();
        cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        cm.setMaxTotal(500);//多线程调用注意配置，根据线程数设定,可接收的最大请求数
        cm.setDefaultMaxPerRoute(200);//多线程调用注意配置，根据线程数设定，可并行处理的最大线程数
        cm.setValidateAfterInactivity(3000);//验证不活跃

    }

    public static CloseableHttpClient getHttpClient() {
//    	RequestConfig requestConfig = RequestConfig.custom()
//			      .setSocketTimeout(10000)//数据传输过程中数据包之间间隔的最大时间
//			      .setConnectTimeout(10000)//连接建立时间，三次握手完成时间
//			      .setExpectContinueEnabled(true)//重点参数 
//			      .setConnectionRequestTimeout(6000)
//			      .build();
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
//                .setDefaultRequestConfig(requestConfig)
                //.setConnectionManagerShared(true)
                .build();
        return httpClient;
    }
}
